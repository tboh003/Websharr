"""Download manager: queues Webshare downloads and tracks SABnzbd-style state.

Jobs persist to a JSON state file so history survives restarts; jobs that were
still downloading are re-queued on startup and resume from the partial file in
the incomplete dir via an HTTP Range request.

With the "pyload" backend a job is handed to a shared pyLoad instance as the
package websharr-<nzo_id> and polled until it finishes; Websharr then moves
the file into the complete dir and removes the package and its folder.
"""

import asyncio
import json
import logging
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import httpx

from .nzb import sanitize_filename
from .pyload import (FAILED_STATES, FINISHED, PyloadClient, PyloadError, PyloadPackageGone,
                     PyloadUnreachable)
from .settings import BACKENDS
from .webshare import WebshareClient, WebshareError

logger = logging.getLogger("websharr.downloads")

CHUNK_SIZE = 1024 * 1024
HISTORY_CAP = 500  # keep this many completed/failed records in the UI history
PYLOAD_POLL_SECONDS = 5.0  # seconds between pyLoad status polls
# How long polls may keep failing with a transient error (pyLoad restarting,
# HTTP 5xx) before the job fails.
PYLOAD_ERROR_GRACE_SECONDS = 300.0


class _StaleAttach(Exception):
    """A re-attached pyLoad package is gone or failed: start over once."""


def describe_error(exc: BaseException) -> str:
    """Error text for the history and *arr's fail message. Several httpx errors
    (ReadTimeout, PoolTimeout…) carry no message, which left "failed: " with
    nothing to go on; then the exception type is said instead."""
    return str(exc).strip() or type(exc).__name__


def _total_size(resp: httpx.Response, offset: int) -> int:
    """Full file size implied by the response (0 if unknown)."""
    if resp.status_code == 206:
        content_range = resp.headers.get("content-range", "")  # bytes 100-999/1000
        if "/" in content_range:
            try:
                return int(content_range.rsplit("/", 1)[1])
            except ValueError:
                pass
        return offset + int(resp.headers.get("content-length", "0") or 0)
    return int(resp.headers.get("content-length", "0") or 0)


@dataclass
class Job:
    nzo_id: str
    ident: str
    name: str  # sanitized file name incl. extension
    category: str
    size: int
    # Release/display name (e.g. "Skvrna S01E01 - ...") used for the job folder
    # and reported to Sonarr/Radarr, so import parses the episode even when the
    # raw Webshare filename lacks SxxExx. Falls back to the filename stem.
    title: str = ""
    status: str = "queued"  # queued | downloading | paused | completed | failed
    downloaded: int = 0
    speed: float = 0.0  # bytes/s, not persisted meaningfully
    error: str = ""
    storage: str = ""  # final job directory (what Sonarr imports from)
    added_ts: float = field(default_factory=time.time)
    completed_ts: float = 0.0
    # Sonarr removes a download from the SABnzbd history once it has imported it;
    # we honor that for the SABnzbd view but keep the record in the Websharr UI
    # history. This flag hides it from Sonarr only.
    sab_hidden: bool = False
    # Backend chosen when the job was added; a pyLoad job remembers its package
    # id so a restart re-attaches instead of adding a second package.
    backend: str = "direct"
    pyload_pid: int = 0

    @property
    def job_name(self) -> str:
        """Name shown to *arr and used for the download folder."""
        if self.title:
            return self.title
        return self.name.rsplit(".", 1)[0] if "." in self.name else self.name


class DownloadManager:
    def __init__(self, client: WebshareClient, complete_dir: Path, incomplete_dir: Path,
                 state_file: Path, max_concurrent: int = 2, notify=None,
                 pyload: PyloadClient | None = None, backend: str = "direct",
                 pyload_dir: Path = Path("/downloads"), stall_seconds: float = 86400):
        self._client = client
        self._complete_dir = complete_dir
        self._incomplete_dir = incomplete_dir
        self._state_file = state_file
        self._max_concurrent = max(1, int(max_concurrent))
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        # Optional async callback (title, body) for failure notifications.
        self._notify = notify
        self._pyload = pyload
        self._pyload_dir = pyload_dir
        self._stall_seconds = stall_seconds
        self._cleanups: set[asyncio.Task] = set()
        self._backend = "direct"
        self.set_backend(backend)

    @property
    def backend(self) -> str:
        return self._backend

    def set_backend(self, value: str) -> str:
        """Choose the backend for jobs added from now on; queued and running
        jobs keep the one they were added with."""
        if value not in BACKENDS:
            raise ValueError(f"unknown download backend: {value}")
        if value == "pyload" and self._pyload is None:
            raise ValueError("pyLoad backend needs a pyLoad URL and API key")
        self._backend = value
        return self._backend

    def set_pyload(self, client: PyloadClient | None, pyload_dir: Path,
                   stall_seconds: float) -> None:
        """Swap the pyLoad connection at runtime. Without a client, new jobs
        fall back to the direct backend."""
        self._pyload = client
        self._pyload_dir = pyload_dir
        self._stall_seconds = stall_seconds
        if client is None and self._backend == "pyload":
            logger.warning("pyLoad is not configured; new downloads use the direct backend")
            self._backend = "direct"

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    def set_max_concurrent(self, value: int) -> int:
        """Change the concurrency limit at runtime. Running downloads keep their
        slot; a higher limit immediately starts more queued jobs, a lower one
        just applies as active downloads finish."""
        self._max_concurrent = max(1, min(int(value), 10))
        self._schedule()
        return self._max_concurrent

    def ensure_dirs(self) -> None:
        """Pre-create the category folders so *arr's download-client health
        check ("directory does not exist inside the container") passes before
        the first download of that category completes."""
        self._incomplete_dir.mkdir(parents=True, exist_ok=True)
        for category in ("tv", "movies"):
            (self._complete_dir / category).mkdir(parents=True, exist_ok=True)

    # -- persistence -------------------------------------------------------

    def load_state(self) -> None:
        if not self._state_file.exists():
            return
        try:
            raw = json.loads(self._state_file.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not read state file: %s", exc)
            return
        for item in raw.get("jobs", []):
            item.pop("speed", None)
            try:
                job = Job(**item)
            except TypeError:
                continue
            self._jobs[job.nzo_id] = job

    def resume_pending(self) -> None:
        for job in self._jobs.values():
            if job.status in ("queued", "downloading"):
                job.status = "queued"
                logger.info("Re-queued unfinished job %s (%s)", job.nzo_id, job.name)
        self._schedule()

    def _save_state(self) -> None:
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            data = {"jobs": [asdict(j) for j in self._jobs.values()]}
            tmp = self._state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2))
            tmp.replace(self._state_file)
        except OSError as exc:
            logger.warning("Could not write state file: %s", exc)

    # -- public API --------------------------------------------------------

    def add(self, ident: str, name: str, size: int, category: str, title: str = "") -> Job:
        job = Job(
            nzo_id=f"SABnzbd_nzo_{uuid.uuid4().hex[:12]}",
            ident=ident,
            name=sanitize_filename(name),
            category=category or "*",
            size=size,
            title=sanitize_filename(title) if title else "",
            backend=self._backend,
        )
        self._jobs[job.nzo_id] = job
        self._save_state()
        logger.info("Queued download %s -> %s (cat=%s)", ident, job.name, job.category)
        self._schedule()
        return job

    def get(self, nzo_id: str) -> Job | None:
        return self._jobs.get(nzo_id)

    _QUEUE_STATES = ("queued", "downloading", "paused")

    def queue_jobs(self) -> list[Job]:
        return [j for j in self._jobs.values() if j.status in self._QUEUE_STATES]

    def reorder(self, order: list[str]) -> bool:
        """Rearrange the queue to match the given list of nzo_ids. Only the order
        in which queued jobs start is affected — jobs already downloading keep
        their slot. Unlisted queue jobs keep their relative order at the end."""
        rank = {nzo: i for i, nzo in enumerate(order)}
        queue = [j for j in self._jobs.values() if j.status in self._QUEUE_STATES]
        rest = [j for j in self._jobs.values() if j.status not in self._QUEUE_STATES]
        queue.sort(key=lambda j: rank.get(j.nzo_id, len(rank)))
        self._jobs = {j.nzo_id: j for j in queue}
        for job in rest:
            self._jobs[job.nzo_id] = job
        self._save_state()
        self._schedule()
        return True

    def pause(self, nzo_id: str) -> bool:
        """Stop an active/queued download but keep its partial file for resume."""
        job = self._jobs.get(nzo_id)
        if job is None or job.status not in ("queued", "downloading"):
            return False
        if job.backend == "pyload":
            return False  # pyLoad has no per-package pause
        job.status = "paused"
        job.speed = 0.0
        task = self._tasks.pop(nzo_id, None)
        if task is not None and not task.done():
            task.cancel()  # CancelledError unwinds; status stays "paused"
        self._save_state()
        self._schedule()  # let a waiting job take the freed slot
        logger.info("Paused %s (%s)", nzo_id, job.name)
        return True

    def resume(self, nzo_id: str) -> bool:
        """Re-queue a paused job; the worker continues from the partial file."""
        job = self._jobs.get(nzo_id)
        if job is None or job.status != "paused":
            return False
        job.status = "queued"
        self._schedule()
        self._save_state()
        logger.info("Resumed %s (%s)", nzo_id, job.name)
        return True

    def history_jobs(self, include_hidden: bool = True) -> list[Job]:
        """Completed/failed jobs, newest first. `include_hidden=False` drops the
        ones Sonarr already removed from its own history (the SABnzbd view)."""
        jobs = [j for j in self._jobs.values()
                if j.status in ("completed", "failed")
                and (include_hidden or not j.sab_hidden)]
        return sorted(jobs, key=lambda j: j.completed_ts, reverse=True)

    def hide_from_sab(self, nzo_id: str, del_files: bool = False) -> bool:
        """Sonarr imported & removed this download: hide it from the SABnzbd
        history but keep the record for the Websharr UI. Files may be dropped."""
        job = self._jobs.get(nzo_id)
        if job is None:
            return False
        job.sab_hidden = True
        if job.backend == "pyload" and job.status == "failed":
            self._spawn_pyload_cleanup(job)  # *arr gave up on it
        if del_files and job.storage:
            shutil.rmtree(job.storage, ignore_errors=True)
        self._save_state()
        return True

    def clear_history(self) -> int:
        """Remove all completed/failed records from the Websharr history."""
        gone = [j.nzo_id for j in self._jobs.values() if j.status in ("completed", "failed")]
        for nzo_id in gone:
            self.delete(nzo_id)
        return len(gone)

    def retry(self, nzo_id: str) -> Job | None:
        """Re-queue a failed history job; resumes from its partial file if any."""
        job = self._jobs.get(nzo_id)
        if job is None or job.status != "failed":
            return None
        job.status = "queued"
        job.error = ""
        job.completed_ts = 0.0
        self._schedule()
        self._save_state()
        logger.info("Retrying job %s (%s)", job.nzo_id, job.name)
        return job

    def delete(self, nzo_id: str, del_files: bool = False) -> bool:
        job = self._jobs.pop(nzo_id, None)
        if job is None:
            return False
        task = self._tasks.pop(nzo_id, None)
        if task is not None and not task.done():
            task.cancel()
        self._cleanup_incomplete(job)
        if job.backend == "pyload":
            self._spawn_pyload_cleanup(job)
        if del_files and job.storage:
            shutil.rmtree(job.storage, ignore_errors=True)
        self._save_state()
        return True

    async def shutdown(self) -> None:
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._cleanups:
            await asyncio.gather(*self._cleanups, return_exceptions=True)
        self._save_state()

    # -- internals ---------------------------------------------------------

    def _incomplete_path(self, job: Job) -> Path:
        return self._incomplete_dir / job.nzo_id

    def _cleanup_incomplete(self, job: Job) -> None:
        shutil.rmtree(self._incomplete_path(job), ignore_errors=True)

    def _schedule(self) -> None:
        """Start queued jobs in queue order until the concurrency limit is hit.

        Replaces a fixed semaphore so the limit can change at runtime and the
        queue order (drag-and-drop) decides which job runs next. A job's slot is
        claimed synchronously (status -> downloading) before its task is created,
        so repeated calls in the same tick never over-schedule.
        """
        active = sum(1 for j in self._jobs.values() if j.status == "downloading")
        for job in self._jobs.values():
            if active >= self._max_concurrent:
                break
            if job.status == "queued" and job.nzo_id not in self._tasks:
                job.status = "downloading"
                self._tasks[job.nzo_id] = asyncio.create_task(self._run(job))
                active += 1

    async def _run(self, job: Job) -> None:
        try:
            if job.nzo_id not in self._jobs:
                return  # deleted before the task got to run
            self._save_state()
            await self._download(job)
            job.status = "completed"
            job.completed_ts = time.time()
            logger.info("Completed %s -> %s", job.nzo_id, job.storage)
        except asyncio.CancelledError:
            logger.info("Cancelled %s", job.nzo_id)
            raise
        except (WebshareError, PyloadError, httpx.HTTPError, OSError) as exc:
            job.status = "failed"
            job.error = describe_error(exc)
            job.completed_ts = time.time()
            # Keep the partial file so a retry can resume; delete() cleans it up.
            logger.error("Download %s failed: %s", job.nzo_id, job.error,
                         exc_info=not str(exc))  # no text: the traceback is all there is
            if self._notify:
                try:
                    await self._notify("Websharr: download failed", f"{job.job_name}\n{exc}")
                except Exception:  # notifications must never break the manager
                    logger.debug("failure notification errored", exc_info=True)
        finally:
            self._tasks.pop(job.nzo_id, None)
            if job.nzo_id in self._jobs:
                self._prune_history()
                self._save_state()
            self._schedule()  # a slot freed — start the next queued job

    def _prune_history(self) -> None:
        """Cap the retained history so state.json can't grow without bound."""
        history = sorted(
            (j for j in self._jobs.values() if j.status in ("completed", "failed")),
            key=lambda j: j.completed_ts, reverse=True,
        )
        for job in history[HISTORY_CAP:]:
            self.delete(job.nzo_id)

    async def _download(self, job: Job) -> None:
        if job.backend == "pyload":
            await self._download_pyload(job)
            return
        # No retry on a link error: Webshare's "File temporarily unavailable" does
        # not recover in practice. Failing right away frees the slot and lets *arr
        # blocklist the release (see torznab._pub_date) and grab another one.
        url = await self._client.file_link(job.ident)

        work_dir = self._incomplete_path(job)
        work_dir.mkdir(parents=True, exist_ok=True)
        target = work_dir / job.name

        offset = target.stat().st_size if target.exists() else 0

        async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0, read=120.0)) as http:
            while True:
                headers = {"Range": f"bytes={offset}-"} if offset else {}
                async with http.stream("GET", url, headers=headers) as resp:
                    if offset and resp.status_code == 416:
                        # Partial file no longer matches the remote; start over.
                        target.unlink(missing_ok=True)
                        offset = 0
                        continue
                    if offset and resp.status_code != 206:
                        offset = 0  # server ignored the Range header
                    resp.raise_for_status()
                    total = _total_size(resp, offset)
                    if total:
                        job.size = total
                    if offset:
                        logger.info("Resuming %s from %d bytes", job.nzo_id, offset)
                    await self._stream_to_file(job, resp, target, offset)
                break

        final_dir = self._complete_dir / job.category / job.job_name
        final_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(target), final_dir / job.name)
        shutil.rmtree(work_dir, ignore_errors=True)
        job.storage = str(final_dir)
        job.size = max(job.size, job.downloaded)

    @staticmethod
    async def _stream_to_file(job: Job, resp: httpx.Response, target: Path, offset: int) -> None:
        job.downloaded = offset
        last_t = time.monotonic()
        last_bytes = offset
        with target.open("ab" if offset else "wb") as fh:
            async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                fh.write(chunk)
                job.downloaded += len(chunk)
                now = time.monotonic()
                if now - last_t >= 2.0:
                    job.speed = (job.downloaded - last_bytes) / (now - last_t)
                    last_t = now
                    last_bytes = job.downloaded

    # -- pyLoad backend ----------------------------------------------------

    def _spawn_pyload_cleanup(self, job: Job) -> None:
        cleanup = asyncio.create_task(self._pyload_cleanup(job))
        self._cleanups.add(cleanup)
        cleanup.add_done_callback(self._cleanups.discard)

    def _require_pyload(self) -> PyloadClient:
        # Looked up on every call: the settings UI may swap or remove the client.
        if self._pyload is None:
            raise PyloadError("pyLoad backend is not configured")
        return self._pyload

    @staticmethod
    def _pyload_name(job: Job) -> str:
        return f"websharr-{job.nzo_id}"

    def _pyload_folder(self, job: Job) -> Path:
        return self._pyload_dir / self._pyload_name(job)

    async def _download_pyload(self, job: Job) -> None:
        # Any unexpected error (e.g. a malformed pyLoad response) fails the job
        # with a readable reason; no job may stay "downloading" forever.
        try:
            self._require_pyload()
            if job.pyload_pid:
                try:
                    await self._pyload_wait(job, reattached=True)
                except _StaleAttach:
                    logger.info("pyLoad package %d of %s is gone or failed; starting over",
                                job.pyload_pid, job.nzo_id)
                    await self._pyload_cleanup(job)
                    job.pyload_pid = 0
            if not job.pyload_pid:
                url = await self._client.file_link(job.ident)
                job.pyload_pid = await self._require_pyload().add_package(self._pyload_name(job), url)
                self._save_state()
                logger.info("Handed %s to pyLoad as package %d", job.nzo_id, job.pyload_pid)
                await self._pyload_wait(job, reattached=False)
            await self._pyload_finish(job)
        except (PyloadError, WebshareError, httpx.HTTPError, OSError):
            raise
        except Exception as exc:
            raise PyloadError(f"unexpected pyLoad response: {type(exc).__name__}: {exc}") from exc

    async def _pyload_wait(self, job: Job, reattached: bool) -> None:
        """Poll the package until all its files are finished."""
        pid = job.pyload_pid
        first = True
        last_done = -1
        last_change = time.monotonic()
        error_since = None  # start of the current run of transient errors
        while True:
            try:
                files = await self._require_pyload().package_files(pid)
                failed = [f for f in files if f.status in FAILED_STATES]
                finished = bool(files) and all(f.status == FINISHED for f in files)
                progress = None
                if not failed and not finished:
                    progress = await self._require_pyload().progress(pid)
            except PyloadPackageGone:
                if first and reattached:
                    raise _StaleAttach from None
                raise PyloadError(f"package removed in pyLoad (id {pid})") from None
            except PyloadUnreachable as exc:
                now = time.monotonic()
                if error_since is None:
                    error_since = now
                    logger.warning("pyLoad poll for %s failed, retrying: %s", job.nzo_id, exc)
                if now - error_since >= PYLOAD_ERROR_GRACE_SECONDS:
                    raise
                await asyncio.sleep(PYLOAD_POLL_SECONDS)
                continue
            error_since = None
            if failed:
                if first and reattached:
                    raise _StaleAttach
                f = failed[0]
                reason = ": ".join(dict.fromkeys(m for m in (f.statusmsg, f.error) if m))
                raise PyloadError(f"pyLoad download failed: {reason or 'unknown error'}")
            first = False
            if finished:
                job.speed = 0.0
                return
            size = sum(f.size for f in files)
            if size:
                job.size = size
            if progress is None:
                job.speed = 0.0  # no active download row: keep the last byte count
            else:
                job.downloaded, job.speed = progress
            now = time.monotonic()
            if job.downloaded != last_done:
                last_done = job.downloaded
                last_change = now
            elif now - last_change >= self._stall_seconds:
                # Clean up so a retry starts with a new package.
                await self._pyload_cleanup(job)
                job.pyload_pid = 0
                raise PyloadError(f"pyLoad download stalled: no progress for {_fmt_duration(self._stall_seconds)}")
            await asyncio.sleep(PYLOAD_POLL_SECONDS)

    async def _pyload_finish(self, job: Job) -> None:
        """Move the finished file into the complete dir and drop the package.
        pyLoad leaves .chunk* files next to the result; the largest other file
        is the download."""
        folder = self._pyload_folder(job)
        files = [p for p in folder.iterdir()
                 if p.is_file() and not p.suffix.startswith(".chunk")] if folder.is_dir() else []
        if not files:
            raise PyloadError(f"pyLoad finished but left no finished file in {folder}")
        source = max(files, key=lambda p: p.stat().st_size)
        final_dir = self._complete_dir / job.category / job.job_name
        final_dir.mkdir(parents=True, exist_ok=True)
        size = source.stat().st_size
        # Off the event loop: across volumes the move is a multi-GB copy.
        await asyncio.to_thread(shutil.move, str(source), final_dir / job.name)
        job.storage = str(final_dir)  # set now so a delete(del_files) during cleanup removes it
        await self._pyload_cleanup(job)
        job.pyload_pid = 0
        job.size = job.downloaded = max(job.size, size)

    async def _pyload_cleanup(self, job: Job) -> None:
        """Remove this job's package and folder. pyLoad keeps the folder and
        partial files after delete_packages, so Websharr removes it itself."""
        if job.pyload_pid and self._pyload is not None:
            try:
                await self._pyload.delete_package(job.pyload_pid)
            except PyloadError as exc:
                logger.debug("pyLoad package %d not deleted: %s", job.pyload_pid, exc)
        await asyncio.to_thread(shutil.rmtree, self._pyload_folder(job), ignore_errors=True)


def _fmt_duration(seconds: float) -> str:
    return f"{seconds / 3600:g} h" if seconds >= 3600 else f"{seconds:g} s"

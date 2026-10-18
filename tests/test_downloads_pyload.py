"""Download manager: the pyLoad backend (hand-off, polling, finish, cleanup)."""

import asyncio
import json
import logging
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.downloads as downloads_module
import app.main as app_main
from app.config import config
from app.downloads import DownloadManager, Job
from app.main import app
from app.pyload import FINISHED, PyloadError, PyloadFile, PyloadPackageGone

from .conftest import FakeWebshareClient, wait_for
from .test_downloads import _serve

DOWNLOADING = 12


class FakePyload:
    """Stand-in for PyloadClient. Each poll of package_files returns the next
    status of `script` (the last one repeats); the first FINISHED poll writes
    `finish_files` into the package folder, as pyLoad would."""

    def __init__(self, pyload_dir: Path):
        self.dir = pyload_dir
        self.next_pid = 7
        self.packages: dict[int, str] = {}  # pid -> package name
        self.added: list[tuple[str, str]] = []
        self.polled: list[int] = []
        self.deleted: list[int] = []
        self.script: list[int] = [DOWNLOADING]
        self.finish_files: dict[str, bytes] | None = {"Movie.mkv": b"movie" * 100}
        self.size = 500
        self.done = 0
        self.speed = 0.0
        self.statusmsg = ""
        self.error = ""
        self.poll_error: BaseException | None = None
        self.add_error: BaseException | None = None
        self.gone_pids: set[int] = set()
        self.closed = False

    async def add_package(self, name: str, link: str) -> int:
        if self.add_error:
            raise self.add_error
        pid = self.next_pid
        self.next_pid += 1
        self.packages[pid] = name
        self.added.append((name, link))
        return pid

    async def package_files(self, pid: int) -> list[PyloadFile]:
        self.polled.append(pid)
        if self.poll_error:
            raise self.poll_error
        if pid in self.gone_pids or pid not in self.packages:
            raise PyloadPackageGone(f"pyLoad package {pid} not found")
        status = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if status == FINISHED and self.finish_files is not None:
            folder = self.dir / self.packages[pid]
            folder.mkdir(parents=True, exist_ok=True)
            for name, data in self.finish_files.items():
                (folder / name).write_bytes(data)
            self.finish_files = None
        return [PyloadFile(fid=1, name="Movie.mkv", size=self.size, status=status,
                           statusmsg=self.statusmsg, error=self.error)]

    async def progress(self, pid: int) -> tuple[int, float]:
        return self.done, self.speed

    async def delete_package(self, pid: int) -> None:
        self.deleted.append(pid)
        if pid not in self.packages:
            raise PyloadError("pyLoad returned HTTP 500")
        del self.packages[pid]

    async def close(self) -> None:
        self.closed = True


class LoopThread:
    """An event loop on a background thread, so the manager's tasks run while
    the test thread waits with the synchronous wait_for()."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def call(self, fn, *args, **kwargs):
        async def runner():
            return fn(*args, **kwargs)
        return asyncio.run_coroutine_threadsafe(runner(), self.loop).result(5)

    def wait(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(5)

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


@pytest.fixture(autouse=True)
def _fast_poll(monkeypatch):
    monkeypatch.setattr(downloads_module, "PYLOAD_POLL_SECONDS", 0.01)


@pytest.fixture
def env(tmp_path):
    loop = LoopThread()
    fake = FakePyload(tmp_path / "pyload")
    created: list[DownloadManager] = []

    def make(backend: str = "pyload", stall_seconds: float = 3600, pyload=fake) -> DownloadManager:
        manager = DownloadManager(
            client=FakeWebshareClient(),
            complete_dir=tmp_path / "complete",
            incomplete_dir=tmp_path / "incomplete",
            state_file=tmp_path / "state.json",
            pyload=pyload,
            backend=backend,
            pyload_dir=tmp_path / "pyload",
            stall_seconds=stall_seconds,
        )
        created.append(manager)
        return manager

    yield loop, fake, make, tmp_path
    for manager in created:
        loop.wait(manager.shutdown())
    loop.stop()


def _add(loop, manager) -> Job:
    return loop.call(manager.add, "ident1", "Movie.2024.mkv", 0, "movies", title="Movie 2024")


def test_pyload_job_completes_and_moves_file(env):
    loop, fake, make, tmp = env
    fake.script = [DOWNLOADING, FINISHED]
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status in ("completed", "failed"))
    assert job.status == "completed", job.error
    assert job.backend == "pyload"
    assert fake.added == [(f"websharr-{job.nzo_id}", FakeWebshareClient().file_link_url)]
    final = tmp / "complete" / "movies" / job.job_name / job.name
    assert final.read_bytes() == b"movie" * 100
    assert job.storage == str(final.parent)
    assert fake.deleted == [7]
    assert not (tmp / "pyload" / f"websharr-{job.nzo_id}").exists()


def test_progress_updates_job_size_downloaded_speed(env):
    loop, fake, make, _ = env
    fake.size, fake.done, fake.speed = 1000, 400, 250.0
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.downloaded == 400)
    assert job.size == 1000
    assert job.speed == 250.0
    assert job.status == "downloading"
    assert job.pyload_pid == 7
    saved = json.loads((env[3] / "state.json").read_text())["jobs"][0]
    assert saved["pyload_pid"] == 7 and saved["backend"] == "pyload"


def test_pyload_failed_status_fails_job(env):
    loop, fake, make, _ = env
    fake.script = [8]
    fake.statusmsg, fake.error = "failed", "Link expired"
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status == "failed")
    assert "failed" in job.error and "Link expired" in job.error


def test_pyload_unreachable_fails_job(env):
    loop, fake, make, _ = env
    fake.poll_error = PyloadError("pyLoad unreachable")
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status == "failed")
    assert "pyLoad unreachable" in job.error


def test_pyload_rejects_api_key_on_add_fails_job(env):
    loop, fake, make, _ = env
    fake.add_error = PyloadError("pyLoad rejected the API key")
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status == "failed")
    assert "rejected the API key" in job.error


def test_malformed_pyload_response_fails_job(env):
    loop, fake, make, _ = env
    fake.poll_error = KeyError("fid")
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status == "failed")
    assert "pyLoad" in job.error and "KeyError" in job.error


def test_package_removed_fails_job(env):
    loop, fake, make, _ = env
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.pyload_pid == 7 and len(fake.polled) >= 2)
    fake.gone_pids.add(7)
    assert wait_for(lambda: job.status == "failed")
    assert "package removed in pyLoad" in job.error


def _write_state(tmp: Path, **job_fields) -> str:
    nzo_id = "SABnzbd_nzo_restart00001"
    job = {"nzo_id": nzo_id, "ident": "ident1", "name": "Movie.2024.mkv",
           "category": "movies", "size": 500, "title": "Movie 2024",
           "status": "downloading", "backend": "pyload", "pyload_pid": 7}
    job.update(job_fields)
    (tmp / "state.json").write_text(json.dumps({"jobs": [job]}))
    return nzo_id


def test_restart_reattaches_without_second_package(env):
    loop, fake, make, tmp = env
    nzo_id = _write_state(tmp)
    fake.packages[7] = f"websharr-{nzo_id}"
    fake.next_pid = 20
    fake.script = [DOWNLOADING, FINISHED]
    manager = make()
    loop.call(manager.load_state)
    loop.call(manager.resume_pending)
    job = manager.get(nzo_id)
    assert wait_for(lambda: job.status in ("completed", "failed"))
    assert job.status == "completed", job.error
    assert fake.added == []
    assert set(fake.polled) == {7}


def test_restart_with_package_gone_starts_fresh_once(env):
    loop, fake, make, tmp = env
    nzo_id = _write_state(tmp)
    fake.next_pid = 20
    fake.script = [FINISHED]
    manager = make()
    loop.call(manager.load_state)
    loop.call(manager.resume_pending)
    job = manager.get(nzo_id)
    assert wait_for(lambda: job.status in ("completed", "failed"))
    assert job.status == "completed", job.error
    assert len(fake.added) == 1
    assert fake.polled[0] == 7 and set(fake.polled[1:]) == {20}


def test_old_state_without_backend_loads_as_direct(env):
    loop, fake, make, tmp = env
    (tmp / "state.json").write_text(json.dumps({"jobs": [{
        "nzo_id": "SABnzbd_nzo_old000000001", "ident": "i", "name": "a.mkv",
        "category": "tv", "size": 1, "status": "completed"}]}))
    manager = make()
    loop.call(manager.load_state)
    job = manager.get("SABnzbd_nzo_old000000001")
    assert job.backend == "direct" and job.pyload_pid == 0


def test_chunk_files_ignored_largest_file_wins(env):
    loop, fake, make, tmp = env
    fake.script = [FINISHED]
    fake.finish_files = {
        "Movie.mkv.chunk0": b"c" * 5000,
        "Movie.mkv.chunks": b"c" * 4000,
        "sample.mkv": b"s" * 10,
        "Movie.mkv": b"m" * 300,
    }
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status in ("completed", "failed"))
    assert job.status == "completed", job.error
    final = tmp / "complete" / "movies" / job.job_name / job.name
    assert final.read_bytes() == b"m" * 300
    assert not (tmp / "pyload" / f"websharr-{job.nzo_id}").exists()


def test_no_finished_file_fails_job(env):
    loop, fake, make, _ = env
    fake.script = [FINISHED]
    fake.finish_files = {"Movie.mkv.chunk0": b"c" * 100}
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.status in ("completed", "failed"))
    assert job.status == "failed"
    assert "no finished file" in job.error


def test_stall_fails_job(env):
    loop, fake, make, _ = env
    manager = make(stall_seconds=0.05)
    job = _add(loop, manager)
    assert wait_for(lambda: job.status == "failed")
    assert "stalled" in job.error


def test_delete_queued_pyload_job_removes_package_and_folder(env):
    loop, fake, make, tmp = env
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.pyload_pid == 7)
    folder = tmp / "pyload" / f"websharr-{job.nzo_id}"
    folder.mkdir(parents=True)
    (folder / "Movie.mkv.chunk0").write_bytes(b"partial")
    assert loop.call(manager.delete, job.nzo_id) is True
    assert wait_for(lambda: fake.deleted == [7] and not folder.exists())
    assert 7 not in fake.packages


def test_delete_with_package_already_gone_is_quiet(env, caplog):
    loop, fake, make, tmp = env
    nzo_id = _write_state(tmp, status="failed")
    manager = make()
    loop.call(manager.load_state)
    with caplog.at_level(logging.DEBUG, logger="websharr.downloads"):
        assert loop.call(manager.delete, nzo_id) is True
        assert wait_for(lambda: fake.deleted == [7])
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_pause_unsupported_for_pyload_job(env):
    loop, fake, make, _ = env
    manager = make()
    job = _add(loop, manager)
    assert wait_for(lambda: job.pyload_pid == 7)
    assert loop.call(manager.pause, job.nzo_id) is False
    assert job.status == "downloading"


def test_set_backend_validates(env):
    loop, fake, make, _ = env
    manager = make(backend="direct")
    assert manager.backend == "direct"
    with pytest.raises(ValueError):
        manager.set_backend("torrent")
    assert manager.set_backend("pyload") == "pyload"
    no_client = make(backend="direct", pyload=None)
    with pytest.raises(ValueError):
        no_client.set_backend("pyload")
    with pytest.raises(ValueError):
        make(backend="pyload", pyload=None)


def test_job_keeps_backend_chosen_at_add_time(env):
    loop, fake, make, _ = env
    fake.script = [DOWNLOADING]
    manager = make()
    job = _add(loop, manager)
    loop.call(manager.set_backend, "direct")
    assert job.backend == "pyload"
    assert wait_for(lambda: job.pyload_pid == 7)


def test_direct_backend_unchanged(env):
    loop, fake, make, tmp = env
    payload = b"direct" * 1000
    httpd, _ = _serve(payload, support_range=True)
    try:
        manager = make(backend="direct")
        manager._client.file_link_url = f"http://127.0.0.1:{httpd.server_port}/f.mkv"
        job = _add(loop, manager)
        assert wait_for(lambda: job.status in ("completed", "failed"))
        assert job.status == "completed", job.error
        assert job.backend == "direct"
        assert (tmp / "complete" / "movies" / job.job_name / job.name).read_bytes() == payload
        assert fake.added == [] and fake.polled == []
    finally:
        httpd.shutdown()


# -- app wiring (main.py lifespan) -------------------------------------------

@pytest.fixture
def app_env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "api_key", "testkey")
    monkeypatch.setattr(config, "complete_dir", tmp_path / "complete")
    monkeypatch.setattr(config, "incomplete_dir", tmp_path / "incomplete")
    monkeypatch.setattr(config, "state_file", tmp_path / "state.json")
    monkeypatch.setattr(config, "settings_file", tmp_path / "settings.json")
    monkeypatch.setattr(config, "pyload_download_dir", tmp_path / "pyload")
    monkeypatch.setattr(config, "download_backend", "pyload")
    monkeypatch.setattr(config, "pyload_url", "")
    monkeypatch.setattr(config, "pyload_api_key", "")
    monkeypatch.setattr(config, "pyload_stall_hours", 24)
    monkeypatch.setattr(app_main, "WebshareClient", FakeWebshareClient)
    built: list[FakePyload] = []

    def factory(url, api_key, transport=None):
        fake = FakePyload(tmp_path / "pyload")
        fake.args = (url, api_key)
        built.append(fake)
        return fake

    monkeypatch.setattr(app_main, "PyloadClient", factory)
    return monkeypatch, built


def test_app_builds_pyload_client_and_closes_it(app_env):
    monkeypatch, built = app_env
    monkeypatch.setattr(config, "pyload_url", "http://pyload:8000")
    monkeypatch.setattr(config, "pyload_api_key", "secretkey")
    with TestClient(app):
        manager = app.state.downloads
        assert manager.backend == "pyload"
        assert len(built) == 1 and built[0].args == ("http://pyload:8000", "secretkey")
        assert manager._pyload_dir == config.pyload_download_dir
        assert manager._stall_seconds == 24 * 3600
    assert built[0].closed


def test_app_without_pyload_config_falls_back_to_direct(app_env, caplog):
    monkeypatch, built = app_env
    with caplog.at_level(logging.WARNING):
        with TestClient(app):
            assert app.state.downloads.backend == "direct"
    assert built == []
    assert any("pyload" in r.getMessage().lower() for r in caplog.records
               if r.levelno == logging.WARNING)

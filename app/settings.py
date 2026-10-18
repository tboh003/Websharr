"""Persisted runtime settings — UI account, Webshare credentials, API key.

Stored as JSON (SETTINGS_FILE, default /config/settings.json) so values
survive restarts. Saved settings override environment variables; env vars
remain the initial defaults so existing deployments keep working.
"""

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time

from .config import config

logger = logging.getLogger("websharr.settings")

PBKDF2_ITERATIONS = 200_000
SESSION_TTL = 14 * 86400  # seconds
DEFAULT_THEME = "websharr-blue"
THEMES = frozenset({DEFAULT_THEME, "osaka-jade", "space-grey"})
BACKENDS = frozenset({"direct", "pyload"})
# pyLoad settings field -> key under "pyload" in settings.json.
_PYLOAD_KEYS = {"pyload_url": "url", "pyload_api_key": "api_key", "pyload_stall_hours": "stall_hours"}


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return f"pbkdf2:{PBKDF2_ITERATIONS}:{salt}:{dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iterations, salt, expected = stored.split(":")
        if scheme != "pbkdf2":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iterations))
        return hmac.compare_digest(dk.hex(), expected)
    except (ValueError, TypeError):
        return False


class Settings:
    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self.auth_username = ""
        self.auth_password_hash = ""
        self.webshare_username = ""
        self.webshare_password = ""  # legacy plaintext; migrated to digest at startup
        self.webshare_password_digest = ""
        self.api_key = ""
        self.secret = secrets.token_hex(32)
        # Search aliases: [{"from": "<*arr title>", "to": "<Webshare/CZ title>"}]
        # so a Sonarr/Radarr query also searches the Czech name a file uses.
        self.aliases: list[dict] = []
        # TMDB API Read Access Token (v4 bearer) — when set, a *arr query is also
        # searched under its Czech title looked up from TMDB. Env var is the
        # initial default; a value saved in the UI overrides it.
        self.tmdb_token: str = config.tmdb_token
        # Concurrent-download limit; env var is the initial default, editable in
        # the UI (Settings and the Queue page).
        self.max_concurrent: int = config.max_concurrent
        # Apprise notification URLs + VIP-expiry warning threshold (days).
        self.notify_urls: list[str] = list(config.notify_urls)
        self.notify_vip_days: int = config.notify_vip_days
        self.theme: str = DEFAULT_THEME
        # Download backend and pyLoad connection; env vars are the initial
        # defaults, saved UI values override them. The download dir is env-only.
        # Only fields set in the UI are saved (pyload_ui_set), so an unset field
        # keeps following the env, e.g. a PYLOAD_API_KEY rotated in Vault.
        self.pyload_ui_set: set[str] = set()
        self.download_backend: str = config.download_backend if config.download_backend in BACKENDS else "direct"
        self.pyload_url: str = config.pyload_url
        self.pyload_api_key: str = config.pyload_api_key
        self.pyload_stall_hours: int = config.pyload_stall_hours

    @property
    def configured(self) -> bool:
        return bool(self.auth_username and self.auth_password_hash)

    def load(self) -> None:
        self._reset()
        path = config.settings_file
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Could not read %s: %s", path, exc)
            return
        auth = data.get("auth", {})
        self.auth_username = auth.get("username", "")
        self.auth_password_hash = auth.get("password_hash", "")
        ws = data.get("webshare", {})
        self.webshare_username = ws.get("username", "")
        self.webshare_password = ws.get("password", "")
        self.webshare_password_digest = ws.get("password_digest", "")
        self.api_key = data.get("api_key", "")
        self.secret = data.get("secret") or self.secret
        aliases = data.get("aliases", [])
        self.aliases = [a for a in aliases if a.get("from") and a.get("to")]
        self.tmdb_token = data.get("tmdb_token") or config.tmdb_token
        try:
            self.max_concurrent = max(1, int(data.get("max_concurrent") or config.max_concurrent))
        except (TypeError, ValueError):
            self.max_concurrent = config.max_concurrent
        urls = data.get("notify_urls")
        self.notify_urls = [u for u in urls if u] if isinstance(urls, list) else list(config.notify_urls)
        try:
            self.notify_vip_days = int(data.get("notify_vip_days") or config.notify_vip_days)
        except (TypeError, ValueError):
            self.notify_vip_days = config.notify_vip_days
        theme = data.get("theme", DEFAULT_THEME)
        self.theme = theme if theme in THEMES else DEFAULT_THEME
        backend = data.get("download_backend") or config.download_backend
        self.download_backend = backend if backend in BACKENDS else "direct"
        pyload = data.get("pyload")
        pyload = pyload if isinstance(pyload, dict) else {}
        # A key present in the file was set in the UI (older files wrote all keys:
        # treating those as UI-set keeps their previous behavior).
        self.pyload_ui_set = {name for name, key in _PYLOAD_KEYS.items() if key in pyload}
        if "download_backend" in data:
            self.pyload_ui_set.add("download_backend")
        self.pyload_url = pyload.get("url") or config.pyload_url
        self.pyload_api_key = pyload.get("api_key") or config.pyload_api_key
        try:
            self.pyload_stall_hours = max(1, int(pyload.get("stall_hours") or config.pyload_stall_hours))
        except (TypeError, ValueError):
            self.pyload_stall_hours = config.pyload_stall_hours

    def save(self) -> None:
        path = config.settings_file
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "auth": {"username": self.auth_username, "password_hash": self.auth_password_hash},
            "webshare": {
                "username": self.webshare_username,
                "password": self.webshare_password,
                "password_digest": self.webshare_password_digest,
            },
            "api_key": self.api_key,
            "secret": self.secret,
            "aliases": self.aliases,
            "tmdb_token": self.tmdb_token,
            "max_concurrent": self.max_concurrent,
            "notify_urls": self.notify_urls,
            "notify_vip_days": self.notify_vip_days,
            "theme": self.theme,
        }
        if "download_backend" in self.pyload_ui_set:
            data["download_backend"] = self.download_backend
        pyload = {key: getattr(self, name) for name, key in _PYLOAD_KEYS.items()
                  if name in self.pyload_ui_set}
        if pyload:
            data["pyload"] = pyload
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        path.chmod(0o600)

    def ensure_api_key(self) -> None:
        """Guarantee a Sonarr-style random key when none was provided.

        Without this, deleting the key from settings.json would silently fall
        back to the weak built-in default ("websharr").
        """
        if config.api_key == "websharr" and not self.api_key:
            self.api_key = secrets.token_hex(16)
            self.save()
            logger.info("Generated a new API key (see Settings in the UI)")

    def apply(self) -> None:
        """Push saved values into the runtime config (settings win over env)."""
        if self.api_key:
            config.api_key = self.api_key
        if self.webshare_username:
            config.webshare_username = self.webshare_username
            config.webshare_password = self.webshare_password
            config.webshare_password_digest = self.webshare_password_digest
        config.download_backend = self.download_backend
        config.pyload_url = self.pyload_url
        config.pyload_api_key = self.pyload_api_key
        config.pyload_stall_hours = self.pyload_stall_hours

    # -- session cookies ----------------------------------------------------

    def _sign(self, payload: str) -> str:
        return hmac.new(self.secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def issue_session(self) -> str:
        payload = f"{self.auth_username}|{int(time.time()) + SESSION_TTL}"
        # Strip base64 padding so the token needs no cookie quoting.
        payload_b64 = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
        return payload_b64 + "." + self._sign(payload)

    def verify_session(self, token: str) -> bool:
        try:
            payload_b64, sig = token.split(".", 1)
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = base64.urlsafe_b64decode(payload_b64.encode()).decode()
            username, exp_raw = payload.rsplit("|", 1)
            expires = int(exp_raw)
        except (ValueError, UnicodeDecodeError):
            return False
        if not hmac.compare_digest(self._sign(payload), sig):
            return False
        return username == self.auth_username and time.time() < expires


settings = Settings()

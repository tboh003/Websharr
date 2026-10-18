"""Saved settings for the pyLoad download backend."""

import json
import stat

import pytest

from app.config import config
from app.settings import BACKENDS, settings


@pytest.fixture
def settings_file(tmp_path, monkeypatch):
    path = tmp_path / "settings.json"
    monkeypatch.setattr(config, "settings_file", path)
    return path


def test_backends_constant():
    assert BACKENDS == frozenset({"direct", "pyload"})


def test_defaults_use_env(settings_file, monkeypatch):
    monkeypatch.setattr(config, "download_backend", "pyload")
    monkeypatch.setattr(config, "pyload_url", "http://pyload:8000")
    monkeypatch.setattr(config, "pyload_api_key", "envkey")
    monkeypatch.setattr(config, "pyload_stall_hours", 6)
    settings.load()
    assert settings.download_backend == "pyload"
    assert settings.pyload_url == "http://pyload:8000"
    assert settings.pyload_api_key == "envkey"
    assert settings.pyload_stall_hours == 6


def test_saved_values_override_env(settings_file, monkeypatch):
    # setattr first so monkeypatch restores config after apply() mutates it
    monkeypatch.setattr(config, "download_backend", "direct")
    monkeypatch.setattr(config, "pyload_url", "")
    monkeypatch.setattr(config, "pyload_api_key", "")
    monkeypatch.setattr(config, "pyload_stall_hours", 24)
    settings_file.write_text(json.dumps({
        "download_backend": "pyload",
        "pyload": {"url": "http://saved:8000", "api_key": "savedkey", "stall_hours": 12},
    }))
    settings.load()
    assert settings.download_backend == "pyload"
    assert settings.pyload_url == "http://saved:8000"
    assert settings.pyload_api_key == "savedkey"
    assert settings.pyload_stall_hours == 12
    settings.apply()
    assert config.download_backend == "pyload"
    assert config.pyload_url == "http://saved:8000"
    assert config.pyload_api_key == "savedkey"
    assert config.pyload_stall_hours == 12


def test_invalid_backend_falls_back_to_direct(settings_file):
    settings_file.write_text(json.dumps({"download_backend": "bogus"}))
    settings.load()
    assert settings.download_backend == "direct"


def test_save_roundtrip_and_file_mode(settings_file):
    settings.load()
    settings.pyload_ui_set.update({"download_backend", "pyload_url", "pyload_api_key",
                                   "pyload_stall_hours"})
    settings.download_backend = "pyload"
    settings.pyload_url = "http://pyload:8000"
    settings.pyload_api_key = "secret-key"
    settings.pyload_stall_hours = 3
    settings.save()
    assert stat.S_IMODE(settings_file.stat().st_mode) == 0o600
    settings.load()
    assert settings.download_backend == "pyload"
    assert settings.pyload_url == "http://pyload:8000"
    assert settings.pyload_api_key == "secret-key"
    assert settings.pyload_stall_hours == 3


def test_env_only_values_are_not_frozen_by_save(settings_file, monkeypatch):
    monkeypatch.setattr(config, "download_backend", "pyload")
    monkeypatch.setattr(config, "pyload_url", "http://pyload:8000")
    monkeypatch.setattr(config, "pyload_api_key", "oldkey")
    monkeypatch.setattr(config, "pyload_stall_hours", 24)
    settings.load()
    settings.theme = "osaka-jade"  # an unrelated UI save
    settings.save()
    saved = json.loads(settings_file.read_text())
    assert "download_backend" not in saved and "pyload" not in saved
    monkeypatch.setattr(config, "pyload_api_key", "rotatedkey")  # Vault rotated the key
    monkeypatch.setattr(config, "pyload_stall_hours", 12)
    settings.load()
    assert settings.pyload_api_key == "rotatedkey"
    assert settings.pyload_stall_hours == 12
    assert settings.download_backend == "pyload"


def test_ui_set_field_survives_reload_and_beats_env(settings_file, monkeypatch):
    monkeypatch.setattr(config, "pyload_url", "http://env:8000")
    monkeypatch.setattr(config, "pyload_api_key", "envkey")
    settings.load()
    settings.pyload_ui_set.add("pyload_url")
    settings.pyload_url = "http://ui:8000"
    settings.save()
    assert json.loads(settings_file.read_text())["pyload"] == {"url": "http://ui:8000"}
    monkeypatch.setattr(config, "pyload_api_key", "rotatedkey")
    settings.load()
    assert settings.pyload_url == "http://ui:8000"
    assert settings.pyload_api_key == "rotatedkey"
    assert settings.pyload_ui_set == {"pyload_url"}

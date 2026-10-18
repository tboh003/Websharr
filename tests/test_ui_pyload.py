"""Settings API and queue payload for the pyLoad download backend."""

import pytest

from app.main import app
from app.settings import settings


@pytest.fixture(autouse=True)
def _reset_pyload_settings(monkeypatch):
    """`settings` is process-wide; keep these tests from leaking into others."""
    for name, value in (("download_backend", "direct"), ("pyload_url", ""),
                        ("pyload_api_key", ""), ("pyload_stall_hours", 6)):
        monkeypatch.setattr(settings, name, value)
    yield
    from app.config import config
    config.pyload_url = config.pyload_api_key = ""
    config.download_backend = "direct"


def _get(client, path):
    return client.get(f"/ui/api/{path}", params={"apikey": "testkey"})


def _post(client, path, body):
    return client.post(f"/ui/api/{path}", params={"apikey": "testkey"}, json=body)


def test_get_settings_hides_api_key(client):
    settings.pyload_url = "http://pyload:8000"
    settings.pyload_api_key = "s3cret-key"
    data = _get(client, "settings").json()
    assert data["download_backend"] == "direct"
    assert data["pyload_url"] == "http://pyload:8000"
    assert data["pyload_api_key_set"] is True
    assert data["pyload_stall_hours"] >= 1
    assert "s3cret-key" not in client.get(
        "/ui/api/settings", params={"apikey": "testkey"}).text


def test_post_switches_backend_and_persists(client):
    resp = _post(client, "settings", {
        "download_backend": "pyload", "pyload_url": "http://pyload:8000",
        "pyload_api_key": "k1", "pyload_stall_hours": 5})
    assert resp.status_code == 200
    assert app.state.downloads.backend == "pyload"
    assert settings.download_backend == "pyload"
    assert settings.pyload_stall_hours == 5
    # A later POST without the key keeps the saved one.
    resp = _post(client, "settings", {"download_backend": "direct"})
    assert resp.status_code == 200
    assert app.state.downloads.backend == "direct"
    assert settings.pyload_api_key == "k1"
    assert _get(client, "settings").json()["pyload_api_key_set"] is True


def test_post_pyload_without_url_is_400(client):
    resp = _post(client, "settings", {"download_backend": "pyload"})
    assert resp.status_code == 400
    assert "error" in resp.json()
    assert app.state.downloads.backend == "direct"


def test_post_invalid_backend_is_400(client):
    resp = _post(client, "settings", {"download_backend": "nope"})
    assert resp.status_code == 400
    assert "error" in resp.json()


def test_queue_item_reports_backend(client, fake_webshare):
    from app.webshare import SearchResult
    fake_webshare.results = [SearchResult("id1", "Movie.2020.mkv", 1000)]
    r = client.get("/ui/api/search", params={"apikey": "testkey", "q": "Movie"}).json()["results"][0]
    client.post("/sabnzbd/api", params={
        "mode": "addurl", "apikey": "testkey", "name": r["nzb_url"], "cat": "movies"})
    # The unreachable fake link may fail the job fast, so look in both lists.
    jobs = (_get(client, "queue").json()["jobs"] + _get(client, "history").json()["jobs"])
    assert jobs and jobs[0]["backend"] == "direct"

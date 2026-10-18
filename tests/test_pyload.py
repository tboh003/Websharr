import asyncio
import json

import httpx
import pytest

from app.pyload import PyloadClient, PyloadError, PyloadPackageGone

URL = "http://pyload.test"
KEY = "pl_secret"


def make(handler):
    return PyloadClient(URL, KEY, transport=httpx.MockTransport(handler))


def run(coro):
    return asyncio.run(coro)


def test_add_package_sends_json_and_key():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["method"] = request.method
        seen["body"] = json.loads(request.content)
        seen["key"] = request.headers["X-API-Key"]
        return httpx.Response(200, json=5)

    async def go():
        c = make(handler)
        try:
            return await c.add_package("websharr-x", "http://l")
        finally:
            await c.close()

    assert run(go()) == 5
    assert seen["path"] == "/api/add_package"
    assert seen["method"] == "POST"
    assert seen["body"] == {"name": "websharr-x", "links": ["http://l"], "dest": 1}
    assert seen["key"] == KEY


def test_package_files_parses_links():
    def handler(request):
        assert request.url.path == "/api/get_package_data"
        assert request.url.params["package_id"] == "5"
        return httpx.Response(200, json={"pid": 5, "links": [
            {"fid": 10, "name": "a.mkv", "size": 100, "status": 12,
             "statusmsg": "downloading", "error": ""}]})

    files = run(make(handler).package_files(5))
    assert len(files) == 1
    f = files[0]
    assert (f.fid, f.name, f.size, f.status, f.statusmsg, f.error) == (
        10, "a.mkv", 100, 12, "downloading", "")


def test_package_gone_on_500():
    def handler(request):
        return httpx.Response(500, json={"error": "Internal server error"})

    with pytest.raises(PyloadPackageGone):
        run(make(handler).package_files(5))


def test_progress_uses_percent_and_speed():
    rows = [{"fid": 1, "package_id": 9, "size": 50, "percent": 100, "speed": 9},
            {"fid": 10, "package_id": 5, "size": 1000, "percent": 25, "speed": 300}]

    def handler(request):
        assert request.url.path == "/api/status_downloads"
        return httpx.Response(200, json=rows)

    assert run(make(handler).progress(5)) == (250, 300.0)

    def empty(request):
        return httpx.Response(200, json=[])

    assert run(make(empty).progress(5)) is None  # no row for the package


@pytest.mark.parametrize("code", [401, 403])
def test_401_raises_pyload_error(code):
    def handler(request):
        return httpx.Response(code)

    with pytest.raises(PyloadError, match="rejected the API key") as exc:
        run(make(handler).add_package("n", "l"))
    assert KEY not in str(exc.value)


def test_connect_error_raises_pyload_error():
    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    with pytest.raises(PyloadError, match="pyLoad unreachable") as exc:
        run(make(handler).progress(5))
    assert KEY not in str(exc.value)


def test_delete_package_body():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["method"] = request.method
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=None)

    run(make(handler).delete_package(5))
    assert seen == {"path": "/api/delete_packages", "method": "POST",
                    "body": {"package_ids": [5]}}


def test_connect_error_and_5xx_are_transient():
    from app.pyload import PyloadUnreachable

    def down(request):
        raise httpx.ConnectError("boom", request=request)

    with pytest.raises(PyloadUnreachable):
        run(make(down).progress(5))

    def busy(request):
        return httpx.Response(503)

    with pytest.raises(PyloadUnreachable, match="HTTP 503"):
        run(make(busy).progress(5))


def test_401_and_4xx_are_not_transient():
    from app.pyload import PyloadUnreachable

    for code in (401, 404):
        def handler(request, code=code):
            return httpx.Response(code)

        with pytest.raises(PyloadError) as exc:
            run(make(handler).add_package("n", "l"))
        assert not isinstance(exc.value, PyloadUnreachable)

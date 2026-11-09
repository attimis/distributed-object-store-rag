"""Tests for the OAuth 2.1 client: discovery allowlist, PKCE login flow,
token storage permissions, proactive refresh and failure reporting.

All HTTP goes through ``httpx.MockTransport``; the browser is replaced by a
thread that plays the authorization server's redirect back to the loopback
listener.
"""

from __future__ import annotations

import json
import stat
import threading
import time
import urllib.parse
import urllib.request

import httpx
import pytest

from onebucket_demo import onebucket_auth as auth

EVENTS = "https://events.onebucket.io"
AS = "https://oauth.onebucket.io"


class FakeAuthServer:
    """Just enough of an OAuth 2.1 server for the client under test."""

    def __init__(self, *, issue_refresh: bool = True, refresh_status: int = 200):
        self.issue_refresh = issue_refresh
        self.refresh_status = refresh_status
        self.registered: list[dict] = []
        self.token_requests: list[dict] = []
        self.counter = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.url.host == "events.onebucket.io" and path == "/.well-known/oauth-protected-resource":
            return httpx.Response(200, json={"resource": EVENTS, "authorization_servers": [AS], "scopes_supported": ["mcp"]})
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(200, json={
                "issuer": AS, "authorization_endpoint": f"{AS}/authorize", "token_endpoint": f"{AS}/token",
                "registration_endpoint": f"{AS}/register", "code_challenge_methods_supported": ["S256"],
            })
        if path == "/register":
            body = json.loads(request.content)
            self.registered.append(body)
            return httpx.Response(201, json={"client_id": "client-123", **body})
        if path == "/token":
            form = dict(urllib.parse.parse_qsl(request.content.decode()))
            self.token_requests.append(form)
            self.counter += 1
            if form.get("grant_type") == "refresh_token":
                if self.refresh_status != 200:
                    return httpx.Response(self.refresh_status, json={"error": "invalid_grant"})
                return httpx.Response(200, json={"access_token": f"at-refreshed-{self.counter}", "token_type": "Bearer", "expires_in": 600})
            if form.get("grant_type") == "authorization_code":
                assert form["code"] == "the-code" and form["code_verifier"]
                payload = {"access_token": "at-initial", "token_type": "Bearer", "expires_in": 600}
                if self.issue_refresh:
                    payload["refresh_token"] = "rt-initial"
                return httpx.Response(200, json=payload)
        return httpx.Response(404, text="nope")


@pytest.fixture
def fake():
    return FakeAuthServer()


def _mock_client(fake: FakeAuthServer) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(fake.handler))


def _play_browser(monkeypatch, *, echo_state: bool = True):
    """Replace webbrowser.open with a thread that parses the authorization URL
    and immediately 'redirects' to the loopback callback like a browser would."""
    seen: dict = {}

    def fake_open(url: str) -> bool:
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        seen.update(q)
        assert q["code_challenge_method"] == "S256" and q["resource"] == EVENTS
        redirect = q["redirect_uri"]
        assert redirect.startswith("http://127.0.0.1:")
        state = q["state"] if echo_state else "wrong-state"
        cb = f"{redirect}?code=the-code&state={urllib.parse.quote(state)}"

        def hit():
            time.sleep(0.1)
            urllib.request.urlopen(cb, timeout=5).read()

        threading.Thread(target=hit, daemon=True).start()
        return True

    monkeypatch.setattr(auth.webbrowser, "open", fake_open)
    return seen


# --- URL safety ---------------------------------------------------------------


@pytest.mark.parametrize("url", ["http://events.onebucket.io", "https://evil.example.com/", "https://onebucket.io.evil.com"])
def test_check_url_rejects_untrusted(url, monkeypatch):
    monkeypatch.delenv(auth.ENV_ALLOW_HOST, raising=False)
    with pytest.raises(auth.AuthConfigError):
        auth.check_url(url)


def test_check_url_accepts_trusted_and_single_override(monkeypatch):
    assert auth.check_url("https://events.us-ashburn-1.onebucket.io/") == "https://events.us-ashburn-1.onebucket.io/"
    monkeypatch.setenv(auth.ENV_ALLOW_HOST, "dev.internal")
    assert auth.check_url("https://dev.internal/") == "https://dev.internal/"
    with pytest.raises(auth.AuthConfigError):
        auth.check_url("http://dev.internal/")  # override never relaxes https


@pytest.mark.parametrize("bad", ["*.internal", "https://dev.internal", "dev.internal/path", "dev.internal:8443"])
def test_override_must_be_a_bare_hostname(bad, monkeypatch):
    monkeypatch.setenv(auth.ENV_ALLOW_HOST, bad)
    with pytest.raises(auth.AuthConfigError):
        auth.allowed_host_override()


def test_discover_rejects_poisoned_metadata(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("oauth-protected-resource"):
            return httpx.Response(200, json={"authorization_servers": ["https://evil.example.com"]})
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(handler)) as c, pytest.raises(auth.AuthConfigError):
        auth.discover(EVENTS, c)


# --- login --------------------------------------------------------------------


def test_login_stores_tokens_owner_only(tmp_path, fake, monkeypatch):
    _play_browser(monkeypatch)
    token_path = tmp_path / "cfg" / "token.json"
    with _mock_client(fake) as c:
        auth.login(EVENTS, token_path, client=c, timeout_seconds=10)
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(token_path.parent.stat().st_mode) == 0o700
    record = json.loads(token_path.read_text())
    assert record["client_id"] == "client-123"
    assert record["access_token"] == "at-initial" and record["refresh_token"] == "rt-initial"
    assert record["resource"] == EVENTS and record["token_endpoint"] == f"{AS}/token"
    assert record["expires_at"] > time.time() + 500
    reg = fake.registered[0]
    assert reg["token_endpoint_auth_method"] == "none"
    assert reg["grant_types"] == ["authorization_code", "refresh_token"]
    assert reg["redirect_uris"][0].startswith("http://127.0.0.1:")


def test_login_rejects_state_mismatch(tmp_path, fake, monkeypatch):
    _play_browser(monkeypatch, echo_state=False)
    with _mock_client(fake) as c, pytest.raises(auth.AuthError, match="state"):
        auth.login(EVENTS, tmp_path / "token.json", client=c, timeout_seconds=5)
    assert not (tmp_path / "token.json").exists()
    assert not fake.token_requests, "no code exchange may happen after a state mismatch"


# --- token manager ------------------------------------------------------------


def _write_record(path, **overrides):
    record = {
        "client_id": "client-123", "issuer": AS, "token_endpoint": f"{AS}/token", "resource": EVENTS,
        "access_token": "at-stored", "refresh_token": "rt-stored", "token_type": "Bearer",
        "expires_at": time.time() + 3600, "obtained_at": time.time(),
    }
    record.update(overrides)
    auth.write_token_file(path, record)
    return record


def test_manager_refreshes_before_expiry_and_persists(tmp_path, fake):
    path = tmp_path / "token.json"
    _write_record(path, expires_at=time.time() + 30)  # inside the 120 s leeway
    with _mock_client(fake) as c:
        manager = auth.TokenManager(EVENTS, path, client=c)
        assert manager.access_token() == "at-refreshed-1"
        assert manager.access_token() == "at-refreshed-1", "no second refresh while fresh"
    on_disk = json.loads(path.read_text())
    assert on_disk["access_token"] == "at-refreshed-1" and on_disk["refresh_token"] == "rt-stored"
    assert fake.token_requests[0]["grant_type"] == "refresh_token"
    assert fake.token_requests[0]["resource"] == EVENTS


def test_manager_uses_stored_token_while_valid(tmp_path, fake):
    path = tmp_path / "token.json"
    _write_record(path)
    with _mock_client(fake) as c:
        assert auth.TokenManager(EVENTS, path, client=c).access_token() == "at-stored"
    assert not fake.token_requests


def test_manager_reports_expired_when_refresh_rejected(tmp_path):
    fake = FakeAuthServer(refresh_status=400)
    path = tmp_path / "token.json"
    _write_record(path, expires_at=time.time() - 1)
    with _mock_client(fake) as c, pytest.raises(auth.AuthExpired, match="refresh rejected"):
        auth.TokenManager(EVENTS, path, client=c).access_token()


def test_manager_reports_expired_without_refresh_token(tmp_path, fake):
    path = tmp_path / "token.json"
    _write_record(path, refresh_token=None, expires_at=time.time() - 1)
    with _mock_client(fake) as c, pytest.raises(auth.AuthExpired, match="no refresh token"):
        auth.TokenManager(EVENTS, path, client=c).access_token()


def test_manager_requires_token_for_the_configured_resource(tmp_path, fake):
    path = tmp_path / "token.json"
    _write_record(path, resource="https://events.us-ashburn-1.onebucket.io")
    with _mock_client(fake) as c, pytest.raises(auth.AuthResourceMismatch):
        auth.TokenManager(EVENTS, path, client=c).access_token()


def test_manager_refuses_world_readable_token_file(tmp_path, fake):
    path = tmp_path / "token.json"
    _write_record(path)
    path.chmod(0o644)
    with _mock_client(fake) as c, pytest.raises(auth.AuthConfigError, match="readable"):
        auth.TokenManager(EVENTS, path, client=c).access_token()


def test_missing_token_file_tells_operator_to_log_in(tmp_path, fake):
    with _mock_client(fake) as c, pytest.raises(auth.AuthExpired, match="onebucket-login"):
        auth.TokenManager(EVENTS, tmp_path / "none.json", client=c).access_token()


def test_token_file_path_resolution(tmp_path, monkeypatch):
    monkeypatch.delenv(auth.ENV_TOKEN_FILE, raising=False)
    assert auth.token_file_path() == auth.DEFAULT_TOKEN_FILE
    monkeypatch.setenv(auth.ENV_TOKEN_FILE, str(tmp_path / "t.json"))
    assert auth.token_file_path() == tmp_path / "t.json"
    assert auth.token_file_path(tmp_path / "x.json") == tmp_path / "x.json"


def test_redact_hides_bearer_tokens():
    assert auth.redact("Authorization: Bearer abcdefghijklmnop failed") == "Authorization: Bearer <redacted> failed"
    assert auth.redact("plain message") == "plain message"

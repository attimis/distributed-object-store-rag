"""OAuth 2.1 client for OneBucket's MCP servers (``events.<region>.onebucket.io``
and ``data.<region>.onebucket.io``).

OneBucket protects its MCP servers with OAuth 2.1: a public client registers
itself dynamically (RFC 7591), the operator signs in once in a browser with
PKCE, and the server hands back a short-lived access token (ten minutes at
the time of writing) plus a refresh token. A long-running process such as the
event consumer keeps itself signed in by refreshing before the access token
expires; no secret is ever typed into a terminal or a config file.

What this module provides
-------------------------
* :func:`login` - the one-time interactive flow. Discovers the authorization
  server from the resource's protected-resource metadata (RFC 9728), registers
  a client, opens the browser, receives the code on a loopback listener, and
  stores the tokens.
* :class:`TokenManager` - what the daemon uses. Loads the stored token,
  refreshes it proactively, and raises :class:`AuthExpired` when it cannot.

Safety rules baked in
---------------------
* Every URL this module talks to must be ``https`` and must be on
  ``*.onebucket.io`` (or exactly one explicitly configured developer host,
  which prints a warning). A poisoned environment variable or metadata
  document can therefore never redirect tokens elsewhere.
* The browser callback listener binds to ``127.0.0.1`` only, uses S256 PKCE
  and a random ``state`` that the callback must echo back.
* Token files are created with mode ``0600`` inside a ``0700`` directory via
  a temp file + atomic rename; a token file readable by the group or others
  is refused. Nothing in this module logs or includes token material in an
  exception message.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import logging
import os
import secrets
import stat
import tempfile
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

TRUSTED_DOMAIN_SUFFIX = ".onebucket.io"
ENV_ALLOW_HOST = "ONEBUCKET_OAUTH_ALLOW_HOST"
ENV_TOKEN_FILE = "ONEBUCKET_TOKEN_FILE"
DEFAULT_TOKEN_FILE = Path.home() / ".config" / "onebucket" / "token.json"
DEFAULT_SCOPE = "mcp"
CLIENT_NAME = "onebucket-demo-event-consumer"
# Refresh this long before the access token expires.
REFRESH_LEEWAY_SECONDS = 120
LOGIN_TIMEOUT_SECONDS = 120


class AuthError(RuntimeError):
    """Base class for everything this module raises."""


class AuthConfigError(AuthError):
    """A URL, host or file permission failed a safety check."""


class AuthExpired(AuthError):
    """No usable token: missing, unreadable, expired without a refresh token,
    or the refresh was rejected. The operator must run the login again."""


class AuthResourceMismatch(AuthExpired):
    """The stored token was issued for a different resource origin than the
    one now configured; log in again against the new events URL."""


# ---------------------------------------------------------------------------
# URL safety
# ---------------------------------------------------------------------------


def _origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def allowed_host_override() -> str | None:
    """The single developer-override host, validated. ``None`` when unset."""
    raw = os.environ.get(ENV_ALLOW_HOST, "").strip()
    if not raw:
        return None
    if "/" in raw or "*" in raw or ":" in raw or not raw:
        raise AuthConfigError(
            f"{ENV_ALLOW_HOST} must be a bare hostname (no scheme, port, path or wildcard)"
        )
    logger.warning(
        "%s=%s: OAuth and MCP traffic may go to a host outside %s",
        ENV_ALLOW_HOST, raw, TRUSTED_DOMAIN_SUFFIX,
    )
    return raw.lower()


def check_url(url: str, purpose: str = "URL") -> str:
    """Return ``url`` if it is https on a trusted host, else raise."""
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise AuthConfigError(f"{purpose} must use https: {url!r}")
    override = allowed_host_override()
    if not host.endswith(TRUSTED_DOMAIN_SUFFIX) and host != override:
        raise AuthConfigError(
            f"{purpose} host {host!r} is not under {TRUSTED_DOMAIN_SUFFIX}; refusing to send "
            f"credentials there (set {ENV_ALLOW_HOST} for a single development host)"
        )
    return url


# ---------------------------------------------------------------------------
# Discovery (RFC 9728 + RFC 8414)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AuthServer:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    registration_endpoint: str
    resource: str


def discover(resource_url: str, client: httpx.Client | None = None) -> AuthServer:
    """Find the authorization server for an MCP resource and validate it."""
    resource = _origin(check_url(resource_url, "events URL"))
    own = client is None
    client = client or httpx.Client(timeout=30)
    try:
        prm = client.get(f"{resource}/.well-known/oauth-protected-resource")
        prm.raise_for_status()
        servers = prm.json().get("authorization_servers") or []
        if not servers:
            raise AuthConfigError(f"{resource} advertises no authorization server")
        issuer = check_url(str(servers[0]).rstrip("/"), "authorization server")
        meta = client.get(f"{issuer}/.well-known/oauth-authorization-server")
        meta.raise_for_status()
        doc = meta.json()
        if "S256" not in (doc.get("code_challenge_methods_supported") or []):
            raise AuthConfigError("authorization server does not support PKCE S256")
        return AuthServer(
            issuer=issuer,
            authorization_endpoint=check_url(doc["authorization_endpoint"], "authorization endpoint"),
            token_endpoint=check_url(doc["token_endpoint"], "token endpoint"),
            registration_endpoint=check_url(doc["registration_endpoint"], "registration endpoint"),
            resource=resource,
        )
    finally:
        if own:
            client.close()


# ---------------------------------------------------------------------------
# Token file
# ---------------------------------------------------------------------------


def token_file_path(path: str | os.PathLike | None = None) -> Path:
    if path is not None:
        return Path(path).expanduser()
    env = os.environ.get(ENV_TOKEN_FILE, "").strip()
    return Path(env).expanduser() if env else DEFAULT_TOKEN_FILE


def write_token_file(path: Path, data: dict) -> None:
    """Atomic, owner-only write: 0700 directory, temp file opened 0600, rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, tmp_name = tempfile.mkstemp(prefix=".token-", suffix=".tmp", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def read_token_file(path: Path) -> dict:
    """Load the token file, refusing one that other users could read."""
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError as exc:
        raise AuthExpired(
            f"no OneBucket token at {path}; run `uv run python scripts/onebucket-login.py`"
        ) from exc
    if mode & 0o077:
        raise AuthConfigError(
            f"token file {path} is readable by group/others (mode {mode:o}); "
            "fix with: chmod 600 and re-run the login"
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuthExpired(f"token file {path} is unreadable or corrupt: {type(exc).__name__}") from exc


# ---------------------------------------------------------------------------
# Interactive login
# ---------------------------------------------------------------------------


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    received: dict = {}

    def do_GET(self):  # noqa: N802 - http.server API
        parts = urllib.parse.urlsplit(self.path)
        if parts.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return
        type(self).received = dict(urllib.parse.parse_qsl(parts.query))
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OneBucket login complete. You can close this tab.\n")

    def log_message(self, *args):  # silence the default stderr access log
        pass


def register_client(server: AuthServer, redirect_uri: str, client: httpx.Client) -> str:
    response = client.post(
        server.registration_endpoint,
        json={
            "client_name": CLIENT_NAME,
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "scope": DEFAULT_SCOPE,
        },
    )
    if response.status_code >= 400:
        raise AuthError(f"client registration failed: HTTP {response.status_code}")
    client_id = response.json().get("client_id")
    if not client_id:
        raise AuthError("client registration returned no client_id")
    return str(client_id)


def login(
    events_url: str,
    token_path: str | os.PathLike | None = None,
    *,
    open_browser: bool = True,
    timeout_seconds: int = LOGIN_TIMEOUT_SECONDS,
    client: httpx.Client | None = None,
) -> Path:
    """Run the browser login and store the resulting tokens. Returns the path."""
    path = token_file_path(token_path)
    own = client is None
    client = client or httpx.Client(timeout=30)
    try:
        server = discover(events_url, client)
        listener = http.server.HTTPServer(("127.0.0.1", 0), _CallbackHandler)
        port = listener.server_address[1]
        redirect_uri = f"http://127.0.0.1:{port}/callback"
        client_id = register_client(server, redirect_uri, client)

        verifier = secrets.token_urlsafe(64)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        state = secrets.token_urlsafe(32)
        auth_url = server.authorization_endpoint + "?" + urllib.parse.urlencode(
            {
                "response_type": "code",
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": DEFAULT_SCOPE,
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "resource": server.resource,
            }
        )
        _CallbackHandler.received = {}
        listener.timeout = timeout_seconds
        worker = threading.Thread(target=listener.handle_request, daemon=True)
        worker.start()
        print(f"Open this URL to sign in to OneBucket (waiting {timeout_seconds}s):\n  {auth_url}")
        if open_browser:
            webbrowser.open(auth_url)
        worker.join(timeout_seconds + 5)
        listener.server_close()
        received = _CallbackHandler.received
        if not received:
            raise AuthError("no login callback received before the timeout")
        if not secrets.compare_digest(received.get("state", ""), state):
            raise AuthError("login callback state mismatch; aborting")
        if "code" not in received:
            raise AuthError(f"login was not completed: {received.get('error', 'no code returned')}")

        response = client.post(
            server.token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": received["code"],
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "code_verifier": verifier,
                "resource": server.resource,
            },
        )
        if response.status_code != 200:
            raise AuthError(f"token exchange failed: HTTP {response.status_code}")
        record = _token_record(response.json(), client_id=client_id, server=server)
        write_token_file(path, record)
        return path
    finally:
        if own:
            client.close()


def _token_record(token: dict, *, client_id: str, server: AuthServer) -> dict:
    expires_in = int(token.get("expires_in") or 0)
    return {
        "client_id": client_id,
        "issuer": server.issuer,
        "token_endpoint": server.token_endpoint,
        "resource": server.resource,
        "access_token": token["access_token"],
        "refresh_token": token.get("refresh_token"),
        "token_type": token.get("token_type", "Bearer"),
        "expires_at": time.time() + expires_in if expires_in else None,
        "obtained_at": time.time(),
    }


# ---------------------------------------------------------------------------
# Token manager for long-running processes
# ---------------------------------------------------------------------------


class TokenManager:
    """Hands out a valid access token, refreshing it ahead of expiry.

    Thread-safe. Every method that cannot produce a token raises
    :class:`AuthExpired` so the caller can report an explicit auth state and
    stop, rather than looping against the authorization server.
    """

    def __init__(
        self,
        events_url: str,
        token_path: str | os.PathLike | None = None,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.resource = _origin(check_url(events_url, "events URL"))
        self.path = token_file_path(token_path)
        self._client = client or httpx.Client(timeout=30)
        self._lock = threading.Lock()
        self._record: dict | None = None

    def _load(self) -> dict:
        record = read_token_file(self.path)
        if record.get("resource") != self.resource:
            raise AuthResourceMismatch(
                f"stored token is for {record.get('resource')!r}, configured events URL is "
                f"{self.resource!r}; run the login again"
            )
        check_url(str(record.get("token_endpoint", "")), "stored token endpoint")
        return record

    def access_token(self) -> str:
        with self._lock:
            if self._record is None:
                self._record = self._load()
            record = self._record
            expires_at = record.get("expires_at")
            if expires_at is not None and time.time() >= float(expires_at) - REFRESH_LEEWAY_SECONDS:
                self._record = record = self._refresh_locked(record)
            return str(record["access_token"])

    def force_refresh(self) -> str:
        """Refresh now (after a 401), regardless of the recorded expiry."""
        with self._lock:
            record = self._record or self._load()
            self._record = record = self._refresh_locked(record)
            return str(record["access_token"])

    def _refresh_locked(self, record: dict) -> dict:
        refresh_token = record.get("refresh_token")
        if not refresh_token:
            raise AuthExpired("access token expired and no refresh token was issued; log in again")
        try:
            response = self._client.post(
                record["token_endpoint"],
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": record["client_id"],
                    "resource": self.resource,
                },
            )
        except httpx.HTTPError as exc:
            raise AuthExpired(f"token refresh failed: {type(exc).__name__}") from exc
        if response.status_code != 200:
            raise AuthExpired(f"token refresh rejected: HTTP {response.status_code}; log in again")
        token = response.json()
        new = dict(record)
        new["access_token"] = token["access_token"]
        if token.get("refresh_token"):
            new["refresh_token"] = token["refresh_token"]
        expires_in = int(token.get("expires_in") or 0)
        new["expires_at"] = time.time() + expires_in if expires_in else None
        new["obtained_at"] = time.time()
        write_token_file(self.path, new)
        logger.info("OneBucket access token refreshed (expires in %ss)", expires_in or "?")
        return new


def redact(text: str) -> str:
    """Strip anything that looks like a bearer token from a log line."""
    import re

    return re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}", r"\1<redacted>", text)

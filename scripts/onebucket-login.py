#!/usr/bin/env python3
"""Sign in to OneBucket once so the event consumer can run unattended.

Usage:
    uv run python scripts/onebucket-login.py [--events-url URL] [--token-file PATH] [--no-browser]

What happens:
  1. The events server's OAuth metadata is discovered and checked (https,
     *.onebucket.io only).
  2. A public OAuth client is registered for this machine.
  3. Your browser opens the OneBucket sign-in page; the authorization code
     comes back to a listener on 127.0.0.1 (never any other interface).
  4. The access token and refresh token are stored, readable by you only,
     at ~/.config/onebucket/token.json (or $ONEBUCKET_TOKEN_FILE).

Nothing is printed or pasted: no token ever appears on this terminal.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from onebucket_demo import onebucket_auth  # noqa: E402

DEFAULT_EVENTS_URL = "https://events.onebucket.io"


def main() -> int:
    parser = argparse.ArgumentParser(prog="onebucket-login", description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--events-url",
        default=os.environ.get("ONEBUCKET_EVENTS_URL", DEFAULT_EVENTS_URL),
        help="events MCP server to sign in to (default: the global hostname, or $ONEBUCKET_EVENTS_URL)",
    )
    parser.add_argument(
        "--token-file",
        default=None,
        help="where to store the token (default: $ONEBUCKET_TOKEN_FILE or ~/.config/onebucket/token.json)",
    )
    parser.add_argument("--no-browser", action="store_true", help="print the URL instead of opening a browser")
    parser.add_argument("--timeout", type=int, default=onebucket_auth.LOGIN_TIMEOUT_SECONDS)
    args = parser.parse_args()
    try:
        path = onebucket_auth.login(
            args.events_url,
            args.token_file,
            open_browser=not args.no_browser,
            timeout_seconds=args.timeout,
        )
    except onebucket_auth.AuthError as exc:
        print(f"login failed: {exc}", file=sys.stderr)
        return 1
    print(f"Signed in. Token stored at {path} (mode 0600).")
    print("You can now start the consumer: bash scripts/start-event-consumer.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())

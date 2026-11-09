"""Input gates: file size and file type.

The demo deliberately accepts only small plain-text and Markdown files. Both
checks return ``(ok, reason)`` instead of raising so that every caller, the
local ingest, the daemons and the chunker Lambda, can reject a file
gracefully: log the reason, return success to the trigger, and never retry
something that will never pass (see ADR-006).
"""

from __future__ import annotations

from pathlib import Path

from .constants import ALLOWED_EXTENSIONS, MAX_FILE_BYTES


def check_size(path: Path) -> tuple[bool, str | None]:
    size = path.stat().st_size
    if size > MAX_FILE_BYTES:
        size_mb = size / (1024 * 1024)
        return False, (
            f"File '{path.name}' is {size_mb:.2f} MB; the demo limit is 10 MB. "
            f"Please upload a smaller file."
        )
    return True, None


def check_type(path: Path) -> tuple[bool, str | None]:
    ext = path.suffix.lower()
    if ext in ALLOWED_EXTENSIONS:
        return True, None
    ext_bare = ext.lstrip(".") if ext else "(none)"
    msg = (
        f"File '{path.name}' has unsupported type '.{ext_bare}'. "
        f"Supported types: .txt, .md."
    )
    if ext == ".pdf":
        msg += " PDF support is not included in this demo; convert to text first."
    return False, msg

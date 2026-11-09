"""Every relative link in a tracked Markdown file must point at a tracked file.

A link into a gitignored tree (sprint notes, roadmap, agent scaffolding)
works on the author's machine and 404s for everyone who clones the public
repository. This test enumerates tracked ``*.md`` files, resolves each
relative link target, and requires it to be tracked too. Anchors are
checked only for existence of the file, not the heading.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LINK_RE = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


def _tracked() -> set[str]:
    out = subprocess.run(["git", "-C", str(REPO_ROOT), "ls-files", "-z"], check=True, capture_output=True).stdout
    return {rel for rel in out.decode().split("\0") if rel}


TRACKED = _tracked()
MARKDOWN = sorted(p for p in TRACKED if p.endswith(".md"))


def _targets(md_rel: str) -> list[tuple[int, str]]:
    text = (REPO_ROOT / md_rel).read_text(encoding="utf-8")
    found = []
    for m in LINK_RE.finditer(text):
        target = m.group(1)
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        line = text.count("\n", 0, m.start()) + 1
        found.append((line, target.split("#", 1)[0]))
    return found


@pytest.mark.parametrize("md_rel", MARKDOWN)
def test_relative_links_resolve_to_tracked_files(md_rel: str):
    base = (REPO_ROOT / md_rel).parent
    broken = []
    for line, target in _targets(md_rel):
        if not target:
            continue
        resolved = (base / target).resolve()
        try:
            rel = resolved.relative_to(REPO_ROOT).as_posix()
        except ValueError:
            broken.append(f"{md_rel}:{line}: {target!r} escapes the repository")
            continue
        if rel not in TRACKED and not (resolved.is_dir() and any(t.startswith(rel + "/") for t in TRACKED)):
            broken.append(f"{md_rel}:{line}: {target!r} -> {rel} is not a tracked file")
    assert not broken, "\n".join(broken)

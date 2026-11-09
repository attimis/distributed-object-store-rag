"""Leak guard: nothing private or internal may ship in a tracked file.

What it catches, and how, without ever containing the secrets itself:

* **Structural patterns** (regular expressions): credential shapes, account
  ids, JWTs, e-mail addresses, Slack links, absolute home paths, dated bucket
  suffixes, region-formatted cluster tokens, links into the gitignored
  workflow trees, and internal process vocabulary.
* **Hashed tokens**: the specific names that must not appear (people,
  internal clusters, a deployment's bucket names) are stored as SHA-256
  digests of the lower-cased token. The guard hashes every token in every
  tracked file and compares digests, so the forbidden words are never
  written down here.
* **Hostname allowlist**: every ``*.onebucket.io`` hostname in the tree must
  be one of the public ones.

The scan covers every tracked file except binary ones. The only files exempt
from any rule are this test and the managed block of ``.gitignore``, which
necessarily names the ignored trees. Adding another path to that tuple is
itself a failure.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
THIS_FILE = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()

# Files allowed to contain strings that would otherwise fail. Frozen on purpose.
EXEMPT_PATHS = (THIS_FILE, ".gitignore")

BINARY_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".zip", ".gz", ".lance", ".onnx", ".pdf"}

# --- structural patterns ----------------------------------------------------

STRUCTURAL_PATTERNS: dict[str, re.Pattern] = {
    "AWS access key id": re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b"),
    "OneBucket access key id": re.compile(r"\bATMI[A-Z0-9]{12,}\b"),
    "JWT": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
    "12-digit AWS account id": re.compile(r"(?<![0-9A-Za-z.])\d{12}(?![0-9A-Za-z.])"),
    "e-mail address": re.compile(r"\b[A-Za-z0-9._%+-]+@(?!example\.com\b)[A-Za-z0-9.-]+\.[a-z]{2,}\b"),
    "Slack link": re.compile(r"https?://[a-z0-9-]+\.slack\.com/|\bslack thread\b", re.I),
    "absolute home path": re.compile(r"(?<![\w/])/(Users|home)/[a-z][\w-]*/"),
    "dated bucket suffix": re.compile(r"\b[a-z0-9-]+-ap-\d{8}(-\d+)?\b"),
    "link into an ignored tree": re.compile(
        r"docs/(sprints|roadmap|backlog|personas|ideation|memos|initiatives)/|(?<![\w.])\.claude/|scripts/agentic/|\bCLAUDE\.md\b"
    ),
    "internal process vocabulary": re.compile(
        r"\bVP[ -]?(Eng|Prod|Product|Security|DevOps|Compliance)\b|\bVP of \w+|\bCTO\b|\bCEO\b|"
        r"\bSprint[ -]?0?\d\b|\bsprint[ -]plan\b|\bdev[ -]report\b|\bBLOCKER\b|\bMAJOR-\d|\bMINOR-\w+|\bHITL\b|"
        r"\bQ8 (mandate|ruling)\b|\bPRD-\d{3}\b|\bcowork\b",
        re.I,
    ),
}

# Account ids in this range are AWS's documented examples and appear in tests.
ALLOWED_ACCOUNT_IDS = {"123456789012", "111122223333", "444455556666"}

# --- hostname allowlist -------------------------------------------------------

HOSTNAME_RE = re.compile(r"\b([a-z0-9.-]+\.onebucket\.io)\b", re.I)
PUBLIC_HOSTS = {
    "onebucket.io",
    "www.onebucket.io",
    "app.onebucket.io",
    "docs.onebucket.io",
    "oauth.onebucket.io",
    "status.onebucket.io",
    "s3.onebucket.io",
    "data.onebucket.io",
    "events.onebucket.io",
    # The single documented example of a regional hostname.
    "s3.us-ashburn-1.onebucket.io",
    "data.us-ashburn-1.onebucket.io",
    "events.us-ashburn-1.onebucket.io",
}
# A region-formatted token anywhere (not only in a hostname) must be one of
# the two allowed: the SigV4 default and the documented example.
REGION_TOKEN_RE = re.compile(r"\b(us|eu|ap|ca|sa|af|me|cn|il|uk)-[a-z]+-\d{1,2}\b")
ALLOWED_REGION_TOKENS = {"us-east-1", "us-ashburn-1", "us-west-1", "us-west-2", "ap-northeast-1", "eu-west-1"}

# --- hashed deny-list ---------------------------------------------------------

# SHA-256 of lower-cased tokens that must not appear: people's names used as
# identifiers, internal cluster names, private project names and the policy
# names of the original deployment. The comment on each line says only what
# KIND of token it is. To add one:
#   python -c "import hashlib;print(hashlib.sha256(b'<token>').hexdigest())"


def _digest(token: str) -> str:
    return hashlib.sha256(token.lower().encode("utf-8")).hexdigest()


FORBIDDEN_TOKEN_DIGESTS = frozenset(
    {
        "92addb8053103c46c0c8d41a21a644df46bd5dbabe72224a3946f916e996f78e",  # legacy cluster name
        "a37d20d1e1cfa1cf037f956f715ae79fc78c616f46f881c263a814f0c2c802dc",  # legacy cluster name
        "9353d1a6de86e8d63640942d0ad7244ff1b76a29d263ede3b34153525ba39598",  # legacy cluster name
        "93ed555f2af174c6a0f61661818c2f8259a05c17aa710debb2d861bec75b4db5",  # private one-off cluster
        "350c754ba4d38897693aa077ef43072a859d23f613443133fecbbd90a3512ca5",  # private cluster locality
        "1faf23fc4ed5880a64cfd5a2340d74628c99f50b8314d38935a2802c553fc950",  # person (endpoint alias)
        "0345573f9908b378bc518bdf97afe3e563d2e7cee60f3f45f1bd2c24786f0868",  # person (endpoint alias)
        "1bda6ce08df52121e0fbd1877021f3e695e9aeb3d7eb634b015e39c6b24e2575",  # person (surname)
        "8b555452b14c8d3570608964a1ce406429731746211d45dd3c92c21fb35cb039",  # person (first name)
        "720036c8101f751b82cdba6e74fbd217419a2d478dd49f6d7ba6697ed3810ece",  # vendor contact (first name)
        "f96691d38c896727bef5f8945a0db0abb57838fe390b95174c8a4b156f688dc4",  # original policy name
        "87d082c4cfcdd249b46bc0d6a42bf68102b32483584d6d07a1c17bb73420d451",  # original policy name
        "7f373f8884ea51a963388e07419636c141fff8ebaee9204b51105d97db0257e4",  # original policy name
        "38da0d352815ae188ddd94fae326efe1be599c874fe742a81e5911bd387b7a9e",  # original policy name
        "1efde7cdd2a643dad4224ecba6980dbb4f9614bb50075a3eb90f680ef21685f1",  # private project name
        "61b364bae2752c1a7ba6818b9cac02208126222604004733c76b55c9e8e67a0f",  # private project name
        "5d080a2f8367acd7f6124f2343f778c848f65f775fc906f3cd0d5249bdcc90d6",  # retired event host
    }
)


# --- helpers ------------------------------------------------------------------


def _tracked_text_files() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    files = []
    for rel in out.decode("utf-8").split("\0"):
        if not rel:
            continue
        p = REPO_ROOT / rel
        if p.suffix.lower() in BINARY_SUFFIXES or not p.is_file():
            continue
        files.append(p)
    return files


def _tokens(text: str):
    """Yield delimiter-bounded tokens and their hyphen/underscore sub-tokens."""
    for tok in re.split(r"[\s\"'`<>()\[\]{},;:/\\=|!?*&^%$#@~]+", text):
        if not tok:
            continue
        tok = tok.strip(".-_")
        if not tok:
            continue
        yield tok
        if "-" in tok or "_" in tok:
            for sub in re.split(r"[-_]+", tok):
                if sub:
                    yield sub


def check_text(rel_path: str, text: str) -> list[str]:
    """Return every finding for one file's text. Reusable by the publication
    rehearsal, which runs it over the orphan clone's full history."""
    findings: list[str] = []
    if rel_path in EXEMPT_PATHS:
        return findings
    for name, pattern in STRUCTURAL_PATTERNS.items():
        for m in pattern.finditer(text):
            if name == "12-digit AWS account id" and m.group(0) in ALLOWED_ACCOUNT_IDS:
                continue
            line = text.count("\n", 0, m.start()) + 1
            findings.append(f"{rel_path}:{line}: {name}: {m.group(0)[:60]!r}")
    for m in HOSTNAME_RE.finditer(text):
        host = m.group(1).lower()
        if host not in PUBLIC_HOSTS:
            line = text.count("\n", 0, m.start()) + 1
            findings.append(f"{rel_path}:{line}: non-public OneBucket hostname {host!r}")
    for m in REGION_TOKEN_RE.finditer(text):
        if m.group(0) not in ALLOWED_REGION_TOKENS:
            line = text.count("\n", 0, m.start()) + 1
            findings.append(f"{rel_path}:{line}: region-formatted token {m.group(0)!r}")
    seen: set[str] = set()
    for tok in _tokens(text):
        key = tok.lower()
        if key in seen:
            continue
        seen.add(key)
        if _digest(key) in FORBIDDEN_TOKEN_DIGESTS:
            findings.append(f"{rel_path}: forbidden token (digest match) {tok!r}")
    return findings


# --- tests --------------------------------------------------------------------


def test_exemptions_are_frozen():
    assert EXEMPT_PATHS == (THIS_FILE, ".gitignore"), "do not widen the leak-guard exemptions"


@pytest.mark.parametrize("path", _tracked_text_files(), ids=lambda p: p.relative_to(REPO_ROOT).as_posix())
def test_tracked_file_has_no_internal_refs(path: Path):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        pytest.skip("binary content")
    findings = check_text(path.relative_to(REPO_ROOT).as_posix(), text)
    assert not findings, "\n".join(findings)


def test_guard_catches_known_shapes():
    """Self-test with synthetic examples so a regex regression is noticed."""
    sample = "\n".join(
        [
            "key AKIAABCDEFGHIJKLMNOP here",
            "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnopqrstuvwxyz1234567890",
            "path /Users/someone/repo",
            "bucket my-demo-ap-20260101",
            "see docs/sprints/sprint-99/plan.md",
            "per VP Eng review",
            "host s3.us-nowhere-9.onebucket.io",
        ]
    )
    findings = check_text("sample.md", sample)
    kinds = {f.split(": ", 1)[1].split(":")[0] for f in findings}
    assert {"AWS access key id", "JWT", "absolute home path", "dated bucket suffix",
            "link into an ignored tree", "internal process vocabulary"} <= kinds
    assert any("non-public OneBucket hostname" in f for f in findings)
    assert any("region-formatted token" in f for f in findings)

"""Loader + writer for the demo's deployment config, ``variables.yml``.

The file describes one deployment (which OneBuckets exist, their peers, the
endpoints, what the pipeline writes where) plus the operator's intent for a
demo run (tickers, question, pricing). It ships as
``docs/demo_runbook/variables.example.yml`` with ``<placeholders>``; the
operator copies it to ``variables.yml`` (gitignored) and fills it in.

Read path (every MCP tool and script that needs config calls this):
    cfg = runbook_config.load()
    cfg["pipeline"]["raw_writes_to_policy"]  # -> "demo-data"

Write path (the ``set_runbook_variable`` MCP tool):
    runbook_config.set_variable("operator_intent.fresh_ticker_for_live_ingest", "GOOG")

``ruamel.yaml`` in round-trip mode keeps the file's comments intact across
writes; plain PyYAML would drop them. Writes are atomic (temp file +
``os.replace``) so a concurrent reader never sees a half-written file.

Path resolution:
    1. The explicit argument to ``load()`` / ``save()``.
    2. ``ONEBUCKET_RUNBOOK_CONFIG`` if set.
    3. ``<repo>/docs/demo_runbook/variables.yml``.

Fail-closed: :func:`load_ready` refuses a file that still contains
``<placeholder>`` values, so no script ever tries to reach a bucket literally
named ``<your-aws-bucket>``.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML


# --- Path resolution ---------------------------------------------------------


_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = _REPO_ROOT / "docs" / "demo_runbook" / "variables.yml"
EXAMPLE_CONFIG_PATH = _REPO_ROOT / "docs" / "demo_runbook" / "variables.example.yml"
ENV_VAR = "ONEBUCKET_RUNBOOK_CONFIG"

# A string value equal to this, or shaped like ``<...>``, means "the operator
# has not filled this in yet". find_missing_required() reports their paths.
MISSING_SENTINEL = "TODO"
_PLACEHOLDER_RE = re.compile(r"^<[^<>]+>$")


class ConfigIncomplete(RuntimeError):
    """variables.yml still contains placeholders or fails validation."""


def resolve_path(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    env = os.environ.get(ENV_VAR)
    if env:
        return Path(env).expanduser().resolve()
    return DEFAULT_CONFIG_PATH


# --- YAML round-trip engine --------------------------------------------------


def _yaml_engine() -> YAML:
    """Return a configured ruamel.yaml engine that preserves comments + style
    when we read-modify-write."""
    yaml = YAML()
    yaml.preserve_quotes = True
    # Keep line length unlimited so existing long comment lines aren't
    # re-wrapped on write.
    yaml.width = 10**6
    # Don't re-indent existing structures.
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


# --- Public API: load / save -------------------------------------------------


def load(path: str | Path | None = None) -> Any:
    """Read variables.yml and return the parsed structure (a ruamel
    CommentedMap that supports both dict-style access and round-trip writes).
    Raises FileNotFoundError if the file is missing, YAMLError on parse fail.
    """
    resolved = resolve_path(path)
    if not resolved.exists():
        raise FileNotFoundError(
            f"variables.yml not found at {resolved}. Copy "
            f"{EXAMPLE_CONFIG_PATH} to that path and fill in the <placeholders>, "
            f"or point ${ENV_VAR} at your file."
        )
    yaml = _yaml_engine()
    with open(resolved, "r", encoding="utf-8") as fh:
        return yaml.load(fh)


def load_ready(path: str | Path | None = None) -> Any:
    """Load, then refuse to return a config that is not ready to run.

    Raises :class:`ConfigIncomplete` listing every placeholder still present
    and every validation error, so a script fails before it touches storage.
    """
    data = load(path)
    missing = find_missing_required(data)
    errors = validate(data)
    if missing or errors:
        lines = [f"{resolve_path(path)} is not ready:"]
        lines += [f"  placeholder still set: {m}" for m in missing]
        lines += [f"  validation: {e}" for e in errors]
        raise ConfigIncomplete("\n".join(lines))
    return data


def save(data: Any, path: str | Path | None = None) -> None:
    """Atomically write the YAML back to disk. Comments are preserved iff
    ``data`` came from a previous load() (i.e., is a ruamel CommentedMap).
    Plain-dict data will still write, but without comments.
    """
    resolved = resolve_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    yaml = _yaml_engine()
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=resolved.parent,
        delete=False,
        prefix=".variables-tmp-",
        suffix=".yml",
        encoding="utf-8",
    ) as tmp:
        yaml.dump(data, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, resolved)


# --- Dotted-path access ------------------------------------------------------


_PATH_TOKEN_RE = re.compile(r"^([^\[\]]+)((?:\[\d+\])*)$")
_INDEX_RE = re.compile(r"\[(\d+)\]")


def _parse_dotted_path(path: str) -> list[str | int]:
    """Parse ``a.b[2].c`` → ``["a", "b", 2, "c"]``."""
    if not path:
        raise ValueError("empty path")
    tokens: list[str | int] = []
    for part in path.split("."):
        m = _PATH_TOKEN_RE.match(part)
        if not m:
            raise ValueError(f"invalid path segment: {part!r}")
        tokens.append(m.group(1))
        for idx in _INDEX_RE.findall(m.group(2)):
            tokens.append(int(idx))
    return tokens


def get_at_path(data: Any, dotted_path: str) -> Any:
    """Read a value from the config via dotted path. Raises KeyError /
    IndexError on a missing segment so the caller can distinguish "not set"
    from "value is None"."""
    cursor: Any = data
    for tok in _parse_dotted_path(dotted_path):
        if isinstance(tok, int):
            cursor = cursor[tok]
        else:
            cursor = cursor[tok]
    return cursor


def _coerce_to_existing_type(existing: Any, new_value: Any) -> Any:
    """If ``new_value`` is a string and ``existing`` is a numeric/bool, try to
    coerce ``new_value`` to match. MCP tool calls arrive as JSON; agents
    sometimes pass numbers as strings ("10000000" instead of 10000000), and
    we should not silently corrupt schema types in variables.yml.

    Only string→{int, float, bool} coercion is attempted. Other type
    conversions (int→string, list-to-anything, etc.) pass through unchanged
    and let the validator surface a mismatch if the new value is wrong.
    """
    if not isinstance(new_value, str):
        return new_value
    # bool check must come before int (bool is subclass of int in Python)
    if isinstance(existing, bool):
        lower = new_value.strip().lower()
        if lower in ("true", "yes", "1"):
            return True
        if lower in ("false", "no", "0"):
            return False
        return new_value  # ambiguous; pass through
    if isinstance(existing, int):
        try:
            return int(new_value)
        except ValueError:
            return new_value
    if isinstance(existing, float):
        try:
            return float(new_value)
        except ValueError:
            return new_value
    return new_value


def set_at_path(data: Any, dotted_path: str, value: Any) -> None:
    """In-place mutation of ``data`` at the dotted path. Walks all but the
    last token to find the parent container, then sets the final segment.

    Preserves the existing value's type when the new value is a string that
    can be parsed into the same type (see ``_coerce_to_existing_type``).
    This guards against MCP-protocol type drift — agents sometimes pass
    numbers as JSON strings, and without coercion we'd silently corrupt
    integer fields like y1_query_count to quoted strings in the YAML.

    Raises if intermediate segments don't exist — we never auto-create
    nested structure; the schema is operator-defined.
    """
    tokens = _parse_dotted_path(dotted_path)
    if not tokens:
        raise ValueError("empty path")
    parent: Any = data
    for tok in tokens[:-1]:
        parent = parent[tok]
    last = tokens[-1]
    try:
        existing = parent[last]
    except (KeyError, IndexError):
        existing = None
    if existing is not None and not isinstance(existing, (dict, list)):
        value = _coerce_to_existing_type(existing, value)
    parent[last] = value


# --- Convenience wrappers used by the MCP tools ------------------------------


def set_variable(
    dotted_path: str,
    value: Any,
    path: str | Path | None = None,
) -> Any:
    """Load → set → save. Returns the loaded data after the update so the
    caller can return it to the agent."""
    data = load(path)
    set_at_path(data, dotted_path, value)
    save(data, path)
    return data


def find_missing_required(data: Any) -> list[str]:
    """Walk the config; return dotted paths of every scalar the operator has
    not filled in: a ``TODO`` sentinel or a ``<placeholder>`` as shipped in
    ``variables.example.yml``.
    """
    missing: list[str] = []

    def _walk(node: Any, prefix: str) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                child_path = f"{prefix}.{k}" if prefix else str(k)
                _walk(v, child_path)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                _walk(v, f"{prefix}[{i}]")
        else:
            if isinstance(node, str) and (
                node == MISSING_SENTINEL or _PLACEHOLDER_RE.match(node.strip())
            ):
                missing.append(prefix)

    _walk(data, "")
    return missing


# --- Validation --------------------------------------------------------------


VALID_DAEMON_MODES = {"lambda", "local-daemon", "onebucket-events"}
VALID_PEER_TYPES = {"sync", "cache", "async"}


def validate(data: Any) -> list[str]:
    """Run consistency checks on the loaded config. Returns a list of error
    strings (empty list = valid). Caller decides whether to error out or
    warn. Doesn't mutate ``data``.
    """
    errors: list[str] = []

    # 1. schema_version
    sv = data.get("schema_version") if isinstance(data, dict) else None
    if sv != 1:
        errors.append(
            f"schema_version must be 1; got {sv!r}. Update the loader if "
            f"introducing a v2 schema."
        )

    endpoints = data.get("endpoints") or {}
    policies = data.get("policies") or {}
    pipeline = data.get("pipeline") or {}
    daemon = data.get("daemon") or {}

    # 2. Per-policy quorum / required coupling (NOTE 2 in the YAML)
    for policy_name, policy in policies.items():
        peers = policy.get("peers") or []
        quorum = policy.get("quorum")
        if not isinstance(quorum, int) or quorum < 1:
            errors.append(
                f"policies.{policy_name}.quorum must be a positive integer; "
                f"got {quorum!r}"
            )
            continue
        required_count = sum(1 for p in peers if p.get("required"))
        if required_count > quorum:
            errors.append(
                f"policies.{policy_name}: quorum={quorum} but {required_count} "
                f"peer(s) marked required=true. quorum must be >= "
                f"count(required peers). Either lower required count or "
                f"raise quorum."
            )
        if quorum > len(peers):
            errors.append(
                f"policies.{policy_name}: quorum={quorum} but only "
                f"{len(peers)} peer(s) defined."
            )

        # 3. Peer alias references must exist in endpoints
        for i, peer in enumerate(peers):
            alias = peer.get("alias")
            if alias not in endpoints:
                errors.append(
                    f"policies.{policy_name}.peers[{i}].alias={alias!r} not "
                    f"found in endpoints. Define the endpoint or fix the "
                    f"reference."
                )
            ptype = peer.get("type")
            if ptype not in VALID_PEER_TYPES:
                errors.append(
                    f"policies.{policy_name}.peers[{i}].type={ptype!r} must be "
                    f"one of {sorted(VALID_PEER_TYPES)}"
                )

    # 4. Pipeline policy refs must exist
    for field in (
        "raw_writes_to_policy",
        "chunks_writes_to_policy",
        "metadata_writes_to_policy",
    ):
        target = pipeline.get(field)
        if target is not None and target not in policies:
            errors.append(
                f"pipeline.{field}={target!r} not found in policies. "
                f"Available: {sorted(policies.keys())}"
            )

    # 5. daemon.mode allow-list
    mode = daemon.get("mode")
    if mode is not None and mode not in VALID_DAEMON_MODES:
        errors.append(
            f"daemon.mode={mode!r} must be one of {sorted(VALID_DAEMON_MODES)}"
        )

    return errors

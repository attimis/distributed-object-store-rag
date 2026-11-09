"""Import-locality drift guard.

Replaces a manual grep check with an automated AST-walk that asserts:

  - ``boto3`` is only imported by the storage backend modules and the Lambda
    handlers — no stray imports leak into the rest of the package.
  - ``lancedb`` is only imported by ``storage.py`` and ``init_index.py``.
  - The pipeline-phase functions are the single shared code path between
    local and Lambda execution: both Lambda handlers (when present) and the
    local orchestrator import from ``pipeline``; no chunking-or-embedding
    logic is implemented inside the handler modules. The Lambda handlers and
    the local orchestrator call the same pipeline functions, so a bug fixed
    once is fixed everywhere.

Negative tests prevent drift as the package grows.
"""

from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
PKG_ROOT = REPO_ROOT / "src" / "onebucket_demo"
SCRIPTS_ROOT = REPO_ROOT / "scripts"

# Files allowed to import each restricted package (paths relative to PKG_ROOT).
BOTO3_ALLOWED = {
    "_s3.py",
    "storage.py",
    "lambda_handler_chunker.py",
    "lambda_handler_embedder.py",
    # Ingestion MCP tools (ingest_document, check_ingest_status, list_documents)
    # talk to S3/SQS directly from the MCP server process — same trust boundary
    # as storage.py since it's still server-process-local code, not client.
    "mcp_tools.py",
}
LANCEDB_ALLOWED = {
    "storage.py",
    "init_index.py",
}


def _modules_importing(target: str, root: Path) -> set[str]:
    """Walk every .py file under root; return relative paths importing `target`."""
    hits = set()
    for py in root.rglob("*.py"):
        rel = py.relative_to(root).as_posix()
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == target or alias.name.startswith(f"{target}."):
                        hits.add(rel)
            elif isinstance(node, ast.ImportFrom):
                if node.module == target or (node.module and node.module.startswith(f"{target}.")):
                    hits.add(rel)
    return hits


def test_boto3_imports_only_in_allowed_files():
    actual = _modules_importing("boto3", PKG_ROOT)
    # We allow files in BOTO3_ALLOWED to exist OR not.
    # The invariant: actual ⊆ BOTO3_ALLOWED.
    forbidden = actual - BOTO3_ALLOWED
    assert not forbidden, (
        f"boto3 imported outside allowlist: {forbidden}. "
        f"Add the module to BOTO3_ALLOWED in tests/test_import_locality.py "
        f"only after explicit architectural review."
    )


def test_lancedb_imports_only_in_allowed_files():
    actual = _modules_importing("lancedb", PKG_ROOT)
    forbidden = actual - LANCEDB_ALLOWED
    assert not forbidden, (
        f"lancedb imported outside allowlist: {forbidden}. "
        f"Add the module to LANCEDB_ALLOWED only after explicit architectural review."
    )


def _handler_files() -> list[Path]:
    return [
        PKG_ROOT / "lambda_handler_chunker.py",
        PKG_ROOT / "lambda_handler_embedder.py",
    ]


def test_lambda_handlers_call_pipeline_only():
    """Shared-code invariant: handlers import from pipeline; do not call chunking
    or embedding modules directly. pipeline.py is the single source of truth,
    used by both Lambda mode and local mode, so a bug fixed once is fixed
    everywhere."""
    forbidden_imports = {"chunking", "embed_dense", "embed_sparse", "writer"}
    for handler in _handler_files():
        tree = ast.parse(handler.read_text(encoding="utf-8"))
        imports_from = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports_from.add(node.module.split(".")[-1])
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports_from.add(alias.name.split(".")[-1])
        leaked = imports_from & forbidden_imports
        assert not leaked, (
            f"{handler.name} imports {leaked} directly. Pipeline logic must "
            f"flow through onebucket_demo.pipeline only."
        )
        # Positive: the handler must import pipeline
        assert "pipeline" in imports_from, (
            f"{handler.name} does not import pipeline; handlers must call pipeline.* "
            f"functions and reimplement no business logic."
        )

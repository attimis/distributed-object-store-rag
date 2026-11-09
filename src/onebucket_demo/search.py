"""Hybrid search: the one implementation behind the ``search_global_index`` tool.

The MCP server calls :func:`search_global_index` and serialises whatever it
returns, nothing more, so the in-process behaviour and the behaviour over
MCP cannot drift apart.

Response shape:
  Success:
    { "semantic_matches": [ { "chunk_uri", "source_uri", "raw_file_id", "chunk_index", "score" }, ... ],
      "keyword_matches":  [ ...same entry shape... ],
      "_timing_ms": { "embed_query", "dense_search", "sparse_search", "total_internal" } }
  Nothing found:
    { "message": "No relevant documents found for this query in the global index.", "_timing_ms": {...} }
  Index unreachable:
    { "error": "CONNECTION_FAILED", "message": "..." }

There is no ``chunk_text`` anywhere in the response: search returns
pointers, and the agent fetches the text of the chunks it wants with the
``fetch_chunk`` tool. That keeps responses small and lets the index store
only pointers (ADR-004).

Scores:
  - ``semantic_matches.score``: ``1 / (1 + L2 distance)`` between the query
    and chunk vectors, in (0, 1]. The vectors are unit length, so this ranks
    identically to cosine similarity.
  - ``keyword_matches.score``: the SPLADE sparse dot product, in [0, inf).
  The two scales differ on purpose; the consuming model fuses the two ranked
  lists (ADR-005) rather than comparing raw scores.

Sparse search is a full scan plus a Python-side dot product over rows cached
per table version: LanceDB has no native query over our struct-typed sparse
column, and at demo scale (thousands of chunks) the in-memory product takes
tens of milliseconds.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from .embed_dense import embed_dense
from .embed_sparse import compute_canary_hash, embed_sparse
from .errors import CanaryMismatchError
from .storage import TABLE_NAME, connect, read_sidecar

logger = logging.getLogger(__name__)

_MAX_LIMIT = 30  # large enough for a broad "discovery" search across many documents
_DEFAULT_LIMIT = 3


# Module-level cache for the LanceDB Connection + Table objects, keyed by
# dataset_root. Opening a table on object storage costs three or four
# round-trips; in a long-lived MCP server process the handle is safe to keep
# as long as it is refreshed to the latest committed version before every
# use (``checkout_latest`` is a single small read of the manifest pointer).
_table_cache: dict[str, tuple] = {}
_table_cache_lock = threading.Lock()


def _get_table(dataset_root):
    """Return a LanceDB Table for ``dataset_root``, reusing the cached handle.

    The handle is always refreshed to the latest committed version so rows
    committed by another process (the pipeline daemon, a Lambda, a CLI run)
    are visible, and so ``table.version`` is a trustworthy cache key.
    """
    key = str(dataset_root)
    with _table_cache_lock:
        cached = _table_cache.get(key)
    if cached is not None:
        _, table = cached
        _refresh(table)
        return table
    db = connect(dataset_root)
    table = db.open_table(TABLE_NAME)
    _refresh(table)
    with _table_cache_lock:
        _table_cache[key] = (db, table)
    return table


def _refresh(table) -> None:
    try:
        table.checkout_latest()
    except Exception:  # noqa: BLE001 - older LanceDB releases lack checkout_latest
        pass


def _table_version(table) -> int:
    try:
        return int(table.version)
    except Exception:  # noqa: BLE001 - version unavailable: never reuse cached rows
        return -1


def reset_caches(dataset_root: str | Path | None = None) -> None:
    """Drop cached table handles and sparse rows.

    Called by the destructive tools (``delete_document``, ``rebuild_index``)
    after they commit, and by the test suite between tests. With no argument
    everything is dropped; with a ``dataset_root`` only that dataset's entries.
    """
    with _table_cache_lock:
        if dataset_root is None:
            _table_cache.clear()
        else:
            _table_cache.pop(str(dataset_root), None)
    with _sparse_rows_lock:
        if dataset_root is None:
            _sparse_rows_cache.clear()
        else:
            for key in [k for k in _sparse_rows_cache if k[0] == str(dataset_root)]:
                _sparse_rows_cache.pop(key, None)


def assert_canary(dataset_root: str | Path) -> None:
    """Raise CanaryMismatchError if recorded canary != runtime canary."""
    metadata = read_sidecar(dataset_root)
    recorded = metadata.get("sparse_canary_hash", "<unset>")
    runtime = compute_canary_hash()
    if recorded != runtime:
        raise CanaryMismatchError(
            "writer/reader model-version drift detected: "
            f"recorded={recorded[:16]}…, runtime={runtime[:16]}…. "
            "Re-run scripts/init_index.py with the current package, or "
            "verify FastEmbed and model versions match the writer's."
        )


def search_global_index(
    query: str,
    limit: int = _DEFAULT_LIMIT,
    *,
    dataset_root: str | Path,
    scope: list[str] | None = None,
) -> dict:
    """Run hybrid (dense + sparse) search; optionally restrict to a list of
    raw_file_ids (the document content hashes returned by ``ingest_document``
    or surfaced from a discovery-hop search). When ``scope`` is non-empty,
    LanceDB's ``where`` clause filters at scan time so the agent gets back
    only chunks from the specified documents.
    """
    if not isinstance(limit, int) or limit < 1:
        limit = _DEFAULT_LIMIT
    if limit > _MAX_LIMIT:
        limit = _MAX_LIMIT

    try:
        table = _get_table(dataset_root)
    except Exception as exc:  # noqa: BLE001 - boundary: every failure becomes an envelope
        return {
            "error": "CONNECTION_FAILED",
            "message": (
                f"Unable to open the global index at {dataset_root}: {exc}. "
                "Ensure scripts/init_index.py has been run."
            ),
        }

    import time as _time
    t_start = _time.perf_counter()
    try:
        t0 = _time.perf_counter()
        dense_query = embed_dense([query])[0]
        sparse_query = embed_sparse([query])[0]
        embed_ms = int((_time.perf_counter() - t0) * 1000)

        t0 = _time.perf_counter()
        semantic_matches = _dense_search(table, dense_query, limit, scope)
        dense_ms = int((_time.perf_counter() - t0) * 1000)

        t0 = _time.perf_counter()
        keyword_matches = _sparse_search(table, sparse_query, limit, scope, str(dataset_root))
        sparse_ms = int((_time.perf_counter() - t0) * 1000)
    except Exception as exc:  # noqa: BLE001 - same boundary policy
        logger.warning("search failed: %s", exc)
        return {
            "error": "CONNECTION_FAILED",
            "message": f"Search execution failed: {exc}.",
        }

    total_ms = int((_time.perf_counter() - t_start) * 1000)

    if not semantic_matches and not keyword_matches:
        return {
            "message": "No relevant documents found for this query in the global index.",
            "_timing_ms": {
                "embed_query": embed_ms,
                "dense_search": dense_ms,
                "sparse_search": sparse_ms,
                "total_internal": total_ms,
            },
        }

    # _timing_ms is the time spent inside this function: embedding the query
    # plus the two index scans. It excludes the MCP round-trip and the
    # model's own thinking time, which is why a demo should quote these
    # numbers rather than wall-clock around the tool call.
    return {
        "semantic_matches": semantic_matches,
        "keyword_matches": keyword_matches,
        "_timing_ms": {
            "embed_query": embed_ms,
            "dense_search": dense_ms,
            "sparse_search": sparse_ms,
            "total_internal": total_ms,
        },
    }


def _scope_clause(scope: list[str] | None) -> str | None:
    """Build a SQL WHERE clause restricting to a list of raw_file_ids.

    Each id is 16 hex chars (a SHA-256 prefix from the chunker). Anything
    that is not plain alphanumeric is dropped rather than quoted, so no
    caller input ever reaches the LanceDB filter parser unvalidated.
    """
    if not scope:
        return None
    safe = [s for s in scope if isinstance(s, str) and s.isalnum() and 1 <= len(s) <= 64]
    if not safe:
        return None
    quoted = ", ".join(f"'{s}'" for s in safe)
    return f"raw_file_id IN ({quoted})"


def _row_to_hit(r: dict, score: float) -> dict:
    """Shared shape of a search result row.

    raw_file_id and chunk_index are exposed so the caller (and Claude during
    multi-hop discovery) can build a ``scope`` parameter for a follow-up
    focused search without parsing source_uri strings.
    """
    return {
        "chunk_uri": r["chunk_uri"],
        "source_uri": r["source_uri"],
        "raw_file_id": r.get("raw_file_id", ""),
        "chunk_index": r.get("chunk_index", -1),
        "score": float(score),
    }


def _dense_search(
    table, dense_query: list[float], limit: int, scope: list[str] | None
) -> list[dict]:
    q = table.search(dense_query, vector_column_name="vector")
    where = _scope_clause(scope)
    if where:
        q = q.where(where)
    rows = q.limit(limit).to_list()
    return [_row_to_hit(r, 1.0 / (1.0 + float(r.get("_distance", 0.0)))) for r in rows]


def _sparse_search(
    table, sparse_query: dict, limit: int, scope: list[str] | None, dataset_root: str = ""
) -> list[dict]:
    """Sparse (SPLADE) search as a full scan plus a Python-side dot product.

    LanceDB has no native query over our struct-typed sparse column, so the
    rows are loaded once and scored in memory. Loading them from object
    storage costs about a second (network plus Arrow decoding), which is why
    they are cached per dataset and table version (see
    ``_get_cached_sparse_rows``). The hot path is an in-memory dot product,
    tens of milliseconds over a few thousand rows.

    ``scope`` is applied in Python as a set-membership filter over the cached
    rows; the dense search applies the same scope as a LanceDB ``where``.
    """
    cached_rows = _get_cached_sparse_rows(table, dataset_root)
    if scope:
        scope_set = set(scope)
        candidate_rows = [r for r in cached_rows if r.get("raw_file_id") in scope_set]
    else:
        candidate_rows = cached_rows
    if not candidate_rows:
        return []
    q_map = dict(zip(sparse_query["indices"], sparse_query["values"]))
    scored: list[tuple[float, dict]] = []
    for r in candidate_rows:
        s = r.get("sparse_vector")
        if not s:
            continue
        score = 0.0
        for idx, val in zip(s["indices"], s["values"]):
            if idx in q_map:
                score += val * q_map[idx]
        if score > 0.0:
            scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [_row_to_hit(r, score) for score, r in scored[:limit]]


# Module-level cache for sparse-vector rows, keyed by (dataset_root, table
# version). LanceDB's table version is monotonic and changes on every commit,
# including commits made by another process, so a cache entry can never be
# served after the table changed underneath it. Row count is deliberately
# not part of the key: deleting one document and ingesting another with the
# same number of chunks leaves the count unchanged.
_sparse_rows_cache: dict[tuple[str, int], list[dict]] = {}
_sparse_rows_lock = threading.Lock()


def _get_cached_sparse_rows(table, dataset_root: str = "") -> list[dict]:
    """Return every row's id columns and sparse vector, cached per table version.

    The caller passes a table handle that ``_get_table`` has already refreshed
    to the latest committed version, so ``table.version`` reflects storage.
    """
    version = _table_version(table)
    key = (str(dataset_root), version)
    if version >= 0:
        with _sparse_rows_lock:
            cached = _sparse_rows_cache.get(key)
            if cached is not None:
                return cached
    try:
        arrow = (
            table.search()
            .select(["raw_file_id", "chunk_uri", "source_uri", "chunk_index", "sparse_vector"])
            .limit(10**9)
            .to_arrow()
        )
        rows = arrow.to_pylist()
    except Exception:  # noqa: BLE001 - older LanceDB: fall back to a full dump
        rows = table.to_arrow().to_pylist()
    if version >= 0:
        with _sparse_rows_lock:
            # Keep only the current generation for this dataset.
            for stale in [k for k in _sparse_rows_cache if k[0] == key[0]]:
                _sparse_rows_cache.pop(stale, None)
            _sparse_rows_cache[key] = rows
    return rows

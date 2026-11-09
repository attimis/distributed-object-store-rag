"""The pipeline's two phases, shared by every execution mode.

  * :func:`chunk_and_write`  chunk a source file, write the chunk files, then
    write ``MANIFEST.json`` last. Called by the chunker Lambda, by both
    daemons and by the local ingest.
  * :func:`embed_and_commit`  read a manifest, embed its chunks (dense and
    sparse) in bounded batches, ``merge_insert`` the rows into LanceDB.
    Called by the embedder Lambda, by both daemons and by the local ingest.
  * :func:`ingest_local`  the two phases back to back, for the filesystem
    demo and the tests.

This is the one place chunking, embedding and index writes happen. The
Lambda handlers and the daemons are thin wrappers that only decide *when*
to call these functions and where the bytes live; ``tests/test_import_locality.py``
fails if any of them grows logic of its own. A bug fixed here is fixed in
every mode.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from . import manifest as manifest_module
from . import storage
from .chunking import chunk_text
from .errors import StorageWriteError  # noqa: F401  -- re-export for callers

# Default embed batch size for embed_and_commit. Bounded to keep total memory
# under the embedder Lambda's 3008 MB ceiling (the AWS-account default quota).
# A 138-chunk 10-K embedded in one batch hit Runtime.OutOfMemory at 3008 MB;
# 16 chunks per batch holds resident memory at ~1.8 GB for the same workload.
# Override via ONEBUCKET_EMBED_BATCH_SIZE env var if you have headroom or are
# running locally with more memory.
DEFAULT_EMBED_BATCH_SIZE = 16

# pyarrow / embed_dense / embed_sparse / schema / search / gates / connect are
# imported lazily inside the functions that need them. The chunker Lambda
# only ever calls chunk_and_write, so its image (about 200 MB) never needs
# fastembed, LanceDB or the model weights that make the embedder image ten
# times larger. Both functions still live in this one module; only the
# import edges differ.

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriterResult:
    path: str
    accepted: bool
    reason: str | None
    raw_file_id: str | None
    chunks_written: int


@dataclass(frozen=True)
class EmbedResult:
    raw_file_id: str
    n_committed: int


class EmptySource(ValueError):
    """Source produced no chunks; chunker emits no manifest, embedder is not
    triggered. The chained design's "no manifest = no downstream work" property
    is preserved."""


# ---------------------------------------------------------------------------
# Phase 1: chunk_and_write
# ---------------------------------------------------------------------------


def chunk_and_write(
    local_source_path: str | Path,
    *,
    source_uri: str,
    raw_object_key: str,
    chunk_root: str | Path,
) -> manifest_module.Manifest:
    """Chunk a source file and write chunks + MANIFEST.json LAST.

    Parameters
    ----------
    local_source_path
        Where to *read* the source bytes from. In Lambda mode this is the
        ``/tmp`` path produced by boto3 download. In local mode this is the
        actual file path.
    source_uri
        Canonical identifier for the source — embedded in the manifest and
        carried through to ``source_uri`` on every Lance row. ``file://...``
        in local mode; ``s3://bucket/raw/...`` in Lambda mode.
    raw_object_key
        S3 object key (Lambda mode) or local path (local mode) for tracking.
    chunk_root
        Root URI under which chunks are written. Local path or ``s3://``.

    Returns the Manifest just written (already persisted to ``chunk_root``).
    Raises :class:`EmptySource` if the source produced zero chunks (manifest
    is NOT written; embedder is NOT triggered).
    Raises :class:`StorageWriteError` on persistent storage failure (manifest
    is NOT written if any chunk write failed first).
    """
    p = Path(local_source_path).resolve()
    text = p.read_text(encoding="utf-8")
    chunks = chunk_text(text)
    if not chunks:
        raise EmptySource(f"source has no chunks: {source_uri}")

    raw_file_id = chunks[0].raw_file_id

    # Write all chunk files first; only on full success do we write the manifest
    for chunk in chunks:
        storage.write_chunk(
            chunk_root, raw_file_id, chunk.chunk_index, chunk.chunk_text
        )

    # MANIFEST.json LAST — its appearance is the embedder's barrier signal
    manifest = manifest_module.Manifest(
        raw_file_id=raw_file_id,
        source_uri=source_uri,
        n_chunks=len(chunks),
        raw_object_key=raw_object_key,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    manifest_target = storage.manifest_uri(chunk_root, raw_file_id)
    manifest_module.write(manifest_target, manifest)
    return manifest


# ---------------------------------------------------------------------------
# Phase 2: embed_and_commit
# ---------------------------------------------------------------------------


def embed_and_commit(
    manifest_uri: str,
    *,
    dataset_root: str | Path,
    chunk_root: str | Path,
) -> EmbedResult:
    """Read a manifest, batch-embed all chunks, merge_insert into Lance.

    Parameters
    ----------
    manifest_uri
        Full URI to the MANIFEST.json file. ``file://...`` or ``s3://...``.
    dataset_root
        Root URI (or local path) of the LanceDB dataset.
    chunk_root
        Root URI (or local path) under which chunk files live.

    Raises :class:`onebucket_demo.errors.CanaryMismatchError` on writer/reader
    model drift, :class:`onebucket_demo.manifest.UnsupportedManifestVersion`
    on schema drift, and ``ValueError`` when the manifest's ``raw_file_id``
    does not match the directory it was found in (a cheap defence against a
    manifest copied to the wrong place).
    """
    # Embedder-only imports stay inside the function so the chunker Lambda
    # image can omit fastembed, LanceDB and pyarrow.
    import pyarrow as pa  # noqa: PLC0415

    from .embed_dense import embed_dense  # noqa: PLC0415
    from .embed_sparse import embed_sparse  # noqa: PLC0415
    from .schema import get_schema  # noqa: PLC0415
    from .search import assert_canary  # noqa: PLC0415
    from .storage import TABLE_NAME, connect  # noqa: PLC0415

    manifest = manifest_module.read(manifest_uri)

    # The manifest must live under the document id it names.
    expected_suffix = f"/{manifest.raw_file_id}/{manifest_module.MANIFEST_FILENAME}"
    if not manifest_uri.endswith(expected_suffix):
        raise ValueError(
            f"manifest path mismatch: {manifest_uri!r} does not contain "
            f"raw_file_id={manifest.raw_file_id!r}"
        )

    # Writer/reader canary contract
    assert_canary(dataset_root)

    # Read chunk texts
    chunk_uris: list[str] = []
    chunk_texts: list[str] = []
    for i in range(manifest.n_chunks):
        uri = storage.chunk_uri(chunk_root, manifest.raw_file_id, i)
        chunk_uris.append(uri)
        chunk_texts.append(storage.read_chunk(uri))

    # Batched embedding — split into bounded chunks to keep peak memory under
    # the Lambda 3008 MB cap. Without this, FastEmbed's batch embed of all
    # ~138 chunks of a 10-K in one pass hit Runtime.OutOfMemory at 3008 MB.
    # Per-batch cost: one ONNX inference per call × per_batch_size; the cached
    # model is reused across batches (no per-batch reload), so only the
    # activation memory is bounded.
    batch_size = int(
        os.environ.get("ONEBUCKET_EMBED_BATCH_SIZE", DEFAULT_EMBED_BATCH_SIZE)
    )
    dense: list = []
    sparse: list = []
    for batch_start in range(0, manifest.n_chunks, batch_size):
        batch = chunk_texts[batch_start : batch_start + batch_size]
        dense.extend(embed_dense(batch))
        sparse.extend(embed_sparse(batch))

    now = datetime.now(timezone.utc)
    rows = [
        {
            "id": f"{manifest.raw_file_id}-{i:04d}",
            "raw_file_id": manifest.raw_file_id,
            "chunk_index": i,
            "chunk_uri": chunk_uris[i],
            "source_uri": manifest.source_uri,
            "vector": dense[i],
            "sparse_vector": sparse[i],
            "created_at": now,
        }
        for i in range(manifest.n_chunks)
    ]

    db = connect(dataset_root)
    table = db.open_table(TABLE_NAME)
    arrow_data = pa.Table.from_pylist(rows, schema=get_schema())
    (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(arrow_data)
    )

    return EmbedResult(raw_file_id=manifest.raw_file_id, n_committed=manifest.n_chunks)


# ---------------------------------------------------------------------------
# Phase 1+2 orchestrator: ingest_local
# ---------------------------------------------------------------------------


def ingest_local(
    source_path: str | Path,
    *,
    dataset_root: str | Path,
    chunk_root: str | Path,
) -> WriterResult:
    """Local-mode orchestrator: chunk_and_write, then embed_and_commit.

    Gate violations (wrong type, too large, missing file) are reported in
    ``WriterResult.reason`` rather than raised, so a CLI can print them and
    exit non-zero without a traceback.
    """
    p = Path(source_path).resolve()
    accepted, reason = _local_validate(p)
    if not accepted:
        logger.warning("ingest skipped: %s", reason)
        return WriterResult(
            path=str(p),
            accepted=False,
            reason=reason,
            raw_file_id=None,
            chunks_written=0,
        )

    try:
        manifest = chunk_and_write(
            p,
            source_uri=p.as_uri(),
            raw_object_key=str(p),
            chunk_root=chunk_root,
        )
    except EmptySource:
        return WriterResult(
            path=str(p),
            accepted=True,
            reason="empty file",
            raw_file_id=None,
            chunks_written=0,
        )

    manifest_target = storage.manifest_uri(chunk_root, manifest.raw_file_id)
    embed_and_commit(
        manifest_target, dataset_root=dataset_root, chunk_root=chunk_root
    )

    return WriterResult(
        path=str(p),
        accepted=True,
        reason=None,
        raw_file_id=manifest.raw_file_id,
        chunks_written=manifest.n_chunks,
    )


def _local_validate(path: Path) -> tuple[bool, str | None]:
    from .gates import check_size, check_type  # noqa: PLC0415

    if not path.exists():
        return False, f"File '{path}' does not exist."
    ok, reason = check_type(path)
    if not ok:
        return False, reason
    ok, reason = check_size(path)
    if not ok:
        return False, reason
    return True, None

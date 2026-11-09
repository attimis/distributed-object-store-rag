"""Storage chokepoint: every read and write of chunks, manifests and the index
sidecar goes through here, and the backend is chosen by the URI scheme.

  - ``s3://...``                 the S3 backend (boto3)
  - ``file://...`` or bare path  the local filesystem

This is what makes "local", "AWS S3" and "OneBucket" one code path. boto3
reads ``AWS_ENDPOINT_URL`` at client construction, so pointing it at a
OneBucket S3 endpoint (for example ``https://s3.onebucket.io``) sends the
identical calls to OneBucket instead of AWS; no branch in the code changes.

``boto3`` and ``lancedb`` are imported only here, in ``_s3.py`` and in the
two Lambda handlers (``tests/test_import_locality.py`` enforces it), which
keeps the chunker Lambda image free of the embedding and database stack.

Both backends raise the same exceptions from ``errors.py``:
``ChunkNotFoundError`` for a missing key, ``StorageWriteError`` for a failed
write.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from . import _s3
from .errors import ChunkNotFoundError, StorageWriteError

if TYPE_CHECKING:  # pragma: no cover — typing only
    import lancedb

SIDECAR_FILENAME = "_init_metadata.json"
TABLE_NAME = "global_index"


class InitMetadata(TypedDict):
    fastembed_version: str
    dense_model: str
    sparse_model: str
    sparse_canary_hash: str
    schema_version: str
    initialized_at: str


def _is_s3(uri: str | Path) -> bool:
    return str(uri).startswith("s3://")


def _strip_file_scheme(uri: str) -> str:
    if uri.startswith("file://"):
        return uri[len("file://") :]
    return uri


def connect(dataset_root: str | Path) -> "lancedb.DBConnection":
    """Open a LanceDB connection. LanceDB natively handles ``s3://`` URIs.

    The ``lancedb`` import is deferred so the chunker Lambda image, which
    never calls ``connect()``, does not need LanceDB installed at all.
    """
    import lancedb  # noqa: PLC0415  -- intentional deferred import

    return lancedb.connect(str(dataset_root))


def sidecar_uri(dataset_root: str | Path) -> str:
    """Return the canonical sidecar URI for a dataset_root.

    Local mode: ``{dataset_root}/_init_metadata.json`` as a path-string.
    S3 mode:    ``{dataset_root}/_init_metadata.json`` as an s3:// URI.
    """
    root = str(dataset_root)
    if _is_s3(root):
        return _s3.join_uri(root, SIDECAR_FILENAME)
    return str(Path(root) / SIDECAR_FILENAME)


def sidecar_path(dataset_root: str | Path) -> Path:
    """Local-only sidecar path helper.

    Returns a ``pathlib.Path`` for the sidecar of a local dataset_root.
    Raises ``ValueError`` if dataset_root is an s3:// URI; backend-agnostic
    callers use :func:`sidecar_uri` and the read/write/exists helpers.
    """
    uri = sidecar_uri(dataset_root)
    if uri.startswith("s3://"):
        raise ValueError(f"sidecar_path is local-only; got s3 URI: {uri}")
    return Path(uri)


def sidecar_exists(dataset_root: str | Path) -> bool:
    """Multi-mode existence check for the sidecar."""
    uri = sidecar_uri(dataset_root)
    if _is_s3(uri):
        return _s3.head_object(uri)
    return Path(uri).exists()


def write_sidecar(dataset_root: str | Path, metadata: InitMetadata) -> None:
    payload = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode("utf-8")
    target = sidecar_uri(dataset_root)
    if _is_s3(target):
        _s3.put_object(target, payload)
        return
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def read_sidecar(dataset_root: str | Path) -> InitMetadata:
    target = sidecar_uri(dataset_root)
    if _is_s3(target):
        return json.loads(_s3.get_object(target).decode("utf-8"))
    return json.loads(Path(target).read_text())


def chunk_uri(chunk_root: str | Path, raw_file_id: str, chunk_index: int) -> str:
    """Compute the URI a chunk file lives at, in canonical form.

    For local roots: ``file:///abs/path/chunks/{raw_file_id}/chunk_NNNN.txt``
    For s3 roots:    ``s3://bucket/chunks/{raw_file_id}/chunk_NNNN.txt``
    """
    chunk_basename = f"chunk_{chunk_index:04d}.txt"
    root = str(chunk_root)
    if _is_s3(root):
        return _s3.join_uri(root, raw_file_id, chunk_basename)
    abs_path = (Path(root).resolve() / raw_file_id / chunk_basename)
    return abs_path.as_uri()


def write_chunk(
    chunk_root: str | Path,
    raw_file_id: str,
    chunk_index: int,
    content: str,
) -> str:
    """Write a chunk file under chunk_root, return its full URI."""
    uri = chunk_uri(chunk_root, raw_file_id, chunk_index)
    payload = content.encode("utf-8")
    if _is_s3(uri):
        _s3.put_object(uri, payload)
        return uri
    local_path = Path(_strip_file_scheme(uri))
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(payload)
    except OSError as exc:
        raise StorageWriteError(f"local chunk write failed for {uri}: {exc}") from exc
    return uri


def read_chunk(uri: str) -> str:
    """Read a chunk by URI; raises ChunkNotFoundError if missing."""
    if _is_s3(uri):
        return _s3.get_object(uri).decode("utf-8")
    local_path = Path(_strip_file_scheme(uri))
    try:
        return local_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ChunkNotFoundError(f"local chunk not found: {uri}") from exc


def chunk_exists(uri: str) -> bool:
    if _is_s3(uri):
        return _s3.head_object(uri)
    return Path(_strip_file_scheme(uri)).exists()


def manifest_uri(chunk_root: str | Path, raw_file_id: str) -> str:
    """URI of the MANIFEST.json for a given raw_file_id under chunk_root."""
    root = str(chunk_root)
    if _is_s3(root):
        return _s3.join_uri(root, raw_file_id, "MANIFEST.json")
    return (Path(root).resolve() / raw_file_id / "MANIFEST.json").as_uri()


def write_text(uri: str, content: str) -> None:
    """Write arbitrary UTF-8 text to a URI (used for MANIFEST.json)."""
    payload = content.encode("utf-8")
    if _is_s3(uri):
        _s3.put_object(uri, payload)
        return
    local_path = Path(_strip_file_scheme(uri))
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(payload)
    except OSError as exc:
        raise StorageWriteError(f"local write failed for {uri}: {exc}") from exc


def read_text(uri: str) -> str:
    """Read arbitrary UTF-8 text from a URI (used for MANIFEST.json)."""
    if _is_s3(uri):
        return _s3.get_object(uri).decode("utf-8")
    local_path = Path(_strip_file_scheme(uri))
    try:
        return local_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ChunkNotFoundError(f"local file not found: {uri}") from exc

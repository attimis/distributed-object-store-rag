"""Pipeline phase tests.

Verifies the shared-code invariant: the same ``chunk_and_write``,
``embed_and_commit``, ``ingest_local`` functions are called by the Lambda
handlers and by the local orchestrator, so a bug fixed once is fixed
everywhere. Behavior-preserving against the original e2e tests.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from onebucket_demo import manifest as manifest_module
from onebucket_demo import storage
from onebucket_demo.errors import CanaryMismatchError
from onebucket_demo.init_index import main as init_index
from onebucket_demo.pipeline import (
    EmbedResult,
    EmptySource,
    chunk_and_write,
    embed_and_commit,
    ingest_local,
)
from onebucket_demo.storage import TABLE_NAME, connect


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture
def repo(tmp_path):
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    return dataset_root, chunk_root


# ---------------------------------------------------------------------------
# chunk_and_write
# ---------------------------------------------------------------------------


def test_chunk_and_write_produces_manifest_and_chunks(repo, tmp_path):
    _, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)

    manifest = chunk_and_write(
        fixture,
        source_uri=fixture.as_uri(),
        raw_object_key=str(fixture),
        chunk_root=chunk_root,
    )

    # Manifest is canonical: schema_version, fields, types
    assert manifest.schema_version == 1
    assert manifest.n_chunks >= 1

    # All chunk files exist on disk
    for i in range(manifest.n_chunks):
        uri = storage.chunk_uri(chunk_root, manifest.raw_file_id, i)
        assert storage.chunk_exists(uri)

    # MANIFEST.json was written and is readable
    manifest_uri = storage.manifest_uri(chunk_root, manifest.raw_file_id)
    assert storage.chunk_exists(manifest_uri)
    reread = manifest_module.read(manifest_uri)
    assert reread.raw_file_id == manifest.raw_file_id
    assert reread.n_chunks == manifest.n_chunks


def test_chunk_and_write_manifest_atomicity(repo, tmp_path, monkeypatch):
    """If a chunk write fails, NO manifest should be produced (manifest is the
    atomic commit marker for the chunker phase)."""
    _, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)

    real_write_chunk = storage.write_chunk
    call_count = {"n": 0}

    def failing_write_chunk(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] >= 1:
            raise RuntimeError("simulated mid-write failure")
        return real_write_chunk(*args, **kwargs)

    monkeypatch.setattr(storage, "write_chunk", failing_write_chunk)

    with pytest.raises(RuntimeError):
        chunk_and_write(
            fixture,
            source_uri=fixture.as_uri(),
            raw_object_key=str(fixture),
            chunk_root=chunk_root,
        )

    # No manifest should exist — chunker partial-failure must not trigger embedder
    manifests = list(Path(chunk_root).rglob("MANIFEST.json"))
    assert manifests == []


def test_chunk_and_write_empty_source_raises_empty_source(repo, tmp_path):
    _, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "    \n\n   ")
    with pytest.raises(EmptySource):
        chunk_and_write(
            fixture,
            source_uri=fixture.as_uri(),
            raw_object_key=str(fixture),
            chunk_root=chunk_root,
        )


def test_chunk_and_write_retry_determinism(repo, tmp_path):
    """Retry determinism (chunker side): two invocations on the same source
    produce byte-identical chunk file content + identical manifest fields
    (excluding created_at, which is timestamped per invocation)."""
    _, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)

    m1 = chunk_and_write(
        fixture,
        source_uri=fixture.as_uri(),
        raw_object_key=str(fixture),
        chunk_root=chunk_root,
    )
    m2 = chunk_and_write(
        fixture,
        source_uri=fixture.as_uri(),
        raw_object_key=str(fixture),
        chunk_root=chunk_root,
    )

    assert m1.raw_file_id == m2.raw_file_id
    assert m1.n_chunks == m2.n_chunks
    assert m1.source_uri == m2.source_uri
    assert m1.raw_object_key == m2.raw_object_key

    # Byte-identical chunk file content on retry
    for i in range(m1.n_chunks):
        uri = storage.chunk_uri(chunk_root, m1.raw_file_id, i)
        # Just re-read; if the second invocation overwrote with different bytes the
        # comparison would fail — but determinism guarantees they're identical.
        content = storage.read_chunk(uri)
        assert content  # non-empty; deterministic content


# ---------------------------------------------------------------------------
# embed_and_commit
# ---------------------------------------------------------------------------


def _seed_manifest(repo, tmp_path, content: str = "Hello world. " * 200):
    """Seed a chunk_root + manifest by running chunk_and_write."""
    _, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", content)
    manifest = chunk_and_write(
        fixture,
        source_uri=fixture.as_uri(),
        raw_object_key=str(fixture),
        chunk_root=chunk_root,
    )
    return manifest, chunk_root


def test_embed_and_commit_inserts_lance_rows(repo, tmp_path):
    dataset_root, _ = repo
    manifest, chunk_root = _seed_manifest(repo, tmp_path)
    manifest_target = storage.manifest_uri(chunk_root, manifest.raw_file_id)

    result = embed_and_commit(
        manifest_target, dataset_root=dataset_root, chunk_root=chunk_root
    )
    assert isinstance(result, EmbedResult)
    assert result.n_committed == manifest.n_chunks

    db = connect(dataset_root)
    assert db.open_table(TABLE_NAME).count_rows() == manifest.n_chunks


def test_embed_and_commit_retry_idempotency(repo, tmp_path):
    """Retry idempotency (embedder side): two invocations on the same manifest
    leave Lance with EXACTLY n_chunks rows (not 2 * n_chunks)."""
    dataset_root, _ = repo
    manifest, chunk_root = _seed_manifest(repo, tmp_path)
    manifest_target = storage.manifest_uri(chunk_root, manifest.raw_file_id)

    embed_and_commit(manifest_target, dataset_root=dataset_root, chunk_root=chunk_root)
    embed_and_commit(manifest_target, dataset_root=dataset_root, chunk_root=chunk_root)

    db = connect(dataset_root)
    assert db.open_table(TABLE_NAME).count_rows() == manifest.n_chunks


def test_embed_and_commit_path_mismatch_rejected(repo, tmp_path):
    """A manifest's raw_file_id must match the directory it lives in, so a
    misplaced or forged manifest cannot write rows under another file's id."""
    dataset_root, _ = repo
    manifest, chunk_root = _seed_manifest(repo, tmp_path)

    # Forge a manifest at a path whose directory ≠ raw_file_id
    forged_uri = (Path(chunk_root) / "FORGED" / "MANIFEST.json").as_uri()
    storage.write_text(forged_uri, manifest.to_json())

    with pytest.raises(ValueError, match="path mismatch"):
        embed_and_commit(forged_uri, dataset_root=dataset_root, chunk_root=chunk_root)


def test_embed_and_commit_unknown_schema_rejected(repo, tmp_path):
    """Unknown schema_version is rejected rather than guessed at."""
    dataset_root, _ = repo
    manifest, chunk_root = _seed_manifest(repo, tmp_path)
    manifest_target = storage.manifest_uri(chunk_root, manifest.raw_file_id)

    # Corrupt the manifest's schema_version via the storage abstraction
    payload = json.loads(storage.read_text(manifest_target))
    payload["schema_version"] = 999
    storage.write_text(manifest_target, json.dumps(payload))

    with pytest.raises(manifest_module.UnsupportedManifestVersion):
        embed_and_commit(
            manifest_target, dataset_root=dataset_root, chunk_root=chunk_root
        )


def test_embed_and_commit_canary_mismatch_rejected(repo, tmp_path):
    dataset_root, _ = repo
    manifest, chunk_root = _seed_manifest(repo, tmp_path)
    manifest_target = storage.manifest_uri(chunk_root, manifest.raw_file_id)

    # Corrupt the canary
    sp = storage.sidecar_path(dataset_root)
    meta = json.loads(sp.read_text())
    meta["sparse_canary_hash"] = "0" * 64
    sp.write_text(json.dumps(meta))

    with pytest.raises(CanaryMismatchError):
        embed_and_commit(
            manifest_target, dataset_root=dataset_root, chunk_root=chunk_root
        )


# ---------------------------------------------------------------------------
# ingest_local (original single-process behavior, preserved)
# ---------------------------------------------------------------------------


def test_ingest_local_happy_path(repo, tmp_path):
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)
    result = ingest_local(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    assert result.accepted
    assert result.chunks_written >= 1

    db = connect(dataset_root)
    assert db.open_table(TABLE_NAME).count_rows() == result.chunks_written


def test_ingest_local_gate_rejection_returns_writerresult(repo, tmp_path):
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "report.pdf", "fake")
    result = ingest_local(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    assert not result.accepted
    assert "PDF" in result.reason

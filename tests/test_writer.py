"""Tests for the writer pipeline, including re-ingest idempotency."""

from pathlib import Path

import pytest

from onebucket_demo.chunking import chunk_text
from onebucket_demo.init_index import main as init_index
from onebucket_demo.storage import TABLE_NAME, connect
from onebucket_demo.writer import ingest


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


def test_ingest_writes_chunks_and_lance_rows(repo, tmp_path):
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)
    result = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    assert result.accepted
    assert result.chunks_written >= 1

    chunk_files = sorted((chunk_root / result.raw_file_id).glob("chunk_*.txt"))
    assert len(chunk_files) == result.chunks_written

    db = connect(dataset_root)
    table = db.open_table(TABLE_NAME)
    assert table.count_rows() == result.chunks_written


def test_ingest_uri_format_is_full_file_uri(repo, tmp_path):
    """URIs are full file:// URIs on the local backend (the S3 backend swaps
    the scheme to s3://...). Same shape on every backend: every URI is a complete
    addressable path the agent or the OneBucket MCP server can fetch.
    """
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)
    result = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)

    db = connect(dataset_root)
    rows = db.open_table(TABLE_NAME).search().to_list()
    chunk_root_abs = Path(chunk_root).resolve()
    fixture_abs = fixture.resolve()
    for r in rows:
        assert r["chunk_uri"].startswith("file://")
        assert r["source_uri"].startswith("file://")
        # chunk_uri resolves to an existing file under chunk_root
        assert r["chunk_uri"].endswith(".txt")
        chunk_local = Path(r["chunk_uri"][len("file://"):])
        assert chunk_local.exists()
        assert chunk_local.parent == chunk_root_abs / result.raw_file_id
        # source_uri points at the actual original file
        assert r["source_uri"] == fixture_abs.as_uri()


def test_double_ingest_unchanged_row_count(repo, tmp_path):
    """Re-ingest of identical content does not duplicate rows."""
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "doc.md", "Hello world. " * 200)
    a = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    b = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)

    assert a.raw_file_id == b.raw_file_id
    assert a.chunks_written == b.chunks_written

    db = connect(dataset_root)
    table = db.open_table(TABLE_NAME)
    assert table.count_rows() == a.chunks_written


def test_modified_content_keeps_old_rows(repo, tmp_path):
    """Garbage collection is deliberately deferred (see ADR-009, Risks).
    Modified content produces a NEW raw_file_id; old rows REMAIN in the index
    because no garbage collector runs yet. **Revisit this assertion when GC ships.**
    """
    dataset_root, chunk_root = repo
    fixture = tmp_path / "doc.md"
    _write(fixture, "Original content here. " * 200)
    a = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)

    _write(fixture, "Wholly different content now. " * 200)
    b = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)

    assert a.raw_file_id != b.raw_file_id

    db = connect(dataset_root)
    total = db.open_table(TABLE_NAME).count_rows()
    assert total == a.chunks_written + b.chunks_written


def test_ingest_oversize_skipped_no_writes(repo, tmp_path):
    dataset_root, chunk_root = repo
    fixture = tmp_path / "big.txt"
    fixture.write_bytes(b"x" * (10 * 1024 * 1024 + 1))
    result = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    assert not result.accepted
    assert "10 MB" in result.reason

    db = connect(dataset_root)
    assert db.open_table(TABLE_NAME).count_rows() == 0


def test_ingest_pdf_rejected_with_message(repo, tmp_path):
    dataset_root, chunk_root = repo
    fixture = _write(tmp_path / "report.pdf", "fake pdf content")
    result = ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    assert not result.accepted
    assert "PDF" in result.reason

    db = connect(dataset_root)
    assert db.open_table(TABLE_NAME).count_rows() == 0


def test_chunk_token_count_under_splade_window(tmp_path):
    """Every produced chunk fits SPLADE's 512-token context window.

    Empirical guard for the BGE-small-budgets-everything assumption documented
    in ADR-009. SPLADE uses a BERT-base tokenizer; load it directly.
    """
    from tokenizers import Tokenizer

    paragraph = ("This is a sentence with some words in it. " * 60).strip()
    text = "\n\n".join([paragraph] * 5)
    chunks = chunk_text(text)

    splade_tok = Tokenizer.from_pretrained("Qdrant/SPLADE_PP_en_v1")
    for c in chunks:
        n = len(splade_tok.encode(c.chunk_text, add_special_tokens=False).ids)
        assert n <= 512, (
            f"chunk {c.chunk_index} produces {n} SPLADE tokens; exceeds 512-token window"
        )

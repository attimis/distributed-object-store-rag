"""Chunker Lambda handler tests."""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from onebucket_demo import storage
from onebucket_demo.lambda_handler_chunker import handler

REPO_ROOT = Path(__file__).resolve().parent.parent

RAW_BUCKET = "ob-test-raw"


def _event(bucket: str, key: str, size: int) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key, "size": size},
                }
            }
        ]
    }


@pytest.fixture
def aws_env(tmp_path, monkeypatch):
    """Moto-mocked S3 with a raw bucket pre-created. Chunk root under tmp_path."""
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", str(tmp_path / "chunks"))
    storage._s3.client.cache_clear()
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=RAW_BUCKET)
        yield s3, tmp_path
        storage._s3.client.cache_clear()


def _put_raw(s3_client, key: str, body: bytes) -> int:
    s3_client.put_object(Bucket=RAW_BUCKET, Key=key, Body=body)
    return len(body)


def test_handler_happy_path_writes_chunks_and_manifest(aws_env):
    s3, tmp_path = aws_env
    body = ("Hello world. " * 200).encode("utf-8")
    size = _put_raw(s3, "raw/doc.md", body)

    result = handler(_event(RAW_BUCKET, "raw/doc.md", size), context=None)
    assert result["statusCode"] == 200
    assert "raw_file_id" in result
    assert result["n_chunks"] >= 1

    # Manifest landed
    raw_file_id = result["raw_file_id"]
    manifest_uri = storage.manifest_uri(str(tmp_path / "chunks"), raw_file_id)
    assert storage.chunk_exists(manifest_uri)


def test_handler_pdf_skipped_with_message(aws_env):
    s3, _ = aws_env
    size = _put_raw(s3, "raw/report.pdf", b"%PDF-fake")
    result = handler(_event(RAW_BUCKET, "raw/report.pdf", size), context=None)
    assert result["statusCode"] == 200
    assert result["skipped"] is True
    assert "PDF" in result["reason"]


def test_handler_unknown_extension_skipped(aws_env):
    s3, _ = aws_env
    size = _put_raw(s3, "raw/data.csv", b"a,b,c")
    result = handler(_event(RAW_BUCKET, "raw/data.csv", size), context=None)
    assert result["statusCode"] == 200
    assert result["skipped"] is True
    assert "PDF" not in result["reason"]


def test_handler_oversize_skipped_without_download(aws_env):
    """Size gate runs from the event payload — no download is attempted."""
    s3, _ = aws_env
    # Don't actually write 11MB; just synthesize the event with size>limit
    result = handler(_event(RAW_BUCKET, "raw/bigfile.txt", 11 * 1024 * 1024), context=None)
    assert result["statusCode"] == 200
    assert result["skipped"] is True
    assert "too large" in result["reason"]


def test_handler_empty_source_skipped(aws_env):
    s3, _ = aws_env
    size = _put_raw(s3, "raw/blank.txt", b"   \n\n   ")
    result = handler(_event(RAW_BUCKET, "raw/blank.txt", size), context=None)
    assert result["statusCode"] == 200
    assert result["skipped"] is True
    assert "empty" in result["reason"]


def test_handler_application_error_re_raises(aws_env, monkeypatch):
    """Application errors re-raise so the Lambda runtime retries and, on
    exhaustion, routes the event to the DLQ."""
    s3, _ = aws_env
    size = _put_raw(s3, "raw/doc.md", ("Hello world. " * 200).encode("utf-8"))

    # Force pipeline.chunk_and_write to raise
    from onebucket_demo import lambda_handler_chunker as mod

    def boom(*args, **kwargs):
        raise RuntimeError("simulated downstream failure")

    monkeypatch.setattr(mod, "chunk_and_write", boom)

    with pytest.raises(RuntimeError, match="simulated downstream failure"):
        handler(_event(RAW_BUCKET, "raw/doc.md", size), context=None)


def test_handler_retry_determinism(aws_env):
    """Retry determinism (chunker): two invocations yield identical chunks +
    identical manifest fields (excluding the timestamped ``created_at``)."""
    s3, tmp_path = aws_env
    body = ("Hello world. " * 200).encode("utf-8")
    size = _put_raw(s3, "raw/doc.md", body)

    r1 = handler(_event(RAW_BUCKET, "raw/doc.md", size), context=None)
    chunk_root = str(tmp_path / "chunks")
    chunks_a = [
        storage.read_chunk(storage.chunk_uri(chunk_root, r1["raw_file_id"], i))
        for i in range(r1["n_chunks"])
    ]

    r2 = handler(_event(RAW_BUCKET, "raw/doc.md", size), context=None)
    chunks_b = [
        storage.read_chunk(storage.chunk_uri(chunk_root, r2["raw_file_id"], i))
        for i in range(r2["n_chunks"])
    ]

    assert r1["raw_file_id"] == r2["raw_file_id"]
    assert r1["n_chunks"] == r2["n_chunks"]
    assert chunks_a == chunks_b


def test_chunker_does_not_transitively_import_fastembed_or_lancedb():
    """Image-size invariant: importing the chunker handler must NOT pull
    in fastembed or lancedb. Lazy-imports in pipeline.py + storage.py keep the
    embedder stack out of the chunker import graph, so the chunker container
    image stays small."""
    import subprocess
    import sys

    code = (
        "import sys; "
        "import onebucket_demo.lambda_handler_chunker; "
        "loaded = set(sys.modules.keys()); "
        "bad = [m for m in loaded if m == 'fastembed' or m.startswith('fastembed.') "
        "       or m == 'lancedb' or m.startswith('lancedb.') "
        "       or m == 'pyarrow' or m.startswith('pyarrow.')]; "
        "print('BAD:' + ','.join(bad) if bad else 'CLEAN')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    output = result.stdout.strip()
    assert output == "CLEAN", (
        f"chunker handler transitively imports embedder-stack modules: {output}. "
        f"Lazy-imports in pipeline.py / storage.py must keep these out of the "
        f"chunker import graph so the chunker Docker image can be ~200 MB "
        f"instead of ~2 GB."
    )

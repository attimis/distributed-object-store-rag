"""Tests for the ingestion MCP tools (ingest_document, check_ingest_status,
list_documents) in src/onebucket_demo/mcp_tools.py.

Network calls (SEC EDGAR, S3, SQS) are mocked. The unit tests here verify the
boundary logic — input classification (ticker vs URL vs path), HTML→text
conversion, status-phase computation — not LanceDB or real AWS behavior.
The end-to-end behavior is covered by a live deploy + manual demo.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

from onebucket_demo import mcp_tools


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv(
        "ONEBUCKET_CHUNK_ROOT", "s3://onebucket-test-bucket/chunks"
    )
    monkeypatch.setenv(
        "ONEBUCKET_DATASET_ROOT", "s3://onebucket-test-bucket/metadata"
    )
    # ingest_document dispatches on ONEBUCKET_EXECUTION_MODE. The tests in
    # this file predate the daemon-mode heartbeat check, so we pin them to
    # lambda mode (which skips the
    # heartbeat preflight). Local-daemon dispatch is covered in the dedicated
    # tests/test_mcp_tools_local_daemon.py.
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "lambda")
    with mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="onebucket-test-bucket"
        )
        boto3.client("sqs", region_name="us-east-1").create_queue(
            QueueName="onebucket-embedder-dlq"
        )
        yield


def test_is_ticker_recognizes_uppercase_letters_only():
    assert mcp_tools._is_ticker("AAPL")
    assert mcp_tools._is_ticker("MSFT")
    assert mcp_tools._is_ticker("a")  # canonicalizes — but currently we only check upper
    assert not mcp_tools._is_ticker("AAPL.US")
    assert not mcp_tools._is_ticker("https://www.sec.gov/foo")
    assert not mcp_tools._is_ticker("/tmp/foo.html")
    assert not mcp_tools._is_ticker("NotATicker123")  # too long, mixed-case+digits


def test_is_url_detects_http_and_https():
    assert mcp_tools._is_url("https://www.sec.gov/foo")
    assert mcp_tools._is_url("http://example.com/")
    assert not mcp_tools._is_url("AAPL")
    assert not mcp_tools._is_url("/tmp/foo.html")


def test_html_to_text_strips_tags_and_compacts():
    html = "<html><body><h1>Hello</h1><p>World</p>\n\n\n\n<p>Bye</p></body></html>"
    out = mcp_tools._html_to_text(html)
    assert "Hello" in out
    assert "World" in out
    # html2text adds # for h1; just confirm the tags are gone
    assert "<h1>" not in out
    assert "</p>" not in out
    # compact runs of blank lines
    assert "\n\n\n" not in out


def test_safe_filename_sanitizes_characters():
    assert mcp_tools._safe_filename("AAPL-10k-2024", "txt") == "aapl-10k-2024.txt"
    assert mcp_tools._safe_filename("../etc/passwd", "txt").endswith(".txt")
    assert "/" not in mcp_tools._safe_filename("foo/bar/baz.html", "txt")


def test_bucket_layout_derives_from_chunk_root(monkeypatch):
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://my-bucket/chunks")
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://my-bucket/metadata")
    monkeypatch.delenv("ONEBUCKET_RAW_ROOT", raising=False)
    layout = mcp_tools._bucket_layout()
    assert layout.bucket == "my-bucket"
    assert layout.raw_prefix == "raw"
    assert layout.chunks_prefix == "chunks"
    assert layout.embedder_dlq_name == "onebucket-embedder-dlq"


def test_bucket_layout_respects_explicit_raw_root(monkeypatch):
    monkeypatch.setenv("ONEBUCKET_RAW_ROOT", "s3://other/raw-tier")
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://other/chunks")
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://other/metadata")
    layout = mcp_tools._bucket_layout()
    assert layout.bucket == "other"
    assert layout.raw_prefix == "raw-tier"


def test_bucket_layout_raises_when_no_s3_config(monkeypatch):
    monkeypatch.delenv("ONEBUCKET_CHUNK_ROOT", raising=False)
    monkeypatch.delenv("ONEBUCKET_RAW_ROOT", raising=False)
    monkeypatch.delenv("ONEBUCKET_DATASET_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="ONEBUCKET_RAW_ROOT"):
        mcp_tools._bucket_layout()


def test_ingest_document_local_path_uploads_to_s3(aws_env, tmp_path):
    """Local path input: read file, hash, upload."""
    src = tmp_path / "dummy.txt"
    src.write_text("hello world\nthis is a tiny test fixture\n")
    result = mcp_tools.ingest_document(str(src))
    assert "raw_file_id" in result
    assert len(result["raw_file_id"]) == 16
    assert result["raw_key"].startswith("raw/")
    assert result["source_kind"] == "path"
    assert "uploaded" in result["status"]

    # Verify the object actually landed in moto's S3
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket="onebucket-test-bucket", Key=result["raw_key"])
    body = obj["Body"].read().decode("utf-8")
    assert "hello world" in body


def test_ingest_document_local_html_gets_converted(aws_env, tmp_path):
    """Local HTML path: convert via html2text before upload."""
    src = tmp_path / "doc.html"
    src.write_text("<html><body><h1>Title</h1><p>Body text</p></body></html>")
    result = mcp_tools.ingest_document(str(src))

    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket="onebucket-test-bucket", Key=result["raw_key"])
    body = obj["Body"].read().decode("utf-8")
    assert "<h1>" not in body
    assert "Title" in body
    assert "Body text" in body


def test_ingest_document_url_mode_calls_httpx(aws_env):
    """URL input: fetch via httpx, then upload. We patch _fetch_remote so the
    test doesn't actually hit the network."""
    fake_html = b"<html><body>fake remote content</body></html>"
    with patch.object(mcp_tools, "_fetch_remote", return_value=fake_html):
        result = mcp_tools.ingest_document("https://example.com/doc.html")
    assert result["source_kind"] == "url"
    assert result["source_url"] == "https://example.com/doc.html"


def test_ingest_document_local_path_missing_raises(aws_env):
    with pytest.raises(RuntimeError, match="local path does not exist"):
        mcp_tools.ingest_document("/nonexistent/path/to/file.txt")


def test_check_ingest_status_uploading_phase(aws_env):
    """No chunks landed yet → uploading_or_chunker_pending phase."""
    # Patch the LanceDB connect to return an empty mock so we don't try real
    # LanceDB during this status test.
    with patch.object(mcp_tools, "connect") as mock_connect:
        empty_table = MagicMock()
        empty_table.search.return_value.where.return_value.limit.return_value.to_list.return_value = []
        mock_connect.return_value.open_table.return_value = empty_table

        result = mcp_tools.check_ingest_status("0123456789abcdef")
    assert result["raw_file_id"] == "0123456789abcdef"
    assert result["chunk_count"] == 0
    assert result["manifest_present"] is False
    assert result["rows_committed"] == 0
    assert result["phase"] == "uploading_or_chunker_pending"
    assert result["ready"] is False


def test_check_ingest_status_chunking_phase(aws_env):
    """Chunks landed but no manifest yet → chunking phase."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    raw_id = "abc1234567890def"
    for i in range(3):
        s3.put_object(
            Bucket="onebucket-test-bucket",
            Key=f"chunks/{raw_id}/chunk_{i:04d}.txt",
            Body=b"x",
        )
    with patch.object(mcp_tools, "connect") as mock_connect:
        empty_table = MagicMock()
        empty_table.search.return_value.where.return_value.limit.return_value.to_list.return_value = []
        mock_connect.return_value.open_table.return_value = empty_table
        result = mcp_tools.check_ingest_status(raw_id)
    assert result["chunk_count"] == 3
    assert result["manifest_present"] is False
    assert result["phase"] == "chunking"


def test_check_ingest_status_embedding_phase(aws_env):
    """Manifest present but no rows committed → embedding phase."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    raw_id = "1234567890abcdef"
    for i in range(2):
        s3.put_object(
            Bucket="onebucket-test-bucket",
            Key=f"chunks/{raw_id}/chunk_{i:04d}.txt",
            Body=b"x",
        )
    s3.put_object(
        Bucket="onebucket-test-bucket",
        Key=f"chunks/{raw_id}/MANIFEST.json",
        Body=b"{}",
    )
    with patch.object(mcp_tools, "connect") as mock_connect:
        empty_table = MagicMock()
        empty_table.search.return_value.where.return_value.limit.return_value.to_list.return_value = []
        mock_connect.return_value.open_table.return_value = empty_table
        result = mcp_tools.check_ingest_status(raw_id)
    assert result["manifest_present"] is True
    assert result["rows_committed"] == 0
    assert result["phase"] == "embedding"


def test_check_ingest_status_ready_phase(aws_env):
    """Rows committed >= chunk count → ready phase."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    raw_id = "fedcba9876543210"
    for i in range(2):
        s3.put_object(
            Bucket="onebucket-test-bucket",
            Key=f"chunks/{raw_id}/chunk_{i:04d}.txt",
            Body=b"x",
        )
    s3.put_object(
        Bucket="onebucket-test-bucket",
        Key=f"chunks/{raw_id}/MANIFEST.json",
        Body=b"{}",
    )
    with patch.object(mcp_tools, "connect") as mock_connect:
        rows = [{"raw_file_id": raw_id, "chunk_index": 0}, {"raw_file_id": raw_id, "chunk_index": 1}]
        table = MagicMock()
        table.search.return_value.where.return_value.limit.return_value.to_list.return_value = rows
        mock_connect.return_value.open_table.return_value = table
        result = mcp_tools.check_ingest_status(raw_id)
    assert result["rows_committed"] == 2
    assert result["phase"] == "ready"
    assert result["ready"] is True


def test_check_ingest_status_rejects_garbage_raw_file_id(aws_env):
    with pytest.raises(RuntimeError, match="invalid raw_file_id"):
        mcp_tools.check_ingest_status("../../../etc/passwd")
    with pytest.raises(RuntimeError, match="invalid raw_file_id"):
        mcp_tools.check_ingest_status("'; DROP TABLE foo")


def test_list_documents_aggregates_by_raw_file_id(aws_env):
    """list_documents groups rows by raw_file_id, counts chunks per doc."""
    rows = [
        {"raw_file_id": "doc1", "source_uri": "s3://b/raw/a.txt"},
        {"raw_file_id": "doc1", "source_uri": "s3://b/raw/a.txt"},
        {"raw_file_id": "doc1", "source_uri": "s3://b/raw/a.txt"},
        {"raw_file_id": "doc2", "source_uri": "s3://b/raw/b.txt"},
    ]
    with patch.object(mcp_tools, "connect") as mock_connect:
        table = MagicMock()
        table.search.return_value.select.return_value.limit.return_value.to_list.return_value = rows
        mock_connect.return_value.open_table.return_value = table
        result = mcp_tools.list_documents()
    assert result["total_documents"] == 2
    assert result["total_chunks"] == 4
    docs_by_id = {d["raw_file_id"]: d for d in result["documents"]}
    assert docs_by_id["doc1"]["chunk_count"] == 3
    assert docs_by_id["doc2"]["chunk_count"] == 1


def test_list_documents_empty_index(aws_env):
    with patch.object(mcp_tools, "connect") as mock_connect:
        table = MagicMock()
        table.search.return_value.select.return_value.limit.return_value.to_list.return_value = []
        mock_connect.return_value.open_table.return_value = table
        result = mcp_tools.list_documents()
    assert result["total_documents"] == 0
    assert result["total_chunks"] == 0
    assert result["documents"] == []


def test_check_ingest_status_skips_dlq_outside_lambda_mode(monkeypatch):
    """In the local modes there is no DLQ, and AWS_ENDPOINT_URL would route an
    SQS call to the S3 endpoint; the status check must not attempt it."""
    from unittest.mock import MagicMock

    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://demo-data/chunks")
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://demo-meta/metadata")
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "local-daemon")
    created = []

    def fake_client(service, **_kw):
        created.append(service)
        c = MagicMock()
        c.list_objects_v2.return_value = {"Contents": []}
        return c

    monkeypatch.setattr(mcp_tools.boto3, "client", fake_client)
    monkeypatch.setattr(mcp_tools, "connect", MagicMock(side_effect=RuntimeError("no index in this test")))
    result = mcp_tools.check_ingest_status("0123456789abcdef")
    assert "sqs" not in created
    assert result["dlq_pending"] == -1

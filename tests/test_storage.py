"""Storage backend contract suite, parametrized over local + S3 (moto).

Both backends must behave identically at the API: same URIs shapes, same
exception class (``ChunkNotFoundError``) on missing-key reads, same sidecar
round-trip. That is what lets the rest of the code treat a local directory
and an S3-compatible bucket as the same thing.
"""

from __future__ import annotations


import boto3
import pytest
from moto import mock_aws

from onebucket_demo import storage
from onebucket_demo.errors import ChunkNotFoundError

LOCAL_BACKEND = "local"
S3_BACKEND = "s3"
S3_BUCKET = "onebucket-test"


@pytest.fixture
def chunk_root(request, tmp_path):
    """Yield a chunk_root for either backend — local path or s3 URI."""
    if request.param == LOCAL_BACKEND:
        yield str(tmp_path / "chunks")
    elif request.param == S3_BACKEND:
        with mock_aws():
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=S3_BUCKET)
            # Clear cached client so it picks up the moto-mocked one
            storage._s3.client.cache_clear()
            yield f"s3://{S3_BUCKET}/chunks"
            storage._s3.client.cache_clear()
    else:
        raise ValueError(request.param)


def _backend_label(value):
    return f"backend={value}"


@pytest.mark.parametrize("chunk_root", [LOCAL_BACKEND, S3_BACKEND], indirect=True, ids=_backend_label)
class TestStorageContract:
    def test_chunk_round_trip(self, chunk_root):
        uri = storage.write_chunk(chunk_root, "deadbeef", 0, "hello world")
        assert storage.read_chunk(uri) == "hello world"

    def test_chunk_uri_format(self, chunk_root):
        uri = storage.write_chunk(chunk_root, "deadbeef", 7, "x")
        assert uri.endswith("/deadbeef/chunk_0007.txt")
        if str(chunk_root).startswith("s3://"):
            assert uri.startswith("s3://")
        else:
            assert uri.startswith("file://")

    def test_chunk_exists(self, chunk_root):
        uri = storage.write_chunk(chunk_root, "abc", 0, "yo")
        assert storage.chunk_exists(uri)
        # A URI we never wrote
        if uri.startswith("s3://"):
            ghost = uri.replace("/abc/chunk_0000.txt", "/ghost/chunk_0000.txt")
        else:
            ghost = uri.replace("/abc/chunk_0000.txt", "/ghost/chunk_0000.txt")
        assert not storage.chunk_exists(ghost)

    def test_read_chunk_missing_raises_chunk_not_found(self, chunk_root):
        # Build a chunk_uri pointer for a chunk we never wrote
        uri = storage.chunk_uri(chunk_root, "missing-id", 0)
        with pytest.raises(ChunkNotFoundError):
            storage.read_chunk(uri)

    def test_sidecar_round_trip(self, chunk_root):
        # Use the same root-style for the dataset_root variant of the path
        if str(chunk_root).startswith("s3://"):
            dataset_root = chunk_root.replace("/chunks", "/metadata")
        else:
            dataset_root = chunk_root.replace("/chunks", "/metadata")
        meta = {
            "fastembed_version": "x",
            "dense_model": "y",
            "sparse_model": "z",
            "sparse_canary_hash": "h",
            "schema_version": "1",
            "initialized_at": "2026-05-05T00:00:00Z",
        }
        storage.write_sidecar(dataset_root, meta)
        assert storage.read_sidecar(dataset_root) == meta


def test_aws_endpoint_url_honored_by_boto3(monkeypatch):
    """``_s3.client()`` honors ``AWS_ENDPOINT_URL``.

    This is the whole mechanism by which the same code talks to AWS S3 or to
    a OneBucket S3 endpoint: boto3 reads the variable at client construction
    and routes every call through it. We only inspect the constructed client;
    no request is made.
    """
    storage._s3.client.cache_clear()
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")  # moto HTTP server
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # We don't actually start moto's HTTP server here — what we're verifying is
    # that boto3 sees and uses the env var. boto3.client(...) reads AWS_ENDPOINT_URL
    # at construction time. Inspect the client's endpoint URL.
    client = storage._s3.client()
    assert client.meta.endpoint_url == "http://localhost:5000"
    storage._s3.client.cache_clear()

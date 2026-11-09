"""Embedder Lambda handler tests.

Most embedder behavior is verified at the pipeline-level (test_pipeline.py).
These tests cover the transport boundary: event-parsing, env-var routing,
error re-raise, line-count.
"""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from onebucket_demo import storage
from onebucket_demo.init_index import main as init_index
from onebucket_demo.lambda_handler_embedder import handler
from onebucket_demo.pipeline import EmbedResult, chunk_and_write

REPO_ROOT = Path(__file__).resolve().parent.parent

CHUNKS_BUCKET = "ob-test-chunks"


def _event(bucket: str, key: str) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


@pytest.fixture
def chained_setup(tmp_path, monkeypatch):
    """Init Lance dataset locally; seed s3 chunks via pipeline.chunk_and_write.

    Yields ``(manifest, dataset_root, chunk_root_uri)``. Lance lives on local
    disk (tmp_path); chunks live in moto-mocked S3. The handler will be invoked
    against a synthesized S3 event for the manifest object.
    """
    dataset_root = tmp_path / "metadata"
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", str(dataset_root))
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", f"s3://{CHUNKS_BUCKET}/chunks")
    storage._s3.client.cache_clear()

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=CHUNKS_BUCKET)

        # Init Lance dataset (local) AFTER moto is active so canary writes go to fast disk
        init_index(str(dataset_root))

        # Seed a manifest by running chunk_and_write
        fixture = tmp_path / "doc.md"
        fixture.write_text("Hello world. " * 200, encoding="utf-8")
        manifest = chunk_and_write(
            fixture,
            source_uri="s3://ob-test-raw/raw/doc.md",
            raw_object_key="raw/doc.md",
            chunk_root=f"s3://{CHUNKS_BUCKET}/chunks",
        )
        yield manifest, dataset_root
        storage._s3.client.cache_clear()


def test_handler_happy_path_commits_lance_rows(chained_setup):
    manifest, dataset_root = chained_setup
    key = f"chunks/{manifest.raw_file_id}/MANIFEST.json"

    result = handler(_event(CHUNKS_BUCKET, key), context=None)

    assert result["statusCode"] == 200
    assert result["raw_file_id"] == manifest.raw_file_id
    assert result["n_committed"] == manifest.n_chunks

    db = storage.connect(str(dataset_root))
    assert db.open_table(storage.TABLE_NAME).count_rows() == manifest.n_chunks


def test_handler_invokes_embed_and_commit_with_correct_manifest_uri(monkeypatch):
    """Transport boundary check: event → manifest_uri synthesis → pipeline call."""
    captured: dict = {}

    def fake_embed(manifest_uri, *, dataset_root, chunk_root):
        captured["manifest_uri"] = manifest_uri
        captured["dataset_root"] = dataset_root
        captured["chunk_root"] = chunk_root
        return EmbedResult(raw_file_id="x", n_committed=3)

    from onebucket_demo import lambda_handler_embedder as mod

    monkeypatch.setattr(mod, "embed_and_commit", fake_embed)
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://datasetbucket/metadata")
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://chunksbucket/chunks")

    event = _event("chunksbucket", "chunks/deadbeef/MANIFEST.json")
    result = handler(event, context=None)

    assert captured["manifest_uri"] == "s3://chunksbucket/chunks/deadbeef/MANIFEST.json"
    assert captured["dataset_root"] == "s3://datasetbucket/metadata"
    assert captured["chunk_root"] == "s3://chunksbucket/chunks"
    assert result["statusCode"] == 200
    assert result["n_committed"] == 3


def test_handler_application_error_re_raises(monkeypatch):
    """Application errors propagate so the Lambda runtime retries and, on
    exhaustion, routes the event to the DLQ."""
    from onebucket_demo import lambda_handler_embedder as mod

    def boom(*args, **kwargs):
        raise RuntimeError("simulated downstream failure")

    monkeypatch.setattr(mod, "embed_and_commit", boom)
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "x")
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "y")

    with pytest.raises(RuntimeError, match="simulated downstream failure"):
        handler(_event("b", "chunks/x/MANIFEST.json"), context=None)

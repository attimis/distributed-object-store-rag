"""Local-chain conformance test.

Wires both Lambda handlers end-to-end against moto-mocked S3:

    raw/{file} upload
        ↓ (synthesized event)
    chunker.handler — writes chunks/{id}/* + MANIFEST.json LAST
        ↓ (manifest write becomes synthesized event)
    embedder.handler — reads MANIFEST + chunks, commits Lance row

Verifies the chained-Lambda architecture (ADR-012) without requiring a deployed
AWS Lambda.
"""

from __future__ import annotations


import boto3
import pytest
from moto import mock_aws

from onebucket_demo import storage
from onebucket_demo.init_index import main as init_index
from onebucket_demo.lambda_handler_chunker import handler as chunker_handler
from onebucket_demo.lambda_handler_embedder import handler as embedder_handler

RAW_BUCKET = "ob-chain-raw"
CHUNKS_BUCKET = "ob-chain-chunks"


def _raw_event(bucket: str, key: str, size: int) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key, "size": size}}}]}


def _manifest_event(bucket: str, key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


@pytest.fixture
def chain_env(tmp_path, monkeypatch):
    """Moto S3 with raw + chunks buckets; local Lance dataset; env wired."""
    dataset_root = tmp_path / "metadata"
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", str(dataset_root))
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", f"s3://{CHUNKS_BUCKET}/chunks")
    storage._s3.client.cache_clear()
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=RAW_BUCKET)
        s3.create_bucket(Bucket=CHUNKS_BUCKET)
        init_index(str(dataset_root))
        yield s3, dataset_root
        storage._s3.client.cache_clear()


def test_local_chain_e2e_chunker_then_embedder_lance_row(chain_env):
    """Full chain: raw upload → chunker → manifest → embedder → Lance row."""
    s3, dataset_root = chain_env
    body = ("Quarterly revenue grew. " * 200).encode("utf-8")
    s3.put_object(Bucket=RAW_BUCKET, Key="raw/finance.txt", Body=body)

    # Stage 1: chunker
    chunker_result = chunker_handler(
        _raw_event(RAW_BUCKET, "raw/finance.txt", len(body)), context=None
    )
    assert chunker_result["statusCode"] == 200
    raw_file_id = chunker_result["raw_file_id"]
    n_chunks = chunker_result["n_chunks"]

    # Verify manifest landed in s3://CHUNKS_BUCKET/chunks/{id}/MANIFEST.json
    manifest_key = f"chunks/{raw_file_id}/MANIFEST.json"
    s3.head_object(Bucket=CHUNKS_BUCKET, Key=manifest_key)  # raises if missing

    # Stage 2: embedder, triggered by the manifest write
    embedder_result = embedder_handler(
        _manifest_event(CHUNKS_BUCKET, manifest_key), context=None
    )
    assert embedder_result["statusCode"] == 200
    assert embedder_result["raw_file_id"] == raw_file_id
    assert embedder_result["n_committed"] == n_chunks

    # Verify Lance has the rows
    db = storage.connect(str(dataset_root))
    assert db.open_table(storage.TABLE_NAME).count_rows() == n_chunks


def test_local_chain_chunker_partial_failure_no_embedder_trigger(chain_env, monkeypatch):
    """If chunker fails before MANIFEST is written, no manifest object exists →
    in production, the embedder is never triggered. Validates the manifest-last
    atomicity contract (ADR-012)."""
    s3, _ = chain_env
    body = ("Hello world. " * 200).encode("utf-8")
    s3.put_object(Bucket=RAW_BUCKET, Key="raw/doomed.txt", Body=body)

    # Sabotage one of the chunk writes to force the chunker to fail before the manifest
    real_write_chunk = storage.write_chunk
    call_n = {"i": 0}

    def failing_write_chunk(*args, **kwargs):
        call_n["i"] += 1
        if call_n["i"] >= 1:
            raise RuntimeError("simulated mid-chunk write failure")
        return real_write_chunk(*args, **kwargs)

    monkeypatch.setattr(storage, "write_chunk", failing_write_chunk)
    # The pipeline imports write_chunk via storage.write_chunk at call time, so the patch sticks.

    with pytest.raises(RuntimeError):
        chunker_handler(
            _raw_event(RAW_BUCKET, "raw/doomed.txt", len(body)), context=None
        )

    # The manifest should NOT exist — embedder would not be triggered
    response = s3.list_objects_v2(Bucket=CHUNKS_BUCKET, Prefix="chunks/")
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    manifests = [k for k in keys if k.endswith("MANIFEST.json")]
    assert manifests == [], (
        f"chunker partial-failure produced a MANIFEST.json — atomicity broken. Found: {manifests}"
    )

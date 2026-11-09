"""Chunker Lambda handler: a thin wrapper over ``pipeline.chunk_and_write``.

Triggered by an S3 ObjectCreated notification under ``raw/``. Downloads the
object, runs the gates, chunks it, and writes ``MANIFEST.json`` last under
``chunks/{raw_file_id}/``; the manifest's appearance triggers the embedder.

Error semantics:
  * Gate violations (type, size, empty source) log a warning and return 200
    with ``skipped: true``. Retrying would never succeed, so the event is
    consumed.
  * Everything else re-raises so the Lambda runtime retries and, if the
    failure persists, parks the event on the chunker's dead-letter queue.

The handler contains no chunking logic of its own; keeping it this small is
what lets its container image stay around 200 MB.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import unquote_plus

import boto3

from .constants import MAX_FILE_BYTES
from .gates import check_type
from .pipeline import EmptySource, chunk_and_write

logger = logging.getLogger(__name__)


def handler(event, context=None):  # noqa: ARG001 - Lambda runtime signature
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    size_bytes = int(record["s3"]["object"]["size"])
    pseudo_path = Path(key)
    ok, reason = check_type(pseudo_path)
    if not ok:
        logger.warning("chunker: skipped %s: %s", key, reason)
        return {"statusCode": 200, "skipped": True, "reason": reason}
    if size_bytes > MAX_FILE_BYTES:
        reason = f"file too large: {size_bytes} bytes (limit {MAX_FILE_BYTES})"
        logger.warning("chunker: skipped %s: %s", key, reason)
        return {"statusCode": 200, "skipped": True, "reason": reason}
    local_path = Path("/tmp") / pseudo_path.name
    boto3.client("s3").download_file(bucket, key, str(local_path))
    chunk_root = os.environ["ONEBUCKET_CHUNK_ROOT"]
    source_uri = f"s3://{bucket}/{key}"
    try:
        manifest = chunk_and_write(
            local_path,
            source_uri=source_uri,
            raw_object_key=key,
            chunk_root=chunk_root,
        )
    except EmptySource:
        return {"statusCode": 200, "skipped": True, "reason": "empty source"}
    return {
        "statusCode": 200,
        "raw_file_id": manifest.raw_file_id,
        "n_chunks": manifest.n_chunks,
    }

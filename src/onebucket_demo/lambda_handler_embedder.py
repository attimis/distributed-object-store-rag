"""Embedder Lambda handler: a thin wrapper over ``pipeline.embed_and_commit``.

Triggered by an S3 ObjectCreated notification under ``chunks/`` whose key
ends in ``MANIFEST.json``, i.e. once per document, after all its chunks are
written.

Error semantics: every failure re-raises. A schema, canary or path mismatch
means the writer/reader contract is broken and needs an operator; a LanceDB,
embedding or S3 failure is worth a retry. Either way the Lambda runtime
retries and then parks the event on the embedder's dead-letter queue.

The handler does no chunking, embedding or index writing itself; that all
lives in ``pipeline.embed_and_commit``, which the daemons and the local
ingest call too.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import unquote_plus

from .pipeline import embed_and_commit

logger = logging.getLogger(__name__)


def handler(event, context=None):  # noqa: ARG001 - Lambda runtime signature
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    manifest_uri = f"s3://{bucket}/{key}"
    result = embed_and_commit(
        manifest_uri,
        dataset_root=os.environ["ONEBUCKET_DATASET_ROOT"],
        chunk_root=os.environ["ONEBUCKET_CHUNK_ROOT"],
    )
    return {
        "statusCode": 200,
        "raw_file_id": result.raw_file_id,
        "n_committed": result.n_committed,
    }

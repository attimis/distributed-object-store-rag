"""S3 backend for the storage abstraction.

A thin wrapper over boto3 that maps S3 errors onto the package's own
exceptions. The client honours ``AWS_ENDPOINT_URL``, so the same code talks
to AWS S3 or to a OneBucket S3 endpoint depending on one environment
variable. The client is cached for the life of the process; the test suite
clears the cache between tests.
"""

from __future__ import annotations

from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

from .errors import ChunkNotFoundError, StorageWriteError


@lru_cache(maxsize=1)
def client():
    return boto3.client("s3")


def parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 URI: {uri!r}")
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 URI: {uri!r}")
    return bucket, key


def put_object(uri: str, body: bytes) -> None:
    bucket, key = parse_uri(uri)
    try:
        client().put_object(Bucket=bucket, Key=key, Body=body)
    except ClientError as exc:
        raise StorageWriteError(f"S3 PutObject failed for {uri}: {exc}") from exc


def get_object(uri: str) -> bytes:
    bucket, key = parse_uri(uri)
    try:
        response = client().get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            raise ChunkNotFoundError(f"S3 object not found: {uri}") from exc
        raise StorageWriteError(f"S3 GetObject failed for {uri}: {exc}") from exc


def head_object(uri: str) -> bool:
    bucket, key = parse_uri(uri)
    try:
        client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return False
        raise


def join_uri(root: str, *parts: str) -> str:
    base = root.rstrip("/")
    suffix = "/".join(p.strip("/") for p in parts)
    return f"{base}/{suffix}"

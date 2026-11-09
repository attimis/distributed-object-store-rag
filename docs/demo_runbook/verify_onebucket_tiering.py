#!/usr/bin/env python3
"""Ground-truth probe: where do a document's objects physically live?

OneBucket's ``locate`` and ``list`` tools (the ``data.onebucket.io``
connector) report which backing buckets hold each object. This script
checks that claim with OneBucket out of the path: it talks plain S3 to every
backing bucket of one policy and lists what is really there.

Expected picture for a chunks prefix: every sync peer holds the full set;
a cache peer holds only the chunks that have been read or prefetched
through the policy. For the metadata policy (two sync peers) both sides
should be identical.

Everything comes from ``docs/demo_runbook/variables.yml``: the policy's
peers and backing buckets, and the ``direct_access`` block of each endpoint
(an AWS CLI profile plus optional endpoint URL that reaches the backing
bucket directly). Credentials are read by boto3 from that profile; nothing
is passed on the command line or in bespoke environment variables. Peers
whose endpoint has no ``direct_access`` block are reported as skipped.

Usage:
    uv run python docs/demo_runbook/verify_onebucket_tiering.py --raw-file-id <16 hex chars>
    uv run python docs/demo_runbook/verify_onebucket_tiering.py --raw-file-id <id> --policy demo-data
    uv run python docs/demo_runbook/verify_onebucket_tiering.py --policy demo-meta --prefix metadata/
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from onebucket_demo import runbook_config  # noqa: E402


def _client(direct: dict, region: str | None):
    """Plain S3 client for one backing bucket, via its AWS CLI profile."""
    session = boto3.Session(profile_name=direct["profile"])
    return session.client(
        "s3",
        endpoint_url=direct.get("endpoint_url") or None,
        region_name=region or None,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _list(client, bucket: str, prefix: str) -> dict[str, int]:
    """Every key under ``prefix`` with its size, following pagination."""
    keys: dict[str, int] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys[obj["Key"]] = obj["Size"]
    return keys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw-file-id", help="16-char hex id from ingest_document / list_documents")
    ap.add_argument("--policy", help="policy name from variables.yml (default: pipeline.chunks_writes_to_policy)")
    ap.add_argument("--prefix", help="key prefix to compare (default: chunks/<raw-file-id>/)")
    ap.add_argument("--config", help="variables.yml path (default: docs/demo_runbook/variables.yml)")
    args = ap.parse_args()
    if not args.raw_file_id and not args.prefix:
        ap.error("pass --raw-file-id or --prefix")

    try:
        cfg = runbook_config.load_ready(args.config)
    except (FileNotFoundError, runbook_config.ConfigIncomplete) as exc:
        sys.exit(f"ERROR: {exc}")

    policy_name = args.policy or cfg["pipeline"]["chunks_writes_to_policy"]
    policy = cfg["policies"].get(policy_name)
    if not policy:
        sys.exit(f"ERROR: policy {policy_name!r} not in variables.yml (have: {', '.join(cfg['policies'])})")
    prefix = args.prefix or f"chunks/{args.raw_file_id}/"

    print(f"policy {policy_name}  prefix {prefix}")
    listings: dict[str, tuple[str, dict[str, int]]] = {}  # alias -> (type, keys)
    for peer in policy["peers"]:
        alias, ptype, bucket = peer["alias"], peer["type"], peer["bucket"]
        direct = (cfg["endpoints"].get(alias) or {}).get("direct_access")
        if not direct or not direct.get("profile"):
            print(f"  {alias:<14} {ptype:<6} {bucket}: skipped (no direct_access profile on this endpoint)")
            continue
        try:
            keys = _list(_client(direct, peer.get("region")), bucket, prefix)
        except ClientError as exc:
            print(f"  {alias:<14} {ptype:<6} {bucket}: ERROR {exc.response['Error'].get('Code')}")
            continue
        listings[alias] = (ptype, keys)
        print(f"  {alias:<14} {ptype:<6} {bucket}: {len(keys)} objects, {sum(keys.values()):,} bytes")

    sync = {a: k for a, (t, k) in listings.items() if t == "sync"}
    cache = {a: k for a, (t, k) in listings.items() if t == "cache"}
    if not sync:
        print("\nno sync peer could be listed; nothing to compare against")
        return 1

    status = 0
    union = set().union(*sync.values())
    for alias, keys in sync.items():
        missing = sorted(union - set(keys))
        if missing:
            status = 1
            print(f"\nDRIFT: sync peer {alias} is missing {len(missing)} object(s) other sync peers hold:")
            for k in missing[:10]:
                print(f"    {k}")
    if len(sync) > 1 and status == 0:
        print(f"\nsync peers agree: {len(union)} objects on every sync peer")

    source_bytes = max(sum(k.values()) for k in sync.values())
    for alias, keys in cache.items():
        extra = sorted(set(keys) - union)
        if extra:
            status = 1
            print(f"\nUNEXPECTED: cache peer {alias} holds {len(extra)} object(s) no sync peer has, e.g. {extra[0]}")
        held = sum(keys.values())
        pct = 100.0 * held / source_bytes if source_bytes else 0.0
        print(f"\ncache peer {alias}: {len(keys)} of {len(union)} objects present ({pct:.1f}% of the bytes)")
        for k in sorted(keys)[:20]:
            print(f"    {k}")
        if len(keys) > 20:
            print(f"    ... {len(keys) - 20} more")
    if not cache:
        print("\n(no cache peer on this policy; only the sync comparison applies)")
    return status


if __name__ == "__main__":
    sys.exit(main())

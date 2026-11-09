#!/usr/bin/env python3
"""End-to-end setup automation for the OneBucket RAG demo.

Takes the deployment from "the OneBucket console is provisioned" to "the demo
is ready to run in Claude Desktop" in one command. Everything deployment-
specific comes from docs/demo_runbook/variables.yml (copy the shipped
variables.example.yml and fill in the placeholders first).

Phases:
    1. Preflight: uv, variables.yml (fails closed on placeholders), OneBucket
       sign-in token, AWS CLI profile
    2. fastembed cache: stable location + pre-warm both models
    3. LanceDB initialization (idempotent — skip if table exists)
    4. Seed corpus ingest (idempotent — skip tickers already in index)
    5. Event consumer (background daemon)
    6. End-to-end verification
    7. Ready summary

Idempotent — re-running on a healthy setup logs [skip] per step and finishes
in seconds. Use --force-rebuild to wipe and re-ingest from scratch.

Usage:
    bash scripts/demo-setup.sh                # normal run
    bash scripts/demo-setup.sh --force-rebuild # wipe + re-ingest
    bash scripts/demo-setup.sh --skip-daemon   # don't start the event consumer
    bash scripts/demo-setup.sh --tickers AAPL,MSFT  # only these tickers
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NoReturn

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))


# --- Logging -----------------------------------------------------------------


_PHASE = "init"


def _set_phase(label: str) -> None:
    global _PHASE
    _PHASE = label


def _log(tag: str, msg: str) -> None:
    """[phase] [tag] msg — consistent format an SE can grep."""
    print(f"[{_PHASE}] [{tag}] {msg}", flush=True)


def _section(num: int, title: str) -> None:
    print()
    print(f"==> [{num}/7] {title}", flush=True)


def _fatal(msg: str, suggestion: str = "") -> NoReturn:
    print(f"\nFATAL: {msg}", file=sys.stderr, flush=True)
    if suggestion:
        print(f"  → {suggestion}", file=sys.stderr, flush=True)
    sys.exit(2)


# --- Phase 1: preflight ------------------------------------------------------


def preflight(args) -> dict:
    """Validate environment, load variables.yml, return the config dict."""
    _set_phase("preflight")

    # uv binary
    try:
        uv_v = subprocess.check_output(
            ["uv", "--version"], text=True, stderr=subprocess.STDOUT
        ).strip()
        _log("ok", f"uv installed: {uv_v}")
    except (FileNotFoundError, subprocess.CalledProcessError):
        _fatal(
            "uv binary not on PATH",
            "Install: curl -LsSf https://astral.sh/uv/install.sh | sh",
        )

    # Python deps importable
    try:
        from onebucket_demo import runbook_config  # noqa: F401
    except ImportError as exc:
        _fatal(
            f"Python package onebucket_demo not importable: {exc}",
            "Run `uv sync --all-extras` in the repo root.",
        )

    # variables.yml: fail closed while any <placeholder> is unfilled or the
    # file is inconsistent, so nothing below ever reaches a made-up bucket.
    from onebucket_demo import runbook_config

    try:
        config = runbook_config.load_ready()
    except FileNotFoundError as exc:
        _fatal(
            str(exc),
            f"cp {runbook_config.EXAMPLE_CONFIG_PATH} {runbook_config.DEFAULT_CONFIG_PATH} "
            "and fill in every <placeholder>.",
        )
    except runbook_config.ConfigIncomplete as exc:
        _fatal(str(exc), "Mirror the OneBucket console into variables.yml, then re-run.")
    _log("ok", f"variables.yml loaded ({len(config.get('policies', {}))} policies), validation passed")

    # OneBucket sign-in token for the event consumer (stored by
    # scripts/onebucket-login.py; mode 0600; never pasted anywhere).
    from onebucket_demo import onebucket_auth

    token_file = str(onebucket_auth.token_file_path(args.token_file))
    if not Path(token_file).expanduser().exists():
        _fatal(
            f"OneBucket token not found at {token_file}",
            "Sign in once with `uv run python scripts/onebucket-login.py` "
            "(or pass --token-file <path>).",
        )
    _log("ok", f"OneBucket token present at {token_file}")

    # AWS CLI profile holding the OneBucket S3 access key. Precedence:
    # --aws-profile, then $AWS_PROFILE, then daemon.aws_profile in variables.yml.
    yaml_profile = (config.get("daemon") or {}).get("aws_profile")
    shell_profile = os.environ.get("AWS_PROFILE")
    aws_profile = args.aws_profile or shell_profile or yaml_profile
    if not aws_profile:
        _fatal(
            "no AWS profile configured",
            "Set daemon.aws_profile in variables.yml, export AWS_PROFILE, or pass --aws-profile.",
        )
    if shell_profile and yaml_profile and shell_profile != yaml_profile and not args.aws_profile:
        _log(
            "warn",
            f"shell AWS_PROFILE={shell_profile!r} differs from variables.yml daemon.aws_profile="
            f"{yaml_profile!r}; using the shell value (pass --aws-profile to be explicit)",
        )
    try:
        result = subprocess.run(
            ["aws", "configure", "list-profiles"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        profiles = result.stdout.split()
        if aws_profile not in profiles:
            _fatal(
                f"AWS profile {aws_profile!r} not configured",
                f"Run `aws configure --profile {aws_profile}` first. "
                "Or pass --aws-profile <name>.",
            )
        _log("ok", f"AWS profile {aws_profile!r} configured")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        _log("warn", "aws CLI not on PATH or timed out — skipping profile check")

    return {
        "config": config,
        "token_file": token_file,
        "aws_profile": aws_profile,
    }


def _apply_runtime_env(state: dict) -> None:
    """Set the env vars mcp_tools / LanceDB / boto3 expect, derived from
    variables.yml. Called once after preflight so every downstream phase
    runs with consistent config — and Claude Desktop's claude_desktop_config.json
    isn't the only thing that can wire up these vars.
    """
    _set_phase("env")
    config = state["config"]
    pipeline = config["pipeline"]
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL") or config["onebucket"]["s3_endpoint"]
    if os.environ.get("AWS_ENDPOINT_URL") and os.environ["AWS_ENDPOINT_URL"] != config["onebucket"]["s3_endpoint"]:
        _log(
            "warn",
            f"shell AWS_ENDPOINT_URL={os.environ['AWS_ENDPOINT_URL']!r} differs from variables.yml "
            f"onebucket.s3_endpoint={config['onebucket']['s3_endpoint']!r}; using the shell value",
        )

    raw_policy = pipeline["raw_writes_to_policy"]
    chunks_policy = pipeline["chunks_writes_to_policy"]
    meta_policy = pipeline["metadata_writes_to_policy"]

    # First segment of each prefix (strip trailing slash and any {raw_file_id}
    # placeholder tail). variables.yml has:
    #   raw_key_prefix:      "raw/"
    #   chunks_key_prefix:   "chunks/{raw_file_id}/"
    #   metadata_key_prefix: "metadata/global_index.lance/"
    raw_prefix = pipeline["raw_key_prefix"].split("/")[0]
    chunks_prefix = pipeline["chunks_key_prefix"].split("/")[0]
    meta_prefix = pipeline["metadata_key_prefix"].split("/")[0]

    # OneBucket uses the region only for SigV4 signature derivation; the
    # product documents us-east-1 as the value to configure.
    env_updates = {
        "AWS_ENDPOINT_URL": s3_endpoint,
        "AWS_PROFILE": state["aws_profile"],
        "AWS_DEFAULT_REGION": "us-east-1",
        "ONEBUCKET_RAW_ROOT": f"s3://{raw_policy}/{raw_prefix}",
        "ONEBUCKET_CHUNK_ROOT": f"s3://{chunks_policy}/{chunks_prefix}",
        "ONEBUCKET_DATASET_ROOT": f"s3://{meta_policy}/{meta_prefix}",
    }
    for k, v in env_updates.items():
        os.environ[k] = v
        _log("set", f"{k}={v}")
    state["roots"] = {
        "raw": env_updates["ONEBUCKET_RAW_ROOT"],
        "chunks": env_updates["ONEBUCKET_CHUNK_ROOT"],
        "dataset": env_updates["ONEBUCKET_DATASET_ROOT"],
    }
    state["heartbeat_path"] = str(
        Path((config.get("daemon") or {}).get("heartbeat_path") or "/tmp/oneb-pipeline-daemon.heartbeat").expanduser()
    )
    os.environ["ONEBUCKET_DAEMON_HEARTBEAT_PATH"] = state["heartbeat_path"]


# --- Phase 2: fastembed cache + pre-warm ------------------------------------


def setup_fastembed(args) -> str:
    """Set FASTEMBED_CACHE_PATH to a stable location + pre-warm both models.

    Returns the cache path used.
    """
    _set_phase("fastembed")

    cache_path = args.fastembed_cache or os.environ.get(
        "FASTEMBED_CACHE_PATH", str(Path.home() / ".cache" / "fastembed")
    )
    cache_path = str(Path(cache_path).expanduser().resolve())
    os.environ["FASTEMBED_CACHE_PATH"] = cache_path
    Path(cache_path).mkdir(parents=True, exist_ok=True)
    _log("ok", f"FASTEMBED_CACHE_PATH = {cache_path}")

    # Pre-warm both models. This forces fastembed to download if not already
    # on disk and load into memory.
    _log("warming", "loading dense model (BGE-small) ...")
    t = time.time()
    from onebucket_demo.embed_dense import _model as dense_model

    list(dense_model().embed(["warmup"]))
    _log("ok", f"dense model ready ({time.time() - t:.1f}s)")

    _log("warming", "loading sparse model (SPLADE) ...")
    t = time.time()
    from onebucket_demo.embed_sparse import _model as sparse_model

    list(sparse_model().embed(["warmup"]))
    _log("ok", f"sparse model ready ({time.time() - t:.1f}s)")

    return cache_path


# --- Phase 3: LanceDB init ---------------------------------------------------


def init_lancedb(state: dict) -> None:
    """Idempotent: create the LanceDB table if it doesn't exist."""
    _set_phase("lancedb")
    config = state["config"]

    meta_policy = config["pipeline"]["metadata_writes_to_policy"]
    meta_prefix = config["pipeline"]["metadata_key_prefix"].rstrip("/")
    dataset_root = f"s3://{meta_policy}/{meta_prefix.split('/')[0]}"
    # variables.yml has metadata_key_prefix = "metadata/global_index.lance/"
    # so the dataset_root is s3://<policy>/metadata
    _log("info", f"dataset_root = {dataset_root}")
    # AWS_ENDPOINT_URL / AWS_PROFILE / region already set by _apply_runtime_env
    # at startup. If you're tempted to set them here too, see that helper.

    from onebucket_demo.storage import TABLE_NAME, connect

    db = connect(dataset_root)
    if TABLE_NAME in db.list_tables().tables:
        _log("skip", f"table {TABLE_NAME!r} already exists at {dataset_root}")
        return

    _log("creating", f"LanceDB table {TABLE_NAME!r} at {dataset_root}")
    # Reuse the existing init script's logic
    from onebucket_demo.init_index import main as init_main

    init_main(dataset_root=dataset_root)
    _log("ok", f"table created at {dataset_root}")


# --- Phase 4: seed corpus ingest --------------------------------------------


def ingest_seed_corpus(state: dict, args) -> None:
    """For each ticker in operator_intent.seed_corpus_tickers, fetch + chunk +
    embed. Idempotent: skips tickers whose raw_file_id is already in LanceDB.
    """
    _set_phase("ingest")
    config = state["config"]

    # Determine target tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        _log("info", f"tickers from --tickers: {tickers}")
    else:
        tickers = list(config["operator_intent"]["seed_corpus_tickers"])
        _log("info", f"tickers from variables.yml seed_corpus_tickers: {tickers}")

    if args.force_rebuild:
        _log("warn", "--force-rebuild: wiping LanceDB + chunks first")

        # rebuild_index without raw files is a no-op; we explicitly delete
        # LanceDB rows and chunks here, then proceed with ingest below.
        # Note: rebuild_index also re-processes raw/, but we want our explicit
        # ingest path, so we just delete + skip the re-process by passing
        # confirm but expecting raw/ to be empty.
        # ...Simpler: don't use rebuild_index, do it inline.
        _wipe_lancedb_and_chunks(state, skip_chunk_wipe=args.skip_chunk_wipe)

    # Inline ingest of each ticker
    from onebucket_demo import mcp_tools

    # Force lambda mode so ingest_document doesn't require a daemon heartbeat
    # (we're orchestrating manually; daemon hasn't started yet at this phase).
    os.environ["ONEBUCKET_EXECUTION_MODE"] = "lambda"

    already_indexed = _existing_raw_file_ids(state)
    _log("info", f"{len(already_indexed)} document(s) already indexed")

    for i, ticker in enumerate(tickers, 1):
        _log("processing", f"({i}/{len(tickers)}) {ticker}")

        # ingest_document does the SEC EDGAR resolve + S3 PUT
        try:
            result = mcp_tools.ingest_document(ticker)
        except Exception as exc:  # noqa: BLE001
            _log("error", f"{ticker}: ingest_document failed: {exc}")
            continue

        if "error" in result:
            _log("error", f"{ticker}: {result['error']}: {result.get('message','')}")
            continue

        raw_key = result["raw_key"]
        raw_file_id = result["raw_file_id"]

        if raw_file_id in already_indexed:
            _log("skip", f"{ticker}: raw_file_id {raw_file_id!r} already in index, skipping process")
            continue

        # Directly chunk + embed (don't wait for daemon)
        _log("chunking", f"{ticker} → raw_file_id={raw_file_id}")
        try:
            _process_one_directly(state, raw_key)
        except Exception as exc:  # noqa: BLE001
            _log("error", f"{ticker}: chunk/embed failed: {exc}")
            continue

        _log("ok", f"{ticker}: indexed")

    _log("ok", "seed corpus ingest complete")

    _ensure_fresh_ticker_absent(state)
    _ensure_dense_index(state)


def _ensure_dense_index(state: dict) -> None:
    """Build (or confirm) a LanceDB vector index on the `vector` column.

    Without an index, every dense search is a flat scan over every row
    against S3-backed column data — ~600+ ms for a few thousand vectors.
    With an IVF_PQ index, dense queries drop to ~30-50 ms. One-time
    build cost at setup, persistent across daemon/MCP-server restarts.

    Defensive: catches all failure modes (e.g. corpus too small to train
    a partitioned index) and continues. The system works without an
    index, just slower.
    """
    _set_phase("index")
    from onebucket_demo.storage import TABLE_NAME, connect

    dataset_root = state["roots"]["dataset"]

    try:
        db = connect(dataset_root)
        table = db.open_table(TABLE_NAME)
    except Exception as exc:  # noqa: BLE001
        _log("warn", f"could not open table to check/build index: {exc}")
        return

    # Already indexed? Skip.
    try:
        existing = table.list_indices()
    except Exception:  # noqa: BLE001
        existing = []
    if any(
        (getattr(idx, "column", None) == "vector")
        or (isinstance(idx, dict) and idx.get("column") == "vector")
        for idx in existing
    ):
        _log("skip", "vector index already exists on `vector` column")
        return

    try:
        row_count = table.count_rows()
    except Exception:  # noqa: BLE001
        row_count = 0
    if row_count < 256:
        # IVF_PQ training requires at least ~256 vectors per partition.
        # For a tiny corpus there's no benefit to building an index anyway —
        # flat scan is sub-100ms on a handful of rows.
        _log(
            "skip",
            f"corpus too small ({row_count} rows) — flat scan is already fast",
        )
        return

    # Choose partitions ~ sqrt(N) (rule of thumb). PQ sub-vectors: 16 is
    # standard for 384-dim BGE-small (384 / 16 = 24 dims per sub-vector).
    import math

    num_partitions = max(2, int(math.sqrt(row_count)))
    try:
        _log("building", f"IVF_PQ index on vector column (rows={row_count}, partitions={num_partitions})")
        table.create_index(
            metric="l2",
            num_partitions=num_partitions,
            num_sub_vectors=16,
        )
        _log("ok", "dense vector index built")
    except Exception as exc:  # noqa: BLE001
        _log(
            "warn",
            f"dense index build failed ({type(exc).__name__}: {exc}); "
            "flat scan fallback still works",
        )


def _ensure_fresh_ticker_absent(state: dict) -> None:
    """The runbook ingests `operator_intent.fresh_ticker_for_live_ingest`
    LIVE, and the later re-ask depends on it being ABSENT before the demo
    starts. The script otherwise leaves the index untouched between runs —
    so yesterday's fresh-ticker ingest would still be indexed today and
    silently break the demo's precondition. Detect that case and delete.
    """
    _set_phase("fresh-ticker")
    config = state["config"]
    intent = config.get("operator_intent", {}) or {}
    fresh_ticker = intent.get("fresh_ticker_for_live_ingest")
    if not fresh_ticker:
        _log("skip", "no fresh_ticker_for_live_ingest in variables.yml")
        return

    from onebucket_demo import mcp_tools

    # ingest_document names a ticker's file "<ticker>-10k-<date>.txt"; the
    # pattern is configurable (operator_intent.document_id_pattern) and is
    # matched against the filename only, so a short ticker such as "A" can
    # never match every document.
    docs_response = mcp_tools.list_documents()
    docs = docs_response.get("documents", []) if isinstance(docs_response, dict) else []
    needle = _document_needle(intent, fresh_ticker)
    matches = [
        d for d in docs
        if isinstance(d, dict)
        and (d.get("source_uri", "") or "").rsplit("/", 1)[-1].lower().startswith(needle)
    ]
    if not matches:
        _log("ok", f"fresh ticker {fresh_ticker!r} not in index (good)")
        return

    _log(
        "warn",
        f"fresh ticker {fresh_ticker!r} is indexed from a prior run "
        f"({len(matches)} match(es)); deleting so Act 0 starts clean",
    )
    for doc in matches:
        raw_file_id = doc.get("raw_file_id")
        if not raw_file_id:
            continue
        try:
            result = mcp_tools.delete_document(raw_file_id)
            if isinstance(result, dict) and "error" in result:
                _log("warn", f"delete {raw_file_id}: {result.get('error')}: {result.get('message','')}")
            else:
                _log("ok", f"deleted {raw_file_id} ({doc.get('source_uri','')})")
        except Exception as exc:  # noqa: BLE001
            _log("warn", f"delete {raw_file_id} failed: {exc}")


def _document_needle(intent: dict, ticker: str) -> str:
    """Filename prefix identifying a ticker's document, from the configured pattern."""
    pattern = intent.get("document_id_pattern") or "{ticker}-10k-"
    return pattern.format(ticker=ticker).lower()


def _process_one_directly(state: dict, raw_key: str) -> None:
    """Run chunker + embedder for a single raw_key. Same pipeline functions
    the daemon uses; we just call them inline here so the setup is
    self-sufficient (doesn't require a running daemon).
    """
    import tempfile
    import boto3
    from onebucket_demo.constants import MAX_FILE_BYTES
    from onebucket_demo.gates import check_type
    from onebucket_demo.pipeline import (
        EmptySource,
        chunk_and_write,
        embed_and_commit,
    )

    config = state["config"]
    raw_policy = config["pipeline"]["raw_writes_to_policy"]
    chunks_policy = config["pipeline"]["chunks_writes_to_policy"]
    meta_policy = config["pipeline"]["metadata_writes_to_policy"]

    s3 = boto3.client("s3")
    ok, reason = check_type(Path(raw_key))
    if not ok:
        raise RuntimeError(f"gate failed: {reason}")
    head = s3.head_object(Bucket=raw_policy, Key=raw_key)
    if int(head.get("ContentLength", 0)) > MAX_FILE_BYTES:
        raise RuntimeError(f"file too large: {head['ContentLength']} > {MAX_FILE_BYTES}")

    local_path = Path(tempfile.gettempdir()) / Path(raw_key).name
    s3.download_file(raw_policy, raw_key, str(local_path))
    chunk_root = state["roots"]["chunks"]
    dataset_root = state["roots"]["dataset"]
    del chunks_policy, meta_policy  # roots already encode the policies

    try:
        manifest = chunk_and_write(
            local_path,
            source_uri=f"s3://{raw_policy}/{raw_key}",
            raw_object_key=raw_key,
            chunk_root=chunk_root,
        )
    except EmptySource:
        raise RuntimeError("empty source after chunker preprocess")

    manifest_uri = f"{chunk_root}/{manifest.raw_file_id}/MANIFEST.json"
    embed_and_commit(
        manifest_uri,
        dataset_root=dataset_root,
        chunk_root=chunk_root,
    )


def _existing_raw_file_ids(state: dict) -> set[str]:
    """raw_file_ids currently in LanceDB so we can skip the chunk/embed step
    for tickers already indexed. The LanceDB schema (see
    onebucket_demo.schema) has raw_file_id (content-hash) and source_uri but
    no raw_object_key column — that field lives on the S3-side MANIFEST,
    not the index. raw_file_id is the right dedup key anyway since it's
    content-addressed.
    """
    from onebucket_demo.storage import TABLE_NAME, connect

    dataset_root = state["roots"]["dataset"]
    db = connect(dataset_root)
    if TABLE_NAME not in db.list_tables().tables:
        return set()
    table = db.open_table(TABLE_NAME)
    arrow = (
        table.search()
        .select(["raw_file_id"])
        .limit(10**9)
        .to_arrow()
    )
    return set(arrow.column("raw_file_id").to_pylist())


def _kill_running_daemon() -> None:
    """Stop any in-flight event consumer before --force-rebuild touches state.
    Otherwise the daemon races the wipe and re-creates rows from buffered
    events. Also clears the stale heartbeat so the next start_daemon() phase
    doesn't false-positive on a dead PID file.
    """
    try:
        subprocess.run(
            ["pkill", "-f", "oneb_event_consumer"],
            check=False,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        _log("warn", "pkill timed out; daemon may still be running")
    heartbeat = Path(os.environ.get("ONEBUCKET_DAEMON_HEARTBEAT_PATH", "/tmp/oneb-pipeline-daemon.heartbeat"))
    if heartbeat.exists():
        heartbeat.unlink()
    _log("ok", "stopped daemon (if any) and cleared heartbeat")


def _purge_cache_peers(state: dict, purge_policy_override: str | None = None) -> None:
    """Purge the cache peer of the WRITE policy (or `purge_policy_override`).

    The data model:
      - The pipeline writes originals through ONE policy
        (`pipeline.chunks_writes_to_policy`).
      - Originals land on that policy's *sync* peers.
      - Every other bucket in the deployment — including buckets that are
        configured as `type: sync` under a *different* policy in a
        bidirectional A↔B setup — only ever holds cache-fill bytes that
        auto-populated from reads. They are derived data, safe to purge.

    Safety invariant: refuse to purge any bucket that is a sync peer of
    the WRITE policy. Other policies' sync roles are irrelevant — they
    don't write originals.

    With `purge_policy_override`, the operator names a non-write policy
    explicitly. We still enforce the same safety invariant: a bucket that
    holds the write policy's sync data is refused even when reached via
    another policy's cache role.
    """
    import boto3

    config = state["config"]
    policies = config.get("policies", {})
    endpoints = config.get("endpoints", {})

    write_policy_name = config["pipeline"]["chunks_writes_to_policy"]
    write_policy = policies.get(write_policy_name)
    if not write_policy:
        _log("warn", f"write policy {write_policy_name!r} not in policies block; nothing to purge")
        return

    target_policy_name = purge_policy_override or write_policy_name
    target_policy = policies.get(target_policy_name)
    if not target_policy:
        _log("warn", f"target policy {target_policy_name!r} not found")
        return
    if target_policy_name != write_policy_name:
        _log(
            "info",
            f"purge target overridden: {target_policy_name!r} "
            f"(write policy is {write_policy_name!r})",
        )

    # Buckets where the pipeline writes originals. Never purge these,
    # no matter which policy reaches them. In a bidirectional config the
    # SAME bucket is also a sync peer of the reverse policy and that role
    # is fine to ignore — but the write-policy sync role is sacred.
    write_originals_buckets = {
        p["bucket"]
        for p in write_policy.get("peers", [])
        if p.get("type") == "sync"
    }

    for peer in target_policy.get("peers", []):
        if peer.get("type") != "cache":
            continue
        alias = peer["alias"]
        bucket = peer["bucket"]
        if bucket in write_originals_buckets:
            _log(
                "skip",
                f"{target_policy_name}/{alias} ({bucket}): bucket holds "
                f"write-policy originals (sync peer of {write_policy_name!r}) "
                "— refusing to purge",
            )
            continue
        ep = endpoints.get(alias, {})
        direct = ep.get("direct_access") or {}
        if not direct.get("profile"):
            _log(
                "skip",
                f"{write_policy_name}/{alias}: no direct_access configured, "
                "can't purge — skipping",
            )
            continue
        session = boto3.Session(profile_name=direct["profile"])
        client_kwargs = {}
        # endpoint_url precedence:
        # 1. direct_access.endpoint_url if set (e.g. Wasabi region URL)
        # 2. parent endpoint.url (e.g. AWS https://s3.amazonaws.com:443)
        # 3. None — let boto3 pick the default
        # Set explicitly even for AWS: the preflight exported
        # AWS_ENDPOINT_URL=<OneBucket S3 endpoint> for this process, and an
        # unset endpoint_url would make boto3 send these backing-bucket
        # requests (signed with the backing provider's key) to OneBucket,
        # which rejects them.
        if direct.get("endpoint_url"):
            client_kwargs["endpoint_url"] = direct["endpoint_url"]
        elif ep.get("url"):
            client_kwargs["endpoint_url"] = ep["url"]
        if peer.get("region"):
            client_kwargs["region_name"] = peer["region"]
        cache_s3 = session.client("s3", **client_kwargs)
        _log("purging", f"{write_policy_name}/{alias} → s3://{bucket}/chunks/")
        try:
            from onebucket_demo.mcp_tools import _batch_delete, _list_keys

            keys = _list_keys(cache_s3, bucket, "chunks/")
            if not keys:
                _log("ok", f"{alias}: nothing to purge")
                continue
            n = _batch_delete(cache_s3, bucket, keys)
            _log("ok", f"{alias}: purged {n} chunk key(s)")
        except Exception as exc:  # noqa: BLE001
            _log("warn", f"{alias}: purge failed: {exc}")

    # Addendum cache peers: buckets used by optional follow-on segments
    # (a "spin up a new region" finale) that the operator wires into
    # policies in the OneBucket console mid-demo. Those buckets are NOT in variables.yml's
    # `policies:` block at rest, so the policy-driven loop above doesn't see
    # them. But any chunks on them are by definition cache-fill data — safe
    # to purge between dry-runs so stale tiles don't show up next time.
    addendum_aliases = (
        config.get("operator_intent", {}).get("addendum_cache_peers") or []
    )
    if not addendum_aliases:
        return
    for alias in addendum_aliases:
        ep = endpoints.get(alias, {})
        direct = ep.get("direct_access") or {}
        if not direct.get("profile"):
            _log("skip", f"addendum/{alias}: no direct_access configured")
            continue
        # We don't know the exact bucket name from the endpoint block alone
        # (peers carry the bucket name). For addendum peers that aren't
        # currently in any policy, fall back to listing all buckets the
        # profile can see and finding chunk prefixes — OR require the
        # operator to record the bucket somewhere we can read. Simplest:
        # encode the bucket on the endpoint via an optional `addendum_bucket`
        # field; if absent, skip with a clear message.
        bucket = ep.get("addendum_bucket")
        if not bucket:
            _log(
                "skip",
                f"addendum/{alias}: no `addendum_bucket` set on endpoint; "
                "add the bucket name to variables.yml endpoints.<alias>.addendum_bucket "
                "to enable purge",
            )
            continue
        if bucket in write_originals_buckets:
            _log(
                "skip",
                f"addendum/{alias} ({bucket}): bucket holds write-policy "
                "originals — refusing to purge",
            )
            continue
        session = boto3.Session(profile_name=direct["profile"])
        client_kwargs = {}
        if direct.get("endpoint_url"):
            client_kwargs["endpoint_url"] = direct["endpoint_url"]
        elif ep.get("url"):
            client_kwargs["endpoint_url"] = ep["url"]
        addendum_s3 = session.client("s3", **client_kwargs)
        _log("purging", f"addendum/{alias} → s3://{bucket}/chunks/")
        try:
            from onebucket_demo.mcp_tools import _batch_delete, _list_keys

            keys = _list_keys(addendum_s3, bucket, "chunks/")
            if not keys:
                _log("ok", f"addendum/{alias}: nothing to purge")
                continue
            n = _batch_delete(addendum_s3, bucket, keys)
            _log("ok", f"addendum/{alias}: purged {n} chunk key(s)")
        except Exception as exc:  # noqa: BLE001
            _log("warn", f"addendum/{alias}: purge failed: {exc}")


def _wipe_lancedb_and_chunks(state: dict, skip_chunk_wipe: bool = False) -> None:
    """--force-rebuild support: stop daemon → wipe LanceDB rows → wipe S3
    chunks on the sync source → purge cache peers. Order matters: daemon off
    first so it doesn't race the wipe, then LanceDB (so search stops returning
    rows immediately), then chunks on source, then cache peers.

    If `skip_chunk_wipe=True`, the S3 chunk wipe step is skipped. Re-ingest
    writes content-addressed keys (`chunks/<raw_file_id>/chunk_NNNN.txt`),
    so existing keys for the same content are overwritten in place and any
    orphan keys from prior content are dead bytes (space-only, not a
    correctness issue). Use this when the gateway is flapping with Bad
    Gateway errors and you don't want to wait for a doomed delete loop.
    """
    import boto3
    from onebucket_demo.mcp_tools import _batch_delete, _list_keys
    from onebucket_demo.storage import TABLE_NAME, connect

    config = state["config"]
    chunks_policy = config["pipeline"]["chunks_writes_to_policy"]
    meta_policy = config["pipeline"]["metadata_writes_to_policy"]

    _kill_running_daemon()

    s3 = boto3.client("s3")

    db = connect(state["roots"]["dataset"])
    if TABLE_NAME in db.list_tables().tables:
        _log("wiping", "LanceDB table rows")
        db.open_table(TABLE_NAME).delete("raw_file_id != ''")
    del meta_policy

    if skip_chunk_wipe:
        _log("skip", "S3 chunk wipe (--skip-chunk-wipe set); re-ingest will overwrite")
    else:
        _log("wiping", f"S3 chunks/ on source policy {chunks_policy!r}")
        chunk_keys = _list_keys(s3, chunks_policy, "chunks/")
        n = _batch_delete(s3, chunks_policy, chunk_keys)
        _log("ok", f"deleted {n} chunk key(s) from source")

    _purge_cache_peers(state)


# --- Phase 5: event consumer (background daemon) ----------------------------


def start_daemon(state: dict, args) -> None:
    _set_phase("daemon")
    if args.skip_daemon:
        _log("skip", "--skip-daemon set; not starting consumer")
        return

    # Always restart rather than detect-and-skip: a consumer left over from a
    # previous session may hold a stale cursor or an expired sign-in, and a
    # restart costs two seconds.
    _kill_running_daemon()

    config = state["config"]
    env = os.environ.copy()
    env["ONEBUCKET_EVENTS_URL"] = os.environ.get("ONEBUCKET_EVENTS_URL") or config["onebucket"]["events_url"]
    env["ONEBUCKET_TOKEN_FILE"] = state["token_file"]
    env["AWS_PROFILE"] = state["aws_profile"]
    env["AWS_ENDPOINT_URL"] = os.environ["AWS_ENDPOINT_URL"]
    env["ONEBUCKET_CHUNK_ROOT"] = state["roots"]["chunks"]
    env["ONEBUCKET_DATASET_ROOT"] = state["roots"]["dataset"]
    env["ONEBUCKET_DAEMON_HEARTBEAT_PATH"] = state["heartbeat_path"]

    log_path = Path(tempfile.gettempdir()) / "oneb-event-consumer-setup.log"
    _log("starting", f"consumer in background (log: {log_path})")
    with open(log_path, "w") as log_fh:
        proc = subprocess.Popen(
            ["bash", str(_REPO_ROOT / "scripts" / "start-event-consumer.sh")],
            env=env,
            cwd=str(_REPO_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _log("info", f"consumer PID {proc.pid}; waiting up to 30s for heartbeat ...")

    heartbeat_path = Path(state["heartbeat_path"])
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(2)
        if proc.poll() is not None:
            _fatal(
                f"consumer exited with code {proc.returncode} during startup",
                f"Read {log_path}; an exit code of 3 means the OneBucket sign-in is "
                "missing or expired: run `uv run python scripts/onebucket-login.py`.",
            )
        if heartbeat_path.exists():
            try:
                hb = json.loads(heartbeat_path.read_text())
                if hb.get("auth_state", "ok") != "ok":
                    _fatal(
                        f"consumer reports auth_state={hb['auth_state']!r}",
                        "Sign in again with `uv run python scripts/onebucket-login.py`.",
                    )
                if hb.get("connected") is True:
                    _log("ok", f"consumer connected (events_received={hb.get('events_received', 0)})")
                    return
            except (json.JSONDecodeError, OSError):
                continue
    _log("warn", f"consumer started but not connected within 30s; check {log_path}")


# --- Phase 6: verification --------------------------------------------------


def verify_ready(state: dict) -> None:
    _set_phase("verify")
    from onebucket_demo import mcp_tools

    docs = mcp_tools.list_documents()
    n_docs = docs.get("total_documents", 0)
    n_chunks = docs.get("total_chunks", 0)
    _log("ok", f"list_documents: {n_docs} docs, {n_chunks} chunks")

    if n_docs == 0:
        _fatal(
            "list_documents returned 0 — index is empty",
            "Check the previous phase logs for ingest errors.",
        )

    # Hybrid search sanity
    from onebucket_demo.search import search_global_index

    try:
        result = search_global_index(
            "risk factors",
            limit=3,
            dataset_root=state["roots"]["dataset"],
        )
    except Exception as exc:  # noqa: BLE001
        _fatal(
            f"search_global_index failed: {exc}",
            "If the error mentions a missing ONNX file, the fastembed model "
            "isn't on disk — re-run this script (or wipe FASTEMBED_CACHE_PATH "
            "and re-run).",
        )

    sem = len(result.get("semantic_matches", []))
    kw = len(result.get("keyword_matches", []))
    if sem == 0 and kw == 0:
        _fatal(
            "search returned no semantic OR keyword matches",
            "Index has documents but search isn't hitting them. Likely model "
            "mismatch or empty embeddings.",
        )
    _log("ok", f"hybrid search returned {sem} semantic + {kw} keyword matches")


# --- Phase 7: ready summary -------------------------------------------------


def _print_demo_prompt() -> None:
    """Print the start-the-demo prompt with absolute paths.

    Called from every successful exit (full setup, --purge-cache, etc.)
    so the operator never has to scroll back through old terminal output
    or remember the prompt. Uses absolute paths so the prompt works
    regardless of which workspace Claude Desktop is pointed at.
    """
    runbook = _REPO_ROOT / "docs" / "demo_runbook" / "onebucket_rag_demo_runbook.md"
    visual = _REPO_ROOT / "docs" / "demo_runbook" / "cache_warming_visual.html"
    variables = _REPO_ROOT / "docs" / "demo_runbook" / "variables.yml"

    print()
    print("─" * 70)
    print("  📋 START-THE-DEMO PROMPT (paste into a fresh Claude Desktop chat)")
    print("─" * 70)
    print()
    print("> Open and follow the demo runbook at:")
    print(f"> `{runbook}`")
    print(">")
    print("> Start at the pre-demo checklist and run straight through to Act 4")
    print("> without pausing for operator approval between acts. The runbook")
    print("> has exactly one mandatory operator check-in (after Act 2): stop")
    print("> there, wait for me, then continue. Everywhere else, proceed.")
    print(">")
    print("> Render each Act's Summary Card before moving on.")
    print(">")
    print("> The MCP servers you need (this repo's `onebucket-demo` server and")
    print("> the OneBucket `data` connector) are configured in Claude Desktop;")
    print("> call them directly. The chunk-grid visual for Act 4 is at:")
    print(f"> `{visual}`")
    print(">")
    print("> Deployment config (read-only reference) is at:")
    print(f"> `{variables}`")
    print()
    print("─" * 70)
    print()


def print_ready(state: dict) -> None:
    print()
    print("=" * 70)
    print("  ✅ DEMO READY")
    print("=" * 70)
    print()
    print("  1. Open a fresh chat in Claude Desktop (Cmd-N).")
    print("     ⚠️  ONLY IF this repo's MCP server code")
    print("        changed since your last run: fully quit Claude Desktop")
    print("        first (Cmd-Q) and relaunch, so the subprocess restarts")
    print("        with the new code. Otherwise, fresh chat is enough.")
    print("  2. Paste the prompt below.")
    _print_demo_prompt()
    print(f"    Consumer: PID in the heartbeat at {state.get('heartbeat_path', '')}; indexed documents: see the verification phase above.")
    print("To stop the event consumer: pkill -f oneb_event_consumer.py")
    print(
        "To re-run this setup with a clean slate: "
        "bash scripts/demo-setup.sh --force-rebuild"
    )
    print(
        "To purge the cache between dry-runs (no re-ingest): "
        "bash scripts/demo-setup.sh --purge-cache"
    )
    print()


# --- main -------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="demo-setup", description=__doc__)
    p.add_argument(
        "--force-rebuild",
        action="store_true",
        help=(
            "Full reset: stop daemon, wipe LanceDB + source chunks, "
            "purge cache peers, then re-ingest seed corpus "
            "(default: idempotent skip)"
        ),
    )
    p.add_argument(
        "--skip-daemon",
        action="store_true",
        help="Don't start the event consumer (useful if you already have one running)",
    )
    p.add_argument(
        "--purge-cache",
        action="store_true",
        help=(
            "Pre-demo reset of cache peers only — leaves LanceDB index, "
            "source chunks, and daemon alone. Use this when re-running the "
            "demo so Act 2's cold-vs-warm measurement starts from cold."
        ),
    )
    p.add_argument(
        "--purge-policy",
        default=None,
        help=(
            "Override which policy's cache peer to purge. Defaults to "
            "pipeline.chunks_writes_to_policy. Buckets that hold the write "
            "policy's sync data are still refused regardless."
        ),
    )
    p.add_argument(
        "--skip-chunk-wipe",
        action="store_true",
        help=(
            "On --force-rebuild, skip the S3 chunk wipe step. Useful when "
            "the gateway is flapping with Bad Gateway errors — re-ingest "
            "writes content-addressed keys that overwrite the same paths "
            "anyway, so orphans are space-only, not a correctness issue."
        ),
    )
    p.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated tickers to ingest (overrides operator_intent.seed_corpus_tickers)",
    )
    p.add_argument(
        "--token-file",
        default=None,
        help="OneBucket token file (default: $ONEBUCKET_TOKEN_FILE or ~/.config/onebucket/token.json)",
    )
    p.add_argument(
        "--aws-profile",
        default=None,
        help="AWS CLI profile with the OneBucket access key (default: $AWS_PROFILE, then daemon.aws_profile)",
    )
    p.add_argument(
        "--fastembed-cache",
        default=None,
        help="fastembed cache dir (default: $FASTEMBED_CACHE_PATH or ~/.cache/fastembed)",
    )
    return p.parse_args()


def main() -> int:
    started = time.time()
    print("==> OneBucket RAG Demo Setup")
    print(f"    Started: {_dt.datetime.now().isoformat(timespec='seconds')}")

    args = parse_args()

    _section(1, "Preflight checks")
    state = preflight(args)
    _apply_runtime_env(state)

    # --purge-cache: short-circuit branch. Reset cache peers and exit so the
    # operator can re-run the demo with cold cache, without paying the
    # 10-minute re-ingest cost that --force-rebuild incurs.
    if args.purge_cache:
        _section(2, "Cache peer purge")
        _set_phase("purge")
        _purge_cache_peers(state, purge_policy_override=args.purge_policy)

        # Also restart the consumer so a stale sign-in or cursor from a
        # previous session never surfaces mid-demo.
        _section(3, "Event consumer (background) — restart")
        start_daemon(state, args)

        print()
        print("=" * 70)
        print("  ✅ CACHE PEERS PURGED + DAEMON RESTARTED — ready for fresh demo")
        print("=" * 70)
        _print_demo_prompt()
        print(f"    Total elapsed: {time.time() - started:.0f}s")
        return 0

    _section(2, "fastembed cache + model pre-warm")
    setup_fastembed(args)

    _section(3, "LanceDB initialization")
    init_lancedb(state)

    _section(4, "Seed corpus ingest")
    ingest_seed_corpus(state, args)

    _section(5, "Event consumer (background daemon)")
    start_daemon(state, args)

    _section(6, "End-to-end verification")
    verify_ready(state)

    _section(7, "Ready summary")
    print_ready(state)

    print(f"    Total elapsed: {time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())

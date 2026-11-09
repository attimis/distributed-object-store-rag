#!/usr/bin/env python3
"""Demo metrics over the event log the consumer writes.

Reads ``<repo>/.runbook-state/events.jsonl`` (or ``--events PATH``), where
every line is one OneBucket event as the consumer recorded it. A request to a
OneBucket produces one ``gateway`` record (what the client experienced) and
one ``backend`` record per peer bucket it reached, all sharing ``req_id``.
That join is the basis of every number below.

Sections printed:
  1. Schema profile: the fields actually present, action / bucket / outcome
     distributions. If a field this script relies on is missing it says so
     instead of printing a confident wrong number.
  2. M-HEADLINE: per-peer acknowledgement lag on writes to the metadata
     OneBucket (the strict-sync tier).
  3. M-PIPELINE: chunker and embedder latency per ingested document.
  4. M-SCALE: read latency (gateway duration_ms) by OneBucket.
  5. Event volume per minute.

M-COST (bytes copied to a cache peer versus the source size) is computed by
``--m-cost`` from a per-object ``locate``/``list`` listing, because cache
fills are not reported as PutObject events.

Policy and peer names come from variables.yml (``docs/demo_runbook/``); pass
``--config PATH`` to use another file.

Usage:
    uv sync --extra demo                        # installs duckdb
    uv run python docs/demo_runbook/metrics.py
    uv run python docs/demo_runbook/metrics.py --events PATH
    uv run python docs/demo_runbook/metrics.py --m-cost LOCATIONS.json --raw-size N [--cache-peer ALIAS]
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print(
        "ERROR: this script needs DuckDB. Install the demo extra once:\n"
        "    uv sync --extra demo\n"
        "then re-run with `uv run python docs/demo_runbook/metrics.py`.",
        file=sys.stderr,
    )
    sys.exit(2)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))
from onebucket_demo import runbook_config  # noqa: E402

DEFAULT_EVENTS = str(_REPO_ROOT / ".runbook-state" / "events.jsonl")
REQUIRED_FIELDS = {
    "event_id", "req_id", "layer", "action", "bucket", "key", "status_code",
    "bytes_in", "bytes_out", "event_time", "duration_ms", "dispatch_outcome",
}


def load_names(config_path: str | None) -> dict:
    """Policy names and the cache peer alias of the write policy, from config."""
    cfg = runbook_config.load(config_path)
    pipeline = cfg["pipeline"]
    write_policy = pipeline["chunks_writes_to_policy"]
    peers = (cfg.get("policies", {}).get(write_policy, {}) or {}).get("peers") or []
    cache_peers = [p["alias"] for p in peers if p.get("type") == "cache"]
    return {
        "raw_policy": pipeline["raw_writes_to_policy"],
        "chunks_policy": write_policy,
        "meta_policy": pipeline["metadata_writes_to_policy"],
        "cache_peer": cache_peers[0] if cache_peers else None,
    }


# ---------------------------------------------------------------------------
# 1. Schema profile
# ---------------------------------------------------------------------------


def profile_events(events_path: str) -> dict:
    actions, buckets, outcomes, layers = Counter(), Counter(), Counter(), Counter()
    columns: set[str] = set()
    total = 0
    with open(events_path, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                e = json.loads(ln)
            except json.JSONDecodeError:
                continue
            total += 1
            columns |= set(e.keys())
            actions[e.get("action")] += 1
            buckets[e.get("bucket")] += 1
            outcomes[e.get("dispatch_outcome")] += 1
            layers[e.get("layer")] += 1
    return {"total": total, "columns": sorted(columns), "actions": actions, "buckets": buckets,
            "outcomes": outcomes, "layers": layers, "missing": sorted(REQUIRED_FIELDS - columns)}


def print_profile(p: dict) -> None:
    print("=" * 72)
    print(f"Schema profile: {p['total']} events")
    print("=" * 72)
    print("columns:", ", ".join(p["columns"]))
    for name in ("layers", "actions", "buckets", "outcomes"):
        print(f"\n{name}:")
        for k, n in p[name].most_common():
            print(f"  {k!s:<40} {n:>7}")
    if p["missing"]:
        print(f"\nWARNING: fields this script relies on are MISSING: {p['missing']}")
        print("         the sections below may be empty or wrong until the consumer writes them.")
    else:
        print("\nAll required fields present.")


# ---------------------------------------------------------------------------
# DuckDB view + queries
# ---------------------------------------------------------------------------


def connect_events(con: duckdb.DuckDBPyConnection, events_path: str) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE VIEW events AS
        SELECT * FROM read_json_auto('{events_path}', format='newline_delimited', ignore_errors=true)
        """
    )


M_HEADLINE_SQL = """
WITH gw AS (
    SELECT req_id, key, bytes_in, event_time
    FROM events
    WHERE layer = 'gateway' AND action = 's3:PutObject' AND bucket = ? AND status_code = 200
), legs AS (
    SELECT gw.key, gw.bytes_in, b.endpoint AS peer, b.status_code AS peer_status,
           epoch_ms(b.event_time::timestamp) - epoch_ms(gw.event_time::timestamp) AS ack_lag_ms
    FROM gw JOIN events b ON b.req_id = gw.req_id AND b.layer = 'backend'
)
SELECT peer, count(*) AS puts,
       round(quantile_cont(ack_lag_ms, 0.5)) AS p50_ms,
       round(quantile_cont(ack_lag_ms, 0.95)) AS p95_ms,
       round(max(ack_lag_ms)) AS max_ms
FROM legs GROUP BY peer ORDER BY peer;
"""


def m_headline(con, names: dict) -> None:
    meta = names["meta_policy"]
    print("\n" + "=" * 72)
    print(f"M-HEADLINE: per-peer ack lag on writes to {meta} (strict sync)")
    print("=" * 72)
    rows = con.execute(M_HEADLINE_SQL, [meta]).fetchall()
    if not rows:
        print(f"\n(no {meta} PutObject events with backend legs; run an ingest with the consumer on)")
        return
    for peer, n, p50, p95, mx in rows:
        print(f"  peer={peer!s:<22} n={n:>4}  p50={p50:>6} ms  p95={p95:>6} ms  max={mx:>6} ms")


def m_pipeline(events_path: str, names: dict) -> None:
    """Chunker latency = raw PUT -> MANIFEST PUT; embedder = MANIFEST -> first metadata PUT."""
    print("\n" + "=" * 72)
    print("M-PIPELINE: chunker and embedder latency per ingested document")
    print("=" * 72)
    raws, manifests, metas = [], [], []
    with open(events_path, encoding="utf-8") as f:
        for ln in f:
            try:
                e = json.loads(ln)
            except json.JSONDecodeError:
                continue
            if e.get("action") != "s3:PutObject" or e.get("layer") != "gateway":
                continue
            k = e.get("key") or ""
            if e.get("bucket") == names["raw_policy"] and k.startswith("raw/") and e.get("dispatch_outcome") == "dispatch_chunker":
                raws.append(e)
            elif k.endswith("MANIFEST.json"):
                manifests.append(e)
            elif e.get("bucket") == names["meta_policy"] and k.startswith("metadata/"):
                metas.append(e)

    def ts(e):
        return datetime.fromisoformat((e.get("event_time") or "").replace("Z", "+00:00"))

    if not raws:
        print("\n(no dispatched raw PUTs in this log)")
        return
    print(f"\n  {'raw key (tail)':<44} {'chunker s':>10} {'embedder s':>11}")
    for r in sorted(raws, key=ts):
        rid = r.get("dispatch_raw_file_id") or ""
        man = next((m for m in sorted(manifests, key=ts) if rid and rid in (m.get("key") or "") and ts(m) >= ts(r)), None)
        meta = next((m for m in sorted(metas, key=ts) if man and ts(m) >= ts(man)), None)
        chunker = f"{(ts(man) - ts(r)).total_seconds():.1f}" if man else "-"
        embedder = f"{(ts(meta) - ts(man)).total_seconds():.1f}" if man and meta else "-"
        tail = (r.get("key") or "")[-42:]
        print(f"  {tail:<44} {chunker:>10} {embedder:>11}")


M_SCALE_SQL = """
SELECT bucket, count(*) AS n,
       round(quantile_cont(duration_ms, 0.5)) AS p50,
       round(quantile_cont(duration_ms, 0.95)) AS p95,
       max(duration_ms) AS mx
FROM events
WHERE layer = 'gateway' AND action = 's3:GetObject' AND status_code IN (200, 206) AND key LIKE 'chunks/%.txt'
GROUP BY bucket ORDER BY bucket;
"""


def m_scale(con) -> None:
    print("\n" + "=" * 72)
    print("M-SCALE: chunk read latency (gateway duration_ms) by OneBucket")
    print("=" * 72)
    rows = con.execute(M_SCALE_SQL).fetchall()
    if not rows:
        print("\n(no chunk GetObject events)")
        return
    print(f"\n  {'OneBucket':<24} {'n':>6} {'p50 ms':>8} {'p95 ms':>8} {'max ms':>8}")
    for b, n, p50, p95, mx in rows:
        print(f"  {b!s:<24} {n:>6} {p50:>8} {p95:>8} {mx:>8}")


SANITY_SQL = """
SELECT date_trunc('minute', event_time::timestamp) AS minute,
       count(*) AS events,
       count_if(dispatch_outcome = 'dispatch_chunker') AS dispatched,
       count_if(dispatch_outcome LIKE 'skip_%') AS skipped
FROM events GROUP BY 1 ORDER BY 1;
"""


def volume(con) -> None:
    print("\n" + "=" * 72)
    print("Event volume per minute")
    print("=" * 72)
    rows = con.execute(SANITY_SQL).fetchall()
    if not rows:
        print("\n(empty log)")
        return
    print(f"\n  {'minute':<26} {'events':>7} {'dispatched':>11} {'skipped':>8}")
    for minute, events, disp, skip in rows:
        print(f"  {minute!s:<26} {events:>7} {disp:>11} {skip:>8}")


# ---------------------------------------------------------------------------
# M-COST from a locations listing
# ---------------------------------------------------------------------------


def m_cost(locations_path: str, raw_size: int, cache_peer: str) -> None:
    """T/R: bytes present on the cache peer over the source document size."""
    print("\n" + "=" * 72)
    print(f"M-COST: bytes on cache peer {cache_peer!r} vs source size {raw_size:,} B")
    print("=" * 72)
    text = Path(locations_path).read_text(encoding="utf-8").strip()
    try:
        payload = json.loads(text)
        objs = payload.get("objects") or [payload] if isinstance(payload, dict) else payload
    except json.JSONDecodeError:
        objs = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
    transferred = sum((o.get("size") or 0) for o in objs if cache_peer in (o.get("locations") or []))
    warm = sum(1 for o in objs if cache_peer in (o.get("locations") or []))
    pct = 100.0 * transferred / raw_size if raw_size else 0.0
    print(f"\n  chunks present on the cache peer: {warm} of {len(objs)}")
    print(f"  T (bytes on cache peer)   = {transferred:>12,} B")
    print(f"  R (source document size)  = {raw_size:>12,} B")
    print(f"  T / R                     = {pct:6.2f} %  transferred")
    print(f"  1 - T / R                 = {100 - pct:6.2f} %  egress avoided by lazy tiering")


def main() -> None:
    ap = argparse.ArgumentParser(description="Demo metrics over the consumer's event log.")
    ap.add_argument("--events", default=DEFAULT_EVENTS, help=f"events.jsonl (default: {DEFAULT_EVENTS})")
    ap.add_argument("--config", default=None, help="variables.yml to read policy/peer names from")
    ap.add_argument("--m-cost", metavar="LOCATIONS.json", help="run only the M-COST helper on this listing")
    ap.add_argument("--raw-size", type=int, help="source document size in bytes (with --m-cost)")
    ap.add_argument("--cache-peer", default=None, help="cache peer alias (default: the write policy's cache peer from config)")
    args = ap.parse_args()

    names = load_names(args.config)
    if args.m_cost:
        if not args.raw_size:
            ap.error("--m-cost requires --raw-size")
        peer = args.cache_peer or names["cache_peer"]
        if not peer:
            ap.error("no cache peer on the write policy in variables.yml; pass --cache-peer")
        m_cost(args.m_cost, args.raw_size, peer)
        return

    events = Path(args.events)
    if not events.exists() or events.stat().st_size == 0:
        sys.exit(f"ERROR: events file missing or empty: {events}")
    print_profile(profile_events(str(events)))
    con = duckdb.connect()
    connect_events(con, str(events))
    m_headline(con, names)
    m_pipeline(str(events), names)
    m_scale(con)
    volume(con)


if __name__ == "__main__":
    main()

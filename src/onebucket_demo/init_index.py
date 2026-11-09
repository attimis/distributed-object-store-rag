"""Out-of-band index initialisation (ADR-003).

Creating the LanceDB table is a one-time, operator-run step, deliberately
separate from the writers: the embedder Lambda and the daemons only ever
``merge_insert`` into a table that already exists. That keeps concurrent
writers from racing to create the dataset and makes "the index does not
exist" an explicit setup error rather than something a writer papers over.

Idempotent: re-running against an existing dataset is a no-op, except that
a missing sidecar is recreated so the canary check has something to read.
"""

from __future__ import annotations

from datetime import datetime, timezone

import fastembed

from .constants import BGE_SMALL_MODEL_NAME, SPLADE_MODEL_NAME
from .embed_sparse import compute_canary_hash
from .schema import SCHEMA_VERSION, get_schema
from .storage import (
    InitMetadata,
    TABLE_NAME,
    connect,
    sidecar_exists,
    sidecar_uri,
    write_sidecar,
)

DEFAULT_DATASET_ROOT = "data/metadata"


def main(dataset_root: str = DEFAULT_DATASET_ROOT) -> int:
    db = connect(dataset_root)
    if TABLE_NAME in db.list_tables().tables:
        print(
            f"init_index: table '{TABLE_NAME}' already exists at {dataset_root}; no-op."
        )
        if not sidecar_exists(dataset_root):
            print(
                f"init_index: WARNING — sidecar missing at "
                f"{sidecar_uri(dataset_root)}; recreating from current models."
            )
            _write_init_sidecar(dataset_root)
        return 0
    db.create_table(TABLE_NAME, schema=get_schema(), mode="create")
    _write_init_sidecar(dataset_root)
    print(
        f"init_index: created table '{TABLE_NAME}' and sidecar at {dataset_root}."
    )
    return 0


def _write_init_sidecar(dataset_root: str) -> None:
    metadata: InitMetadata = {
        "fastembed_version": fastembed.__version__,
        "dense_model": BGE_SMALL_MODEL_NAME,
        "sparse_model": SPLADE_MODEL_NAME,
        "sparse_canary_hash": compute_canary_hash(),
        "schema_version": SCHEMA_VERSION,
        "initialized_at": datetime.now(timezone.utc).isoformat(),
    }
    write_sidecar(dataset_root, metadata)

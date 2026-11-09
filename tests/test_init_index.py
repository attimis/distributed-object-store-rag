"""Tests for out-of-band index initialization."""

import json

from onebucket_demo.embed_sparse import compute_canary_hash
from onebucket_demo.init_index import main
from onebucket_demo.storage import TABLE_NAME, connect, sidecar_path


def test_init_creates_dataset_and_sidecar(tmp_path):
    dataset_root = tmp_path / "metadata"
    assert main(str(dataset_root)) == 0
    sidecar = sidecar_path(dataset_root)
    assert sidecar.exists()
    metadata = json.loads(sidecar.read_text())
    for key in (
        "fastembed_version",
        "dense_model",
        "sparse_model",
        "sparse_canary_hash",
        "schema_version",
        "initialized_at",
    ):
        assert key in metadata
    db = connect(dataset_root)
    assert TABLE_NAME in db.list_tables().tables


def test_init_is_idempotent(tmp_path):
    dataset_root = tmp_path / "metadata"
    assert main(str(dataset_root)) == 0
    assert main(str(dataset_root)) == 0
    assert sidecar_path(dataset_root).exists()


def test_sidecar_canary_matches_runtime_canary(tmp_path):
    dataset_root = tmp_path / "metadata"
    main(str(dataset_root))
    metadata = json.loads(sidecar_path(dataset_root).read_text())
    assert metadata["sparse_canary_hash"] == compute_canary_hash()


def test_init_recreates_missing_sidecar(tmp_path):
    """If the table exists but sidecar is missing, second run should restore it."""
    dataset_root = tmp_path / "metadata"
    main(str(dataset_root))
    sp = sidecar_path(dataset_root)
    sp.unlink()
    assert main(str(dataset_root)) == 0
    assert sp.exists()

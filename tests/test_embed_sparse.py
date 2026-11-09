"""Tests for sparse embeddings (SPLADE) and the canary-hash composite."""

import pytest

import onebucket_demo.embed_sparse as mod
from onebucket_demo.embed_sparse import compute_canary_hash, embed_sparse


@pytest.fixture(scope="module")
def texts():
    return ["hello world", "the quick brown fox", "lorem ipsum dolor"]


def test_shape(texts):
    vectors = embed_sparse(texts)
    assert len(vectors) == len(texts)
    for v in vectors:
        assert "indices" in v and "values" in v
        assert len(v["indices"]) == len(v["values"])
        assert all(isinstance(i, int) for i in v["indices"])
        assert all(isinstance(x, float) for x in v["values"])
        assert all(x >= 0 for x in v["values"])


def test_empty_input():
    assert embed_sparse([]) == []


def test_canary_deterministic_within_process():
    a = compute_canary_hash()
    b = compute_canary_hash()
    assert a == b
    assert isinstance(a, str)
    assert len(a) == 64


def test_canary_changes_on_model_name_drift(monkeypatch):
    """Negative test: the model-name component of the canary changes the hash."""
    original = compute_canary_hash()
    monkeypatch.setattr(mod, "SPLADE_MODEL_NAME", "fake/different-splade-model")
    assert compute_canary_hash() != original


def test_canary_changes_on_dense_model_name_drift(monkeypatch):
    """Negative test: pairing dense model is part of the writer-reader contract."""
    original = compute_canary_hash()
    monkeypatch.setattr(mod, "BGE_SMALL_MODEL_NAME", "fake/different-dense-model")
    assert compute_canary_hash() != original


def test_canary_changes_on_fastembed_version_drift(monkeypatch):
    """Negative test: the fastembed-version axis of the canary changes the hash."""
    original = compute_canary_hash()
    monkeypatch.setattr(mod.fastembed, "__version__", "0.0.0-fake")
    assert compute_canary_hash() != original

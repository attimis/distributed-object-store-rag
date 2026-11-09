"""Tests for dense embeddings (BGE-small)."""

import pytest

from onebucket_demo.constants import DENSE_DIM
from onebucket_demo.embed_dense import embed_dense


@pytest.fixture(scope="module")
def texts():
    return ["hello world", "the quick brown fox", "lorem ipsum dolor"]


def test_shape(texts):
    vectors = embed_dense(texts)
    assert len(vectors) == len(texts)
    for v in vectors:
        assert len(v) == DENSE_DIM
        assert all(isinstance(x, float) for x in v)


def test_empty_input():
    assert embed_dense([]) == []


def test_determinism(texts):
    a = embed_dense(texts)
    b = embed_dense(texts)
    for va, vb in zip(a, b):
        assert va == vb

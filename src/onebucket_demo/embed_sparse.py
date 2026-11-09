"""Sparse (SPLADE) embeddings, plus the canary hash that guards model drift.

Hybrid search needs both a dense vector and a sparse, vocabulary-weighted
vector per chunk (see ADR-005). This module produces the sparse one.

It also computes the **canary hash**: a digest of the model names, the
fastembed version and the SPLADE tokenizer. ``init_index`` records it in
the index sidecar; the MCP server and the embedder compare it at startup and
refuse to run if it differs, because an index written by one model version
and queried by another returns silently wrong results.

What the canary catches: a model-name change, a fastembed upgrade, a
tokenizer vocabulary change. What it cannot catch: a silent weights swap
under the same name and version, which is why the Lambda images pin the
model files into the image itself.
"""

from __future__ import annotations

import os
from functools import lru_cache
from hashlib import sha256

import fastembed
from fastembed import SparseTextEmbedding

from .constants import BGE_SMALL_MODEL_NAME, SPLADE_MODEL_NAME

# Match embed_dense._LAMBDA_LOCAL_FILES_ONLY: skip HF Hub network calls when
# running in the Lambda runtime where the SPLADE model is already baked into
# the image. The bake step (RUN python -c "embed_sparse(['warmup']); ...")
# runs at Docker build time without AWS_LAMBDA_FUNCTION_NAME set, so it
# continues to download as needed.
_LAMBDA_LOCAL_FILES_ONLY = "AWS_LAMBDA_FUNCTION_NAME" in os.environ


@lru_cache(maxsize=1)
def _model() -> SparseTextEmbedding:
    return SparseTextEmbedding(
        model_name=SPLADE_MODEL_NAME,
        local_files_only=_LAMBDA_LOCAL_FILES_ONLY,
    )


def embed_sparse(texts: list[str]) -> list[dict]:
    if not texts:
        return []
    out: list[dict] = []
    for emb in _model().embed(texts):
        out.append(
            {
                "indices": [int(i) for i in emb.indices.tolist()],
                "values": [float(v) for v in emb.values.tolist()],
            }
        )
    return out


def _splade_tokenizer_bytes() -> bytes:
    """Best-effort retrieval of SPLADE tokenizer bytes for the canary hash.

    FastEmbed exposes the underlying tokenizers.Tokenizer at varying paths
    across versions. On miss, returns a stable marker; the SPLADE model name
    and FastEmbed version still cover the most common drift cases.
    """
    model = _model()
    tok = getattr(model, "tokenizer", None)
    if tok is None:
        inner = getattr(model, "model", None)
        if inner is not None:
            tok = getattr(inner, "tokenizer", None)
    if tok is not None and hasattr(tok, "to_str"):
        return tok.to_str().encode("utf-8")
    return b"<splade-vocab-unavailable>"


def compute_canary_hash() -> str:
    """Deterministic canary for writer/reader model-version drift detection.

    Composite = SHA256( SPLADE_MODEL_NAME ‖ fastembed.__version__ ‖
                        BGE_SMALL_MODEL_NAME ‖ splade_tokenizer_bytes ).
    """
    h = sha256()
    h.update(SPLADE_MODEL_NAME.encode("utf-8"))
    h.update(b"\x00")
    h.update(fastembed.__version__.encode("utf-8"))
    h.update(b"\x00")
    h.update(BGE_SMALL_MODEL_NAME.encode("utf-8"))
    h.update(b"\x00")
    h.update(_splade_tokenizer_bytes())
    return h.hexdigest()

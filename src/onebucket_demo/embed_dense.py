"""Dense embedding via FastEmbed (BAAI/bge-small-en-v1.5)."""

from __future__ import annotations

import os
from functools import lru_cache

from fastembed import TextEmbedding

from .constants import BGE_SMALL_MODEL_NAME

# When running inside an AWS Lambda execution environment, the model is baked
# into the image and we don't want FastEmbed to do a model_info() + list_repo_tree()
# round-trip to huggingface.co on every invocation — Lambda's egress to HF is
# slow/throttled and was costing ~160s per cold start. Auto-detected via the
# AWS_LAMBDA_FUNCTION_NAME env var (Lambda sets this automatically). Local dev
# and the build-time `RUN python -c "embed_dense(...)"` bake step both leave it
# unset, so they continue to fetch normally.
_LAMBDA_LOCAL_FILES_ONLY = "AWS_LAMBDA_FUNCTION_NAME" in os.environ


@lru_cache(maxsize=1)
def _model() -> TextEmbedding:
    return TextEmbedding(
        model_name=BGE_SMALL_MODEL_NAME,
        local_files_only=_LAMBDA_LOCAL_FILES_ONLY,
    )


def embed_dense(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    return [v.tolist() for v in _model().embed(texts)]

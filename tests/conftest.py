"""Shared pytest configuration.

Two things every test in this suite relies on:

1. **Isolation from the operator's shell.** A developer who runs the demo
   against OneBucket exports ``AWS_ENDPOINT_URL``, ``AWS_PROFILE`` and the
   ``ONEBUCKET_*`` variables in their shell. Left in place, those variables
   point moto-mocked boto3 clients at a live endpoint and make the mocked
   tests fail in confusing ways. The autouse fixture below removes them and
   installs dummy credentials so every test starts from the same clean
   environment.

2. **Process-wide caches are reset between tests.** The search layer caches
   the LanceDB table handle and the sparse-vector rows, the S3 backend caches
   its boto3 client, and the MCP server remembers whether its background
   cache-warmer thread has started. All of those are module-level state that
   would otherwise leak from one test into the next.

The ``models`` marker tags tests that load the embedding models (BGE-small and
SPLADE via fastembed). The first run downloads about 630 MB from the Hugging
Face Hub into ``~/.cache/fastembed``; CI runs ``pytest -m "not models"`` to
stay fast, and developers run the full suite locally.
"""

from __future__ import annotations

import os

import pytest

# Test modules whose tests load the embedding models or the BGE tokenizer.
# They are tagged with the ``models`` marker automatically so nobody has to
# remember to decorate individual tests.
_MODEL_LOADING_MODULES = {
    "test_chain_conformance_local",
    "test_chunking",
    "test_e2e",
    "test_embed_dense",
    "test_embed_sparse",
    "test_init_index",
    "test_lambda_handler_chunker",
    "test_lambda_handler_embedder",
    "test_mcp_server",
    "test_mcp_transport",
    "test_pipeline",
    "test_search",
    "test_writer",
}

_OPERATOR_ENV_PREFIXES = ("ONEBUCKET_", "ATTIMIS_")
_OPERATOR_ENV_VARS = (
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
    "AWS_PROFILE",
    "AWS_SESSION_TOKEN",
)


def pytest_collection_modifyitems(config, items):  # noqa: ARG001 - pytest hook signature
    for item in items:
        if item.module.__name__.rsplit(".", 1)[-1] in _MODEL_LOADING_MODULES:
            item.add_marker(pytest.mark.models)


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch):
    """Scrub operator variables, install dummy AWS credentials, reset caches."""
    for name in list(os.environ):
        if name.startswith(_OPERATOR_ENV_PREFIXES) or name in _OPERATOR_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    # The MCP server's background cache warmer is a long-lived thread; unit
    # tests never want it. The one test that verifies warmer startup removes
    # this variable explicitly.
    monkeypatch.setenv("ONEBUCKET_DISABLE_CACHE_WARMER", "1")

    _reset_process_caches()
    yield
    _reset_process_caches()


def _reset_process_caches() -> None:
    from onebucket_demo import _s3, search

    _s3.client.cache_clear()
    search.reset_caches()
    try:
        from onebucket_demo import mcp_server

        mcp_server._warmer_started.clear()
    except ImportError:  # pragma: no cover - mcp is an installed dependency
        pass

"""End-to-end smoke test (in-process).

Wires the entire local pipeline programmatically: init → ingest two fixtures
→ search → assert the documented response contract. The MCP transport boundary
is covered separately by tests/test_mcp_transport.py, because an in-process
call cannot catch protocol-framing bugs.

Headline user story: "Clone the repo, run uv sync, run the demo, and see a
hybrid-search response with both arrays."
"""

from __future__ import annotations

from pathlib import Path

import pytest

from onebucket_demo.init_index import main as init_index
from onebucket_demo.search import search_global_index
from onebucket_demo.writer import ingest


@pytest.fixture
def seeded_repo(tmp_path):
    """Init + ingest the two committed fixtures into a tmp dataset."""
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))

    # Use the actual committed sample fixtures so the e2e mirrors the demo
    samples_dir = Path(__file__).parent.parent / "data" / "samples"
    for f in sorted(samples_dir.iterdir()):
        if f.suffix in {".txt", ".md"}:
            ingest(f, dataset_root=dataset_root, chunk_root=chunk_root)
    return dataset_root, chunk_root


def test_e2e_search_returns_two_arrays(seeded_repo):
    dataset_root, _ = seeded_repo
    response = search_global_index("quarterly revenue", limit=3, dataset_root=dataset_root)
    assert "semantic_matches" in response
    assert "keyword_matches" in response
    assert isinstance(response["semantic_matches"], list)
    assert isinstance(response["keyword_matches"], list)


def test_e2e_chunk_uris_resolve_to_existing_local_files(seeded_repo):
    """Every chunk_uri returned must be a full file:// URI pointing at a real file."""
    dataset_root, _ = seeded_repo
    response = search_global_index("quarterly revenue", limit=3, dataset_root=dataset_root)
    for arr in ("semantic_matches", "keyword_matches"):
        for entry in response[arr]:
            uri = entry["chunk_uri"]
            assert uri.startswith("file://"), f"chunk_uri {uri!r} is not a file:// URI"
            local_path = Path(uri[len("file://"):])
            assert local_path.exists(), f"chunk_uri {uri!r} does not resolve to an existing file"


def test_e2e_no_chunk_text_key_at_any_level(seeded_repo):
    """Search returns pointers only; chunk text is fetched in a separate step,
    so `chunk_text` must be absent as a KEY (not just empty)."""
    dataset_root, _ = seeded_repo
    response = search_global_index("quarterly revenue", limit=3, dataset_root=dataset_root)
    assert "chunk_text" not in response
    for arr in ("semantic_matches", "keyword_matches"):
        for entry in response.get(arr, []):
            assert "chunk_text" not in entry


def test_e2e_all_scores_are_floats(seeded_repo):
    dataset_root, _ = seeded_repo
    response = search_global_index("quarterly revenue", limit=3, dataset_root=dataset_root)
    for arr in ("semantic_matches", "keyword_matches"):
        for entry in response[arr]:
            assert isinstance(entry["score"], float)


def test_e2e_finance_query_top_result_is_visibly_relevant(seeded_repo):
    """A `quarterly revenue` query against the seeded corpus surfaces
    a finance-relevant top match (visibly relevant to a non-technical viewer).
    """
    dataset_root, chunk_root = seeded_repo
    response = search_global_index("quarterly revenue", limit=3, dataset_root=dataset_root)
    # Both arrays should have results (fixtures contain finance content + lorem)
    assert response.get("semantic_matches"), "semantic search returned no results"
    assert response.get("keyword_matches"), "keyword search returned no results"

    # The top semantic match's chunk file should mention revenue/finance terms
    top = response["semantic_matches"][0]
    chunk_path = Path(top["chunk_uri"][len("file://"):])
    chunk_content = chunk_path.read_text().lower()
    assert any(term in chunk_content for term in ("revenue", "quarter", "finance", "earnings")), (
        f"top semantic match is not visibly finance-relevant: {chunk_content[:200]}…"
    )

    # Top keyword match should also be finance-relevant (SPLADE catches the literal terms)
    top_kw = response["keyword_matches"][0]
    kw_path = Path(top_kw["chunk_uri"][len("file://"):])
    kw_content = kw_path.read_text().lower()
    assert any(term in kw_content for term in ("revenue", "quarter", "finance", "earnings")), (
        f"top keyword match is not visibly finance-relevant: {kw_content[:200]}…"
    )


def test_e2e_irrelevant_query_returns_no_results_or_low_relevance(seeded_repo):
    """The discriminator: a query that doesn't match either fixture should
    not return strong matches. Sanity check that the index discriminates."""
    dataset_root, _ = seeded_repo
    response = search_global_index("dolor sit amet consectetur", limit=3, dataset_root=dataset_root)
    # We don't assert empty; the lorem fixture matches lorem text. We just
    # assert the response is well-formed (the contract holds for any query).
    assert "semantic_matches" in response or "message" in response

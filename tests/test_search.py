"""Tests for the search contract layer: response shape, scoping, caching."""

import json

import pytest

from onebucket_demo.errors import CanaryMismatchError
from onebucket_demo.init_index import main as init_index
from onebucket_demo.search import assert_canary, search_global_index
from onebucket_demo.storage import sidecar_path
from onebucket_demo.writer import ingest


@pytest.fixture
def seeded(tmp_path):
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    fixture = tmp_path / "doc.md"
    fixture.write_text(
        "Quarterly revenue grew 12 percent year over year. "
        "The finance team reported strong segment performance and "
        "issued forward guidance for the next fiscal period. " * 30,
        encoding="utf-8",
    )
    ingest(fixture, dataset_root=dataset_root, chunk_root=chunk_root)
    return dataset_root


def test_assert_canary_passes_for_fresh_init(tmp_path):
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    assert_canary(dataset_root)


def test_assert_canary_raises_on_mismatch(tmp_path):
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    sp = sidecar_path(dataset_root)
    meta = json.loads(sp.read_text())
    meta["sparse_canary_hash"] = "0" * 64
    sp.write_text(json.dumps(meta))
    with pytest.raises(CanaryMismatchError):
        assert_canary(dataset_root)


def test_search_returns_two_arrays(seeded):
    response = search_global_index("quarterly revenue", limit=3, dataset_root=seeded)
    assert "semantic_matches" in response
    assert "keyword_matches" in response
    assert isinstance(response["semantic_matches"], list)
    assert isinstance(response["keyword_matches"], list)


def test_search_response_has_no_chunk_text_key_anywhere(seeded):
    """Search returns pointers only; chunk text is fetched in a separate step."""
    response = search_global_index("revenue", limit=3, dataset_root=seeded)
    assert "chunk_text" not in response
    for arr in ("semantic_matches", "keyword_matches"):
        for entry in response.get(arr, []):
            assert "chunk_text" not in entry


def test_search_entry_keys_match_contract(seeded):
    response = search_global_index("revenue", limit=3, dataset_root=seeded)
    expected_keys = {
        "chunk_uri",
        "source_uri",
        "score",
        # raw_file_id lets an agent build a `scope` for a follow-up search;
        # chunk_index is for citations.
        "raw_file_id",
        "chunk_index",
    }
    for arr in ("semantic_matches", "keyword_matches"):
        for entry in response[arr]:
            assert set(entry.keys()) == expected_keys
            assert isinstance(entry["score"], float)
            assert isinstance(entry["chunk_uri"], str)
            assert isinstance(entry["source_uri"], str)
            assert isinstance(entry["raw_file_id"], str)
            assert isinstance(entry["chunk_index"], int)


def test_search_clamps_limit_to_max(seeded):
    response = search_global_index("revenue", limit=999, dataset_root=seeded)
    assert "semantic_matches" in response
    assert len(response["semantic_matches"]) <= 30


def test_search_empty_scope_is_treated_as_no_filter(seeded):
    """Empty scope list should be equivalent to scope=None (whole-corpus search)."""
    no_scope = search_global_index("revenue", limit=3, dataset_root=seeded)
    empty_scope = search_global_index(
        "revenue", limit=3, dataset_root=seeded, scope=[]
    )
    # Same number of hits in each — if empty-scope had been treated as
    # "filter to no docs", semantic_matches would be empty.
    assert len(no_scope.get("semantic_matches", [])) == len(
        empty_scope.get("semantic_matches", [])
    )


def test_search_scope_with_unknown_id_returns_empty(seeded):
    """A scope listing only nonexistent raw_file_ids should return no hits."""
    response = search_global_index(
        "revenue",
        limit=3,
        dataset_root=seeded,
        scope=["0000000000000000"],  # 16 hex zeros — won't match any real doc hash
    )
    # Either empty arrays or the "no relevant documents" message envelope.
    if "message" in response:
        assert "No relevant documents" in response["message"]
    else:
        assert response.get("semantic_matches", []) == []
        assert response.get("keyword_matches", []) == []


def test_search_clamps_limit_to_min(seeded):
    response = search_global_index("revenue", limit=0, dataset_root=seeded)
    assert "semantic_matches" in response


def test_search_empty_dataset_returns_no_results_message(tmp_path):
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    response = search_global_index("anything", dataset_root=dataset_root)
    assert response["message"] == (
        "No relevant documents found for this query in the global index."
    )
    assert "semantic_matches" not in response
    assert "keyword_matches" not in response


def test_search_reports_internal_timing(seeded):
    """Every response carries the time spent inside the tool, split by phase,
    so a demo can quote storage-side latency rather than MCP round-trip time."""
    response = search_global_index("revenue", limit=3, dataset_root=seeded)
    timing = response["_timing_ms"]
    assert set(timing) == {"embed_query", "dense_search", "sparse_search", "total_internal"}
    assert all(isinstance(v, int) and v >= 0 for v in timing.values())
    assert timing["total_internal"] >= max(
        timing["embed_query"], timing["dense_search"], timing["sparse_search"]
    )


def test_search_sees_rows_committed_after_first_query(tmp_path):
    """The search layer caches the table handle and the sparse rows, so a
    document ingested after the first query must still show up: the caches
    key on the table version, which changes on every commit."""
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    first = tmp_path / "first.md"
    first.write_text("Alpha report on quarterly revenue and segment margins. " * 20)
    ingest(first, dataset_root=dataset_root, chunk_root=chunk_root)
    before = search_global_index("zebra habitat", limit=5, dataset_root=dataset_root)
    assert all("zebra" not in h["source_uri"] for h in before.get("keyword_matches", []))

    second = tmp_path / "zebra.md"
    second.write_text("Zebra habitat survey: grassland zebra populations and migration. " * 20)
    ingest(second, dataset_root=dataset_root, chunk_root=chunk_root)
    after = search_global_index("zebra habitat", limit=5, dataset_root=dataset_root)
    assert any(h["source_uri"].endswith("zebra.md") for h in after["keyword_matches"])


def test_search_drops_rows_after_same_size_replacement(tmp_path):
    """Delete a document and ingest a different one with the same chunk count.
    Row count is unchanged, so a cache keyed on row count would serve the
    deleted document's tokens; keying on the table version must not."""
    from onebucket_demo import search as search_module
    from onebucket_demo.storage import TABLE_NAME, connect

    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    doc_a = tmp_path / "a.md"
    doc_a.write_text("Short note about penguins and antarctic ice shelves.")
    result_a = ingest(doc_a, dataset_root=dataset_root, chunk_root=chunk_root)
    assert result_a.chunks_written == 1
    hits = search_global_index("penguins", limit=3, dataset_root=dataset_root)
    assert hits["keyword_matches"][0]["raw_file_id"] == result_a.raw_file_id

    # Out-of-process style mutation: delete directly through LanceDB, no
    # in-process cache eviction, then ingest a same-size document.
    table = connect(dataset_root).open_table(TABLE_NAME)
    table.delete(f"raw_file_id == '{result_a.raw_file_id}'")
    doc_b = tmp_path / "b.md"
    doc_b.write_text("Short note about volcanoes and basalt lava flows.")
    result_b = ingest(doc_b, dataset_root=dataset_root, chunk_root=chunk_root)
    assert result_b.chunks_written == 1

    penguins = search_global_index("penguins", limit=3, dataset_root=dataset_root)
    ids = {h["raw_file_id"] for h in penguins.get("keyword_matches", [])}
    assert result_a.raw_file_id not in ids, "stale rows served after replacement"
    volcanoes = search_global_index("volcanoes", limit=3, dataset_root=dataset_root)
    assert volcanoes["keyword_matches"][0]["raw_file_id"] == result_b.raw_file_id
    assert search_module._sparse_rows_cache, "rows should be cached by version"


def test_scope_clause_validates_ids():
    from onebucket_demo.search import _scope_clause

    assert _scope_clause(None) is None
    assert _scope_clause([]) is None
    assert _scope_clause(["abc123", "DEF456"]) == "raw_file_id IN ('abc123', 'DEF456')"
    # Anything that is not a plain alphanumeric id is dropped, never quoted.
    assert _scope_clause(["x' OR 1=1 --", ""]) is None
    assert _scope_clause(["ok", "bad id"]) == "raw_file_id IN ('ok')"


def test_sparse_scope_filters_across_two_documents(tmp_path):
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    docs = {}
    for name, text in (
        ("finance.md", "Quarterly revenue, margins and guidance for the finance team. " * 15),
        ("wildlife.md", "Revenue from wildlife park tickets, elephant and giraffe exhibits. " * 15),
    ):
        p = tmp_path / name
        p.write_text(text)
        docs[name] = ingest(p, dataset_root=dataset_root, chunk_root=chunk_root).raw_file_id
    unscoped = search_global_index("revenue", limit=10, dataset_root=dataset_root)
    assert {h["raw_file_id"] for h in unscoped["keyword_matches"]} == set(docs.values())
    scoped = search_global_index(
        "revenue", limit=10, dataset_root=dataset_root, scope=[docs["wildlife.md"]]
    )
    for arr in ("semantic_matches", "keyword_matches"):
        assert scoped[arr], f"{arr} should not be empty when the scope has matches"
        assert {h["raw_file_id"] for h in scoped[arr]} == {docs["wildlife.md"]}


def test_search_missing_dataset_returns_connection_failed(tmp_path):
    response = search_global_index("anything", dataset_root=tmp_path / "nonexistent")
    assert response.get("error") == "CONNECTION_FAILED"
    assert "message" in response

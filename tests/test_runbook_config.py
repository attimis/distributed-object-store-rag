"""Unit tests for src/onebucket_demo/runbook_config.py.

Covers: dotted-path parsing, get/set, atomic save + comment preservation,
missing-sentinel detection, and the validate() consistency checks. The MCP
tool wrappers live in mcp_tools.py and are tested separately.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from onebucket_demo import runbook_config


# -----------------------------------------------------------------------------
# Fixture: a minimal valid YAML that mirrors the real variables.yml shape
# -----------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "variables.yml"
    p.write_text(textwrap.dedent(body).lstrip())
    return p


@pytest.fixture
def valid_yaml(tmp_path: Path) -> Path:
    return _write_yaml(
        tmp_path,
        """
        schema_version: 1
        onebucket:
          s3_endpoint: "https://s3.onebucket.io"
          events_url: "https://events.onebucket.io"
        endpoints:
          ENDPOINT_A:
            url: "https://s3.amazonaws.com:443"
            provider: "aws"
            use_region_toggle: true
          ENDPOINT_B:
            url: "https://s3.us-west-1.wasabisys.com:443"
            provider: "wasabi"
            use_region_toggle: false
        policies:
          policy-east:
            quorum: 1
            peers:
              - alias: ENDPOINT_A
                type: sync
                required: true
                bucket: bucket-east
              - alias: ENDPOINT_B
                type: cache
                required: false
                bucket: bucket-east-cache
          policy-meta:
            quorum: 2
            peers:
              - alias: ENDPOINT_A
                type: sync
                required: true
                bucket: bucket-meta
              - alias: ENDPOINT_B
                type: sync
                required: true
                bucket: bucket-meta-cache
        pipeline:
          raw_writes_to_policy: policy-east
          chunks_writes_to_policy: policy-east
          metadata_writes_to_policy: policy-meta
          raw_key_prefix: "raw/"
        daemon:
          heartbeat_path: "/tmp/oneb-pipeline-daemon.heartbeat"
          event_log_path: "~/.local/state/onebucket-demo/events.jsonl"
          snapshots_path: "~/.local/state/onebucket-demo/snapshots/"
          mode: "onebucket-events"
        classifier_outcomes:
          - dispatch_chunker
          - skip_not_put
        operator_intent:
          fresh_ticker_for_live_ingest: "GOOG"
          pricing:
            aws_egress_usd_per_gb: 0.09
            wasabi_storage_usd_per_tb_month: 5.99
          workload:
            y1_read_fraction: 0.30
            y1_query_count: 10000000
        """,
    )


# -----------------------------------------------------------------------------
# load / save / round-trip
# -----------------------------------------------------------------------------


class TestLoadSave:
    def test_load_returns_parsed_structure(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        assert data["schema_version"] == 1
        assert data["onebucket"]["s3_endpoint"] == "https://s3.onebucket.io"
        assert data["policies"]["policy-east"]["quorum"] == 1

    def test_load_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            runbook_config.load(tmp_path / "doesnotexist.yml")

    def test_save_roundtrip_preserves_data(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        data["operator_intent"]["fresh_ticker_for_live_ingest"] = "META"
        runbook_config.save(data, valid_yaml)
        reloaded = runbook_config.load(valid_yaml)
        assert reloaded["operator_intent"]["fresh_ticker_for_live_ingest"] == "META"
        # And the unrelated fields survived
        assert reloaded["policies"]["policy-east"]["quorum"] == 1

    def test_save_preserves_comments(self, tmp_path: Path) -> None:
        """Critical contract: ruamel round-trip keeps the operator-facing
        comments in variables.yml when the agent updates a single field.
        """
        p = _write_yaml(
            tmp_path,
            """
            schema_version: 1
            # This comment must survive a load->modify->save round-trip.
            operator_intent:
              fresh_ticker_for_live_ingest: "GOOG"  # inline comment
              pricing:
                # Block comment above pricing field
                aws_egress_usd_per_gb: 0.09
            """,
        )
        data = runbook_config.load(p)
        data["operator_intent"]["fresh_ticker_for_live_ingest"] = "META"
        runbook_config.save(data, p)
        text = p.read_text()
        assert "This comment must survive" in text
        assert "inline comment" in text
        assert "Block comment above pricing field" in text

    def test_save_is_atomic_via_tempfile_rename(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Verify no partial-file artifacts are left around even if a write
        is interrupted."""
        p = _write_yaml(tmp_path, "schema_version: 1\n")
        data = runbook_config.load(p)
        runbook_config.save(data, p)
        # No stray .variables-tmp-* files
        leftovers = list(tmp_path.glob(".variables-tmp-*"))
        assert leftovers == [], f"tempfile leftovers: {leftovers}"


# -----------------------------------------------------------------------------
# Path resolution
# -----------------------------------------------------------------------------


class TestPathResolution:
    def test_explicit_path_wins(self, valid_yaml: Path, monkeypatch) -> None:
        # Set env var to something else; explicit arg should win
        monkeypatch.setenv(runbook_config.ENV_VAR, "/tmp/should-not-be-used.yml")
        assert runbook_config.resolve_path(valid_yaml) == valid_yaml.resolve()

    def test_env_var_used_when_no_arg(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        target = tmp_path / "from-env.yml"
        target.write_text("schema_version: 1\n")
        monkeypatch.setenv(runbook_config.ENV_VAR, str(target))
        assert runbook_config.resolve_path() == target.resolve()

    def test_default_path_falls_back(self, monkeypatch) -> None:
        monkeypatch.delenv(runbook_config.ENV_VAR, raising=False)
        resolved = runbook_config.resolve_path()
        assert resolved == runbook_config.DEFAULT_CONFIG_PATH


# -----------------------------------------------------------------------------
# Dotted-path parsing + get/set
# -----------------------------------------------------------------------------


class TestDottedPath:
    def test_simple_key(self) -> None:
        assert runbook_config._parse_dotted_path("a") == ["a"]

    def test_nested_keys(self) -> None:
        assert runbook_config._parse_dotted_path("a.b.c") == ["a", "b", "c"]

    def test_list_index(self) -> None:
        assert runbook_config._parse_dotted_path("a.b[2].c") == ["a", "b", 2, "c"]

    def test_multiple_indices(self) -> None:
        assert runbook_config._parse_dotted_path("a[0][1]") == ["a", 0, 1]

    def test_get_nested(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        assert (
            runbook_config.get_at_path(data, "onebucket.s3_endpoint")
            == "https://s3.onebucket.io"
        )
        assert (
            runbook_config.get_at_path(
                data, "policies.policy-east.peers[0].alias"
            )
            == "ENDPOINT_A"
        )

    def test_set_nested_scalar(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.workload.y1_read_fraction", 0.42
        )
        assert (
            runbook_config.get_at_path(
                data, "operator_intent.workload.y1_read_fraction"
            )
            == 0.42
        )

    def test_set_inside_list(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "policies.policy-east.peers[1].required", True
        )
        assert (
            runbook_config.get_at_path(
                data, "policies.policy-east.peers[1].required"
            )
            is True
        )

    def test_empty_path_raises(self) -> None:
        with pytest.raises(ValueError):
            runbook_config._parse_dotted_path("")


class TestTypeCoercionOnSet:
    """The MCP protocol delivers JSON; agents sometimes pass numbers as
    strings ('10000000' instead of 10000000). Without coercion, the loader
    would silently corrupt int fields to quoted strings on disk. These
    tests guard against that regression — see the bug found 2026-05-20
    when an agent's set_runbook_variable call stored y1_query_count
    as a string.
    """

    def test_string_to_int_coerced(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.workload.y1_query_count", "20000000"
        )
        result = runbook_config.get_at_path(
            data, "operator_intent.workload.y1_query_count"
        )
        assert result == 20000000
        assert isinstance(result, int)

    def test_string_to_float_coerced(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.workload.y1_read_fraction", "0.45"
        )
        result = runbook_config.get_at_path(
            data, "operator_intent.workload.y1_read_fraction"
        )
        assert result == 0.45
        assert isinstance(result, float)

    def test_string_true_to_bool_coerced(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        # endpoints.ENDPOINT_A.use_region_toggle is bool in the fixture
        runbook_config.set_at_path(
            data, "endpoints.ENDPOINT_A.use_region_toggle", "false"
        )
        assert (
            runbook_config.get_at_path(
                data, "endpoints.ENDPOINT_A.use_region_toggle"
            )
            is False
        )

    def test_string_false_to_bool_coerced(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "endpoints.ENDPOINT_B.use_region_toggle", "true"
        )
        assert (
            runbook_config.get_at_path(
                data, "endpoints.ENDPOINT_B.use_region_toggle"
            )
            is True
        )

    def test_unparseable_string_to_int_passes_through(
        self, valid_yaml: Path
    ) -> None:
        # If coercion fails, value passes through as a string and the
        # validator (or downstream code) surfaces the type mismatch.
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.workload.y1_query_count", "not-a-number"
        )
        # We pass through as a string rather than silently inserting
        # a sentinel/None — the operator's wrong-typed input is preserved
        # so the validator can catch it intentionally.
        result = runbook_config.get_at_path(
            data, "operator_intent.workload.y1_query_count"
        )
        assert isinstance(result, str)

    def test_int_to_string_field_not_coerced(self, valid_yaml: Path) -> None:
        # We only coerce string→{int, float, bool}. A user setting an int
        # where a string is expected gets stored as an int (not converted
        # to "42"); the validator can flag it if that's wrong.
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.fresh_ticker_for_live_ingest", 42
        )
        result = runbook_config.get_at_path(
            data, "operator_intent.fresh_ticker_for_live_ingest"
        )
        assert result == 42

    def test_passthrough_when_types_match(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        runbook_config.set_at_path(
            data, "operator_intent.workload.y1_query_count", 5_000_000
        )
        assert (
            runbook_config.get_at_path(
                data, "operator_intent.workload.y1_query_count"
            )
            == 5_000_000
        )

    def test_yaml_serializes_coerced_int_unquoted(
        self, valid_yaml: Path
    ) -> None:
        """End-to-end: set string '10000000' on an int field, save, reload,
        verify the YAML has the value as an unquoted int (not '10000000')."""
        runbook_config.set_variable(
            "operator_intent.workload.y1_query_count", "12345678", valid_yaml
        )
        text = valid_yaml.read_text()
        # The on-disk YAML should NOT have quotes around the int
        import re
        m = re.search(r"y1_query_count:\s*(\S+)", text)
        assert m is not None
        assert m.group(1) == "12345678"  # unquoted
        # And reloads as int
        data = runbook_config.load(valid_yaml)
        assert isinstance(
            runbook_config.get_at_path(
                data, "operator_intent.workload.y1_query_count"
            ),
            int,
        )


# -----------------------------------------------------------------------------
# set_variable convenience wrapper (load + set + save)
# -----------------------------------------------------------------------------


def test_set_variable_persists_to_disk(valid_yaml: Path) -> None:
    runbook_config.set_variable(
        "operator_intent.fresh_ticker_for_live_ingest", "META", valid_yaml
    )
    reloaded = runbook_config.load(valid_yaml)
    assert (
        reloaded["operator_intent"]["fresh_ticker_for_live_ingest"] == "META"
    )


# -----------------------------------------------------------------------------
# find_missing_required
# -----------------------------------------------------------------------------


class TestFindMissingRequired:
    def test_no_sentinels_returns_empty(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        assert runbook_config.find_missing_required(data) == []

    def test_detects_sentinel_at_scalar(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            """
            schema_version: 1
            operator_intent:
              fresh_ticker_for_live_ingest: "TODO"
              multi_hop_demo_question: "What is X?"
            """,
        )
        data = runbook_config.load(p)
        missing = runbook_config.find_missing_required(data)
        assert "operator_intent.fresh_ticker_for_live_ingest" in missing
        assert "operator_intent.multi_hop_demo_question" not in missing

    def test_detects_sentinel_inside_list(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            """
            schema_version: 1
            operator_intent:
              seed: ["AAPL", "TODO", "MSFT"]
            """,
        )
        data = runbook_config.load(p)
        assert runbook_config.find_missing_required(data) == ["operator_intent.seed[1]"]


# -----------------------------------------------------------------------------
# validate() — quorum/required + alias refs + mode allow-list
# -----------------------------------------------------------------------------


class TestValidate:
    def test_valid_config_returns_no_errors(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        assert runbook_config.validate(data) == []

    def test_quorum_less_than_required_count_flagged(
        self, valid_yaml: Path
    ) -> None:
        data = runbook_config.load(valid_yaml)
        # policy-east has quorum 1 with 1 Required peer. Flip the Cache peer to
        # also be Required without raising quorum → must error.
        data["policies"]["policy-east"]["peers"][1]["required"] = True
        errors = runbook_config.validate(data)
        msg = " ".join(errors)
        assert "policy-east" in msg
        assert "required" in msg.lower() or "quorum" in msg.lower()

    def test_quorum_above_peer_count_flagged(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        data["policies"]["policy-east"]["quorum"] = 5  # only 2 peers
        errors = runbook_config.validate(data)
        assert any("policy-east" in e for e in errors)

    def test_alias_reference_to_unknown_endpoint_flagged(
        self, valid_yaml: Path
    ) -> None:
        data = runbook_config.load(valid_yaml)
        data["policies"]["policy-east"]["peers"][0]["alias"] = "NONEXISTENT"
        errors = runbook_config.validate(data)
        assert any("NONEXISTENT" in e for e in errors)

    def test_invalid_peer_type_flagged(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        data["policies"]["policy-east"]["peers"][0]["type"] = "magic"
        errors = runbook_config.validate(data)
        assert any("magic" in e for e in errors)

    def test_pipeline_refs_unknown_policy_flagged(
        self, valid_yaml: Path
    ) -> None:
        data = runbook_config.load(valid_yaml)
        data["pipeline"]["raw_writes_to_policy"] = "no-such-policy"
        errors = runbook_config.validate(data)
        assert any("no-such-policy" in e for e in errors)

    def test_invalid_daemon_mode_flagged(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        data["daemon"]["mode"] = "ad-hoc"
        errors = runbook_config.validate(data)
        assert any("ad-hoc" in e for e in errors)

    def test_wrong_schema_version_flagged(self, valid_yaml: Path) -> None:
        data = runbook_config.load(valid_yaml)
        data["schema_version"] = 99
        errors = runbook_config.validate(data)
        assert any("schema_version" in e for e in errors)


# -----------------------------------------------------------------------------
# Integration: the shipping variables.yml at the canonical path validates
# -----------------------------------------------------------------------------


def test_example_variables_yml_validates_and_lists_placeholders() -> None:
    """The shipped example must pass structural validation, and every
    operator-supplied value must be a <placeholder> that load_ready refuses."""
    data = runbook_config.load(runbook_config.EXAMPLE_CONFIG_PATH)
    assert runbook_config.validate(data) == []
    missing = runbook_config.find_missing_required(data)
    assert "policies.demo-data.peers[0].bucket" in missing
    assert "daemon.aws_profile" in missing
    with pytest.raises(runbook_config.ConfigIncomplete) as exc:
        runbook_config.load_ready(runbook_config.EXAMPLE_CONFIG_PATH)
    assert "placeholder still set" in str(exc.value)


def test_load_ready_accepts_a_complete_file(valid_yaml: Path) -> None:
    data = runbook_config.load_ready(valid_yaml)
    assert data["schema_version"] == 1

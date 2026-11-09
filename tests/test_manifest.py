"""Manifest schema tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from onebucket_demo.manifest import (
    CURRENT_SCHEMA_VERSION,
    MANIFEST_FILENAME,
    Manifest,
    UnsupportedManifestVersion,
    read,
    write,
)

REFERENCE_FIXTURE = Path(__file__).parent / "fixtures" / "manifest_reference.json"


def test_reference_fixture_loads():
    data = json.loads(REFERENCE_FIXTURE.read_text())
    manifest = Manifest.from_dict(data)
    assert manifest.schema_version == CURRENT_SCHEMA_VERSION
    assert manifest.raw_file_id == "83dc3a3053c8a509"
    assert manifest.n_chunks == 2


def test_round_trip_serialization():
    m = Manifest(
        raw_file_id="abc123",
        source_uri="file:///tmp/foo.txt",
        n_chunks=5,
        raw_object_key="raw/foo.txt",
        created_at="2026-05-05T00:00:00+00:00",
    )
    payload = m.to_json()
    restored = Manifest.from_json(payload)
    assert restored == m


def test_unknown_schema_version_rejected():
    bad = {
        "schema_version": 999,
        "raw_file_id": "x",
        "source_uri": "x",
        "n_chunks": 0,
        "raw_object_key": "x",
        "created_at": "2026-05-05T00:00:00+00:00",
    }
    with pytest.raises(UnsupportedManifestVersion):
        Manifest.from_dict(bad)


def test_missing_schema_version_rejected():
    bad = {
        "raw_file_id": "x",
        "source_uri": "x",
        "n_chunks": 0,
        "raw_object_key": "x",
        "created_at": "2026-05-05T00:00:00+00:00",
    }
    with pytest.raises(UnsupportedManifestVersion):
        Manifest.from_dict(bad)


def test_write_and_read_round_trip_local(tmp_path):
    m = Manifest(
        raw_file_id="deadbeef",
        source_uri="file:///tmp/x.md",
        n_chunks=3,
        raw_object_key="raw/x.md",
        created_at="2026-05-05T00:00:00+00:00",
    )
    uri = str(tmp_path / "MANIFEST.json")
    write(uri, m)
    assert read(uri) == m


def test_manifest_filename_is_canonical():
    assert MANIFEST_FILENAME == "MANIFEST.json"

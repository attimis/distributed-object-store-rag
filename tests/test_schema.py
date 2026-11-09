"""Tests for the Lance dataset schema per ADR-004."""

import pyarrow as pa

from onebucket_demo.constants import DENSE_DIM
from onebucket_demo.schema import SCHEMA_VERSION, get_schema


def test_schema_field_set():
    schema = get_schema()
    field_names = {f.name for f in schema}
    assert field_names == {
        "id",
        "raw_file_id",
        "chunk_index",
        "chunk_uri",
        "source_uri",
        "vector",
        "sparse_vector",
        "created_at",
    }


def test_dense_vector_is_fixed_size_list():
    schema = get_schema()
    field = schema.field("vector")
    assert pa.types.is_fixed_size_list(field.type)
    assert field.type.list_size == DENSE_DIM
    assert field.type.value_type == pa.float32()


def test_sparse_vector_is_struct_with_indices_and_values():
    schema = get_schema()
    field = schema.field("sparse_vector")
    assert pa.types.is_struct(field.type)
    sub = {sf.name: sf.type for sf in field.type}
    assert set(sub.keys()) == {"indices", "values"}
    assert pa.types.is_list(sub["indices"]) and sub["indices"].value_type == pa.int32()
    assert pa.types.is_list(sub["values"]) and sub["values"].value_type == pa.float32()


def test_chunk_index_is_int32():
    schema = get_schema()
    assert schema.field("chunk_index").type == pa.int32()


def test_created_at_is_utc_timestamp_us():
    schema = get_schema()
    t = schema.field("created_at").type
    assert pa.types.is_timestamp(t)
    assert t.unit == "us"
    assert t.tz == "UTC"


def test_schema_version_pinned():
    assert SCHEMA_VERSION == "1"

"""Tests for ingestion gates per ADR-006."""

from pathlib import Path

from onebucket_demo.gates import check_size, check_type


def _write(tmp_path: Path, name: str, content: str = "") -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


def test_size_under_limit_accepted(tmp_path):
    p = _write(tmp_path, "small.txt", "hello")
    ok, reason = check_size(p)
    assert ok and reason is None


def test_size_at_exact_limit_accepted(tmp_path):
    p = tmp_path / "exact.txt"
    p.write_bytes(b"x" * (10 * 1024 * 1024))
    ok, reason = check_size(p)
    assert ok and reason is None


def test_size_over_limit_rejected(tmp_path):
    p = tmp_path / "big.txt"
    p.write_bytes(b"x" * (10 * 1024 * 1024 + 1))
    ok, reason = check_size(p)
    assert not ok
    assert "10 MB" in reason


def test_type_txt_and_md_accepted(tmp_path):
    assert check_type(_write(tmp_path, "a.txt"))[0]
    assert check_type(_write(tmp_path, "a.md"))[0]


def test_type_pdf_rejected_with_specific_message(tmp_path):
    ok, reason = check_type(_write(tmp_path, "report.pdf"))
    assert not ok
    assert "PDF" in reason


def test_type_unknown_rejected_without_pdf_pointer(tmp_path):
    ok, reason = check_type(_write(tmp_path, "data.csv"))
    assert not ok
    assert "Supported types: .txt, .md" in reason
    assert "PDF support" not in reason

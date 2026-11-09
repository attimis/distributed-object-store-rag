"""Tests for the recursive-cascade chunker per ADR-009."""

from onebucket_demo.chunking import chunk_text
from onebucket_demo.constants import CHUNK_TOKEN_OVERLAP


def test_empty_input_zero_chunks():
    assert chunk_text("") == []
    assert chunk_text("   \n\n   ") == []


def test_short_input_single_chunk():
    text = "Hello world. This is a short fixture."
    chunks = chunk_text(text)
    assert len(chunks) == 1
    c = chunks[0]
    assert c.chunk_text == text
    assert c.chunk_index == 0
    assert c.char_offset_start == 0
    assert c.char_offset_end == len(text)


def test_raw_file_id_is_deterministic():
    a = chunk_text("Some sample text for hashing.")
    b = chunk_text("Some sample text for hashing.")
    assert a[0].raw_file_id == b[0].raw_file_id
    c = chunk_text("Different content")
    assert c[0].raw_file_id != a[0].raw_file_id


def test_long_paragraphs_produce_multiple_chunks():
    paragraph = ("This is a sentence. " * 60).strip()
    text = "\n\n".join([paragraph] * 5)
    chunks = chunk_text(text)
    assert len(chunks) > 1
    for c in chunks:
        assert c.chunk_text == text[c.char_offset_start : c.char_offset_end]


def test_chunk_indices_are_sequential():
    paragraph = ("This is a sentence. " * 60).strip()
    text = "\n\n".join([paragraph] * 5)
    chunks = chunk_text(text)
    assert [c.chunk_index for c in chunks] == list(range(len(chunks)))


def test_cascade_level_2_line_breaks():
    line = "This is a single line of text. " * 30
    text = "\n".join([line] * 5)
    assert "\n\n" not in text
    chunks = chunk_text(text)
    assert len(chunks) >= 1
    for c in chunks:
        assert c.chunk_text == text[c.char_offset_start : c.char_offset_end]


def test_cascade_level_3_sentences():
    text = ". ".join(f"Sentence number {i}" for i in range(200)) + "."
    assert "\n" not in text
    chunks = chunk_text(text)
    assert len(chunks) >= 1
    for c in chunks:
        assert c.chunk_text == text[c.char_offset_start : c.char_offset_end]


def test_cascade_level_4_word_boundary():
    text = " ".join(f"word{i}" for i in range(2000))
    assert "\n" not in text
    assert "." not in text
    chunks = chunk_text(text)
    assert len(chunks) >= 1
    for c in chunks:
        assert c.chunk_text == text[c.char_offset_start : c.char_offset_end]


def test_consecutive_chunks_overlap_in_source():
    """Pack-time overlap walks back N tokens from the just-closed chunk's end,
    so the next chunk's start sits inside the previous chunk's range."""
    paragraph = ("This is a sentence. " * 60).strip()
    text = "\n\n".join([paragraph] * 5)
    chunks = chunk_text(text)
    assert len(chunks) >= 2, "need at least 2 chunks to verify overlap"
    for prev, nxt in zip(chunks, chunks[1:]):
        assert prev.char_offset_start <= nxt.char_offset_start <= prev.char_offset_end


def test_overlap_respects_target_size():
    """The CHUNK_TOKEN_OVERLAP carries strictly fewer tokens than CHUNK_TOKEN_TARGET."""
    assert CHUNK_TOKEN_OVERLAP < 512

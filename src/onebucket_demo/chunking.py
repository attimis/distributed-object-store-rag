"""Recursive cascade chunker per ADR-009.

Cascade order, kicked in only when the previous level produced an over-budget
piece: paragraph (\\n\\n) → line (\\n) → sentence ([.!?]\\s+) → word (whitespace).

Pieces are packed into chunks targeting CHUNK_TOKEN_TARGET tokens (BGE-small
tokenizer for budgeting); each chunk after the first prepends CHUNK_TOKEN_OVERLAP
tokens of suffix from the just-closed chunk for boundary continuity.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from hashlib import sha256

from tokenizers import Tokenizer

from .constants import (
    BGE_SMALL_MODEL_NAME,
    CHUNK_TOKEN_OVERLAP,
    CHUNK_TOKEN_TARGET,
)

logger = logging.getLogger(__name__)

_SENTENCE_BOUNDARY = re.compile(r"[.!?][ \t\n]+")


@dataclass(frozen=True)
class Chunk:
    raw_file_id: str
    chunk_index: int
    chunk_text: str
    char_offset_start: int
    char_offset_end: int


@lru_cache(maxsize=1)
def _tokenizer() -> Tokenizer:
    return Tokenizer.from_pretrained(BGE_SMALL_MODEL_NAME)


def _count_tokens(text: str) -> int:
    return len(_tokenizer().encode(text, add_special_tokens=False).ids)


def _split_on_delim(text: str, delim: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = 0
    while True:
        idx = text.find(delim, cursor)
        if idx == -1:
            break
        if idx > cursor:
            spans.append((cursor, idx))
        cursor = idx + len(delim)
    if cursor < len(text):
        spans.append((cursor, len(text)))
    return spans


def _split_paragraphs(text: str) -> list[tuple[int, int]]:
    return _split_on_delim(text, "\n\n")


def _split_lines(text: str) -> list[tuple[int, int]]:
    return _split_on_delim(text, "\n")


def _split_sentences(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = 0
    for m in _SENTENCE_BOUNDARY.finditer(text):
        end = m.start() + 1  # include the punctuation char
        if end > cursor:
            spans.append((cursor, end))
        cursor = m.end()
    if cursor < len(text):
        spans.append((cursor, len(text)))
    return spans


def _split_words(text: str) -> list[tuple[int, int]]:
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text)]


_CASCADE = (_split_paragraphs, _split_lines, _split_sentences, _split_words)


def _split_recursive(
    source: str, start: int, end: int, target_tokens: int, level: int = 0
) -> list[tuple[int, int]]:
    text = source[start:end]
    if not text.strip():
        return []
    if _count_tokens(text) <= target_tokens:
        return [(start, end)]
    if level >= len(_CASCADE):
        logger.warning(
            "chunker: pass-through over-budget piece (no cascade level remains) hash=%s",
            sha256(text.encode("utf-8")).hexdigest()[:12],
        )
        return [(start, end)]
    if level == len(_CASCADE) - 1:
        logger.warning(
            "chunker: cascade reached level 4 (word boundary) hash=%s",
            sha256(text.encode("utf-8")).hexdigest()[:12],
        )
    sub_spans = _CASCADE[level](text)
    if len(sub_spans) <= 1:
        return _split_recursive(source, start, end, target_tokens, level + 1)
    result: list[tuple[int, int]] = []
    for s, e in sub_spans:
        result.extend(
            _split_recursive(source, start + s, start + e, target_tokens, level + 1)
        )
    return result


def _walk_back_tokens(source: str, start: int, end: int, overlap_tokens: int) -> int:
    text = source[start:end]
    encoding = _tokenizer().encode(text, add_special_tokens=False)
    if len(encoding.ids) <= overlap_tokens:
        return start
    target_idx = len(encoding.ids) - overlap_tokens
    char_in_text = encoding.offsets[target_idx][0]
    return start + char_in_text


def _pack(
    source: str,
    pieces: list[tuple[int, int]],
    target_tokens: int,
    overlap_tokens: int,
) -> list[tuple[int, int]]:
    if not pieces:
        return []
    chunks: list[tuple[int, int]] = []
    cur_start = pieces[0][0]
    cur_end = cur_start
    cur_tokens = 0
    for s, e in pieces:
        piece_tokens = _count_tokens(source[s:e])
        if cur_tokens + piece_tokens > target_tokens and cur_tokens > 0:
            chunks.append((cur_start, cur_end))
            cur_start = _walk_back_tokens(source, cur_start, cur_end, overlap_tokens)
        cur_end = e
        cur_tokens = _count_tokens(source[cur_start:cur_end])
    if cur_tokens > 0:
        chunks.append((cur_start, cur_end))
    return chunks


def chunk_text(source: str) -> list[Chunk]:
    if not source.strip():
        return []
    raw_file_id = sha256(source.encode("utf-8")).hexdigest()[:16]
    pieces = _split_recursive(source, 0, len(source), CHUNK_TOKEN_TARGET)
    spans = _pack(source, pieces, CHUNK_TOKEN_TARGET, CHUNK_TOKEN_OVERLAP)
    return [
        Chunk(
            raw_file_id=raw_file_id,
            chunk_index=i,
            chunk_text=source[s:e],
            char_offset_start=s,
            char_offset_end=e,
        )
        for i, (s, e) in enumerate(spans)
    ]

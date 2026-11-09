"""Project constants: the numbers and names the rest of the package agrees on.

``CHUNK_TOKEN_TARGET`` matches the 512-token context of both embedding
models, so a chunk is never truncated at embedding time. ``MAX_FILE_BYTES``
and ``ALLOWED_EXTENSIONS`` are the demo's input gates (see ``gates.py``).
"""

CHUNK_TOKEN_TARGET = 512
CHUNK_TOKEN_OVERLAP = 50

BGE_SMALL_MODEL_NAME = "BAAI/bge-small-en-v1.5"
SPLADE_MODEL_NAME = "prithivida/Splade_PP_en_v1"
DENSE_DIM = 384

MAX_FILE_BYTES = 10 * 1024 * 1024
ALLOWED_EXTENSIONS = frozenset({".txt", ".md"})

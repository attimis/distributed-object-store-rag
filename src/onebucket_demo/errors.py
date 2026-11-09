"""Project-level exceptions.

Declared once so that the search layer, the MCP server and both storage
backends raise the same classes: a caller never has to know whether a chunk
lives on local disk or in S3 to handle "it is not there".
"""

from __future__ import annotations


class CanaryMismatchError(RuntimeError):
    """The index was written with different model versions than the reader has.

    Raised by ``search.assert_canary()`` when the canary hash recorded by
    ``init_index`` differs from the hash computed at runtime. The MCP server
    refuses to start on it, because mixing embeddings from two model versions
    silently degrades every search.
    """


class ChunkNotFoundError(FileNotFoundError):
    """A chunk URI does not resolve to a stored object.

    The local backend's ``FileNotFoundError`` and the S3 backend's
    ``NoSuchKey`` are both normalised to this one class.
    """


class StorageWriteError(RuntimeError):
    """A storage backend failed to commit a write.

    Local ``OSError`` and S3 ``ClientError`` write failures are both
    normalised to this one class.
    """

"""OneBucket RAG demo: chunking, hybrid embeddings, a LanceDB index and an
MCP search server, with the same pipeline code behind three execution modes
(local polling daemon, OneBucket event consumer, AWS Lambda).

Start with ``pipeline.py`` (the two phase functions every mode calls) and
``storage.py`` (how one code path serves a local directory, AWS S3 and a
OneBucket). The design records live in ``docs/architecture/decisions/``.
"""

__version__ = "0.3.0"

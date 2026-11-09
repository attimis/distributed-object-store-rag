"""Convenience alias: ``writer.ingest`` is :func:`pipeline.ingest_local`.

Kept so the CLI and the tests can say "ingest this file" without caring
about the two-phase split. New code should import from
``onebucket_demo.pipeline`` directly.
"""

from .pipeline import WriterResult
from .pipeline import ingest_local as ingest

__all__ = ["WriterResult", "ingest"]

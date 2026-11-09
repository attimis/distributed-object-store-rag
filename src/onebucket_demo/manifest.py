"""The manifest: the hand-off between the chunker and the embedder.

The chunker writes every chunk file first and ``MANIFEST.json`` last. The
embedder is triggered only by the manifest (an S3 notification filtered on
that suffix, or the daemons noticing it), so the manifest's appearance is the
barrier that says "every chunk for this document is in place". It also
carries the document's identity (``raw_file_id``, the content hash) and the
chunk count the embedder iterates over.

``schema_version`` is mandatory and checked: an embedder that meets a
manifest it does not understand refuses it instead of guessing, which is how
the format can evolve without breaking documents that are mid-flight.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from . import storage

CURRENT_SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})

MANIFEST_FILENAME = "MANIFEST.json"


class UnsupportedManifestVersion(ValueError):
    """Embedder refuses to process a manifest with an unrecognized schema_version."""


@dataclass(frozen=True)
class Manifest:
    raw_file_id: str
    source_uri: str
    n_chunks: int
    raw_object_key: str
    created_at: str
    schema_version: int = CURRENT_SCHEMA_VERSION

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_dict(cls, data: dict) -> Manifest:
        version = data.get("schema_version")
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            raise UnsupportedManifestVersion(
                f"unsupported manifest schema_version={version!r}; "
                f"this embedder accepts {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        return cls(
            raw_file_id=data["raw_file_id"],
            source_uri=data["source_uri"],
            n_chunks=int(data["n_chunks"]),
            raw_object_key=data["raw_object_key"],
            created_at=data["created_at"],
            schema_version=version,
        )

    @classmethod
    def from_json(cls, payload: str) -> Manifest:
        return cls.from_dict(json.loads(payload))


def write(uri: str, manifest: Manifest) -> None:
    """Write a manifest to a URI via the storage abstraction."""
    storage.write_text(uri, manifest.to_json())


def read(uri: str) -> Manifest:
    """Read a manifest from a URI; raises UnsupportedManifestVersion on drift."""
    return Manifest.from_json(storage.read_text(uri))

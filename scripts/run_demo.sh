#!/usr/bin/env bash
# OneBucket Demo — local demo runner.
#
# Initializes a fresh local index, ingests every sample under data/samples/,
# then issues a hybrid search query and prints the response.
#
# Headline user story:
#   "Clone the repo, run `uv sync` and `bash scripts/run_demo.sh`, and see a
#    hybrid-search response containing both `semantic_matches` and
#    `keyword_matches` arrays — validating the end-to-end retrieval contract
#    before touching any cloud infrastructure."

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATASET_ROOT="${ONEBUCKET_DATASET_ROOT:-data/metadata}"
CHUNK_ROOT="${ONEBUCKET_CHUNK_ROOT:-data/chunks}"
QUERY="${ONEBUCKET_DEMO_QUERY:-quarterly revenue}"

echo "==> Initializing index at ${DATASET_ROOT}"
uv run python scripts/init_index.py "${DATASET_ROOT}"

echo
echo "==> Ingesting samples from data/samples/"
for f in data/samples/*.txt data/samples/*.md; do
    [ -f "$f" ] || continue
    echo "    -> ${f}"
    uv run python scripts/ingest.py "$f" "${DATASET_ROOT}" "${CHUNK_ROOT}"
done

echo
echo "==> Searching: '${QUERY}'"
uv run python -c "
import json
from onebucket_demo.search import search_global_index
result = search_global_index('${QUERY}', limit=3, dataset_root='${DATASET_ROOT}')
print(json.dumps(result, indent=2))
"

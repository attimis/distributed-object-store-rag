# Contributing

Thanks for looking under the hood. This repository is a teaching demo, so
clarity beats cleverness: a change that makes the pipeline easier to follow
is as welcome as one that makes it faster.

## Set up

```bash
uv sync --all-extras     # Python 3.11 or 3.12; installs test + demo extras
uv run pytest            # full suite
```

The first full test run downloads two embedding models (about 630 MB) from
the Hugging Face Hub into `~/.cache/fastembed`. That is a one-time cost; it
is also why the first search in a fresh environment looks slow. `pytest -m
"not models"` runs everything that does not need the models and is what CI
runs on every push.

Tests never touch a real AWS account or OneBucket: S3 is mocked with moto,
the events server with an in-process fake, and the suite scrubs
`AWS_ENDPOINT_URL`, `AWS_PROFILE` and every `ONEBUCKET_*` variable from the
environment before each test (see `tests/conftest.py`), so an operator's
shell cannot leak into a test run.

## Before you open a pull request

- `uv run pytest` is green, including `tests/test_no_internal_refs.py` (no
  private identifiers, no internal vocabulary, no links into untracked
  paths) and `tests/test_docs_links.py` (every relative link resolves).
- `uv run ruff check .` is clean.
- Docstrings explain *why* a thing is shaped the way it is. The module
  docstrings in `src/onebucket_demo/` are the model.
- Anything that varies per deployment goes in `docs/demo_runbook/variables.yml`
  (copied from `variables.example.yml`), never in code or in tracked docs.
- Never put a bucket name, access key, token, hostname of a private cluster,
  or a person's name into a tracked file. The leak guard will stop the PR,
  but please do not make it work that hard.

## Where things live

| Path | What |
|---|---|
| `src/onebucket_demo/` | The package. Start with `pipeline.py` and `storage.py`. |
| `scripts/` | Entry points: local demo, the two consumers, the OneBucket login, the AWS deploy. |
| `tests/` | Unit and integration tests; `fixtures/` holds synthetic payloads. |
| `docs/architecture/decisions/` | Architecture decision records. |
| `docs/operations/` | Setup and runbooks for each execution mode. |
| `docs/demo_runbook/` | The scripted live demo and its config template. |
| `infra/`, `lambda/` | IAM policy templates, S3 notification config, Lambda Dockerfiles. |

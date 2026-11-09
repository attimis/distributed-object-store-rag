# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems.

Report them privately through GitHub instead:
[Security → Report a vulnerability](https://github.com/attimis/distributed-object-store-rag/security/advisories/new).
We aim to acknowledge reports within three working days and will keep you
updated until a fix is released.

## Scope

This repository is a teaching demo, but some parts handle real credentials
and deserve extra care:

- `src/onebucket_demo/onebucket_auth.py` and `scripts/onebucket-login.py`
  (OAuth sign-in, token storage and refresh)
- the MCP tools that write to or delete from a bucket
  (`src/onebucket_demo/mcp_tools.py`)
- the IAM policies under `infra/iam/`

Only the latest commit on `main` is supported.

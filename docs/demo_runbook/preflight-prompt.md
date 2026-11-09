# Demo start prompt

This file is the single source for the prompt that starts a demo session.
`bash scripts/demo-setup.sh` prints the same prompt with absolute paths
filled in at the end of a successful run; if you lose that output, paste
the block below into a fresh Claude Desktop chat instead (the relative
paths resolve against the repository the `onebucket-demo` MCP server was
started from).

> Open and follow the demo runbook at
> `docs/demo_runbook/onebucket_rag_demo_runbook.md`.
>
> Start at the pre-demo checklist and run straight through to Act 4
> without pausing for operator approval between acts. The runbook has
> exactly one mandatory operator check-in (after Act 2): stop there,
> wait for me, then continue. Everywhere else, proceed.
>
> Render each Act's Summary Card before moving on.
>
> The MCP servers you need (this repo's `onebucket-demo` server and the
> OneBucket `data.onebucket.io` connector) are configured in Claude
> Desktop; call them directly. The chunk-grid visual for Act 4 is at
> `docs/demo_runbook/cache_warming_visual.html`.
>
> Deployment config (read-only reference) is at
> `docs/demo_runbook/variables.yml`.

That is the whole prompt. The runbook's pre-demo checklist walks the agent
through the config check (`get_runbook_config` / `validate_runbook_config`),
the consumer heartbeat, the index state (`list_documents`,
`search_global_index`) and the permission pre-warm; everything after the
checklist is the demo proper.

## Checklist only, no demo

To run the pre-demo checklist without starting the acts (for example the
day before a recording), paste:

> Read `docs/demo_runbook/onebucket_rag_demo_runbook.md`, run the
> pre-demo checklist and stop after the readiness banner. Do not start
> Act 0.

## Related docs

- [`onebucket_rag_demo_runbook.md`](onebucket_rag_demo_runbook.md): the demo itself (the checklist is its first section)
- [`demo_setup_runbook.md`](demo_setup_runbook.md): how to get from a fresh OneBucket account to "ready to run"
- [`variables.example.yml`](variables.example.yml): the config schema; copy it to `variables.yml` and fill in the placeholders
- [`../operations/runbooks/event-consumer.md`](../operations/runbooks/event-consumer.md): operating the event consumer the checklist relies on

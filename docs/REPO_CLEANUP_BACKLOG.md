# Repo Cleanup Backlog

This backlog tracks remaining cleanup items after phase2 baseline hardening.

## Completed

- [x] Baseline guardrails and branch policy
- [x] Secret guard + mixed-scope commit guard
- [x] Active docs vs archive split
- [x] Runtime/local clutter audit script
- [x] Runtime cleanup helper script (dry-run/apply)

## Remaining (safe to do next)

- [ ] GitHub remote connect and first push (`main`, `release/phase1-lock`)
- [ ] Add CI workflow for guard scripts + smoke tests
- [ ] Normalize line endings (`.gitattributes`) to remove CRLF warning noise

## Remaining (requires careful refactor)

- [ ] Fixture dedup in `tests/fixtures/shopee_ads_alerts*`
  - Current duplicate groups exist by design (`open/resolved/pacing` paths).
  - Refactor should preserve test path assumptions in CLI/scheduler.
- [ ] Runbook merge (`runbook_samord_minmin_probe.md` into live runbook)
  - Keep operator steps concise; avoid deleting historical trace abruptly.

## Optional (do only if requested)

- [ ] Physical delete local runtime outputs:
  - `.pytest_cache`, `artifacts`, `reports`, `dotori.db`, `shopee_tokens_export.json`
  - Command: `.\scripts\cleanup_runtime_local.ps1 -Apply`

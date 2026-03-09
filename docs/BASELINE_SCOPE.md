# Baseline Scope (Clean Core)

This file defines the minimal "do not confuse" base for this project.

## Core runtime (keep stable)

- `src/dotori_shopee_automation/shopee/`  
  Shopee auth/signing/client surface
- `src/dotori_shopee_automation/token_preflight_gate.py`  
  Token TTL gate + run guard
- `src/dotori_shopee_automation/webapp.py`  
  API endpoints (`/ops/phase1/*`, report serving)
- `src/dotori_shopee_automation/ads/`  
  Ingest, metrics, reporting
- `src/dotori_shopee_automation/discord_notifier.py`  
  Discord message transport
- `src/dotori_shopee_automation/cli.py`  
  Operational command entrypoint

## Config/runtime boundary

- Version-controlled templates only:
  - `.env.example`
  - `config/shops.example.yaml`
- Local/private runtime only (never commit):
  - `.env`
  - `shopee_tokens_export*.json`
  - `*.db`
  - `artifacts/`
  - `reports/`
  - `collaboration/`

## Commit safety rules

1. Do not mix `shopee/token/webapp` changes with `ads/report` changes in one commit.
2. Validate with:
   - `python scripts/git_secret_guard.py`
   - `python scripts/baseline_guard.py`
3. Use:
   - `.\scripts\git_commit_safe.ps1 -Message "type(scope): message" -AddAll`

## What we intentionally do NOT delete now

- Existing tests/fixtures under `tests/` are retained for regression safety.
- Existing runbooks/docs under `docs/` are retained as operational memory.
- Ignored local files are left untouched unless explicitly requested to purge.

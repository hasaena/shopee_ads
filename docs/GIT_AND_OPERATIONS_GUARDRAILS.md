# Git & Operations Guardrails (Phase1)

## 1) Priority Order (always)
1. **Shopee live connectivity safety**
2. **Report/alert data integrity**
3. **Operational reproducibility**
4. **Feature/UI tuning**

If any change can impact (1) or (2), stop and run the release checklist before push.

---

## 2) Source of Truth

- **Live DB**: production DB only (never local fixture DB)
- **Final report numbers**: DB aggregate -> single metrics helper -> HTML/MD/Discord
- **Token health**: `/ops/phase1/token/status` on server
- **Scope freeze**:
  - product-level campaign: per-campaign supported
  - gms/group/shop-auto: aggregate only

---

## 3) Common Failure Modes (must avoid)

1. Local `.env` values accidentally used for production command
2. Old HTML left in reports folder (stale rendered file)
3. Token preflight ignored (expired tokens -> live run skipped)
4. Mixed environments (local DB + server reports path)
5. Secrets committed to git (webhooks, partner key, ops token)

---

## 4) Release Checklist (before push/deploy)

1. Confirm environment target
   - DB path
   - shops config path
   - reports dir
   - Discord webhook target
2. Token check
   - `ops phase1 token appsscript status`
   - `ops phase1 token appsscript preflight --min-access-ttl-sec 120`
3. Re-render target report (if reconcile mismatch/stale suspected)
4. Reconcile run
   - `ops phase1 report reconcile ...`
   - expected root cause: `aligned_db_and_rendered`
5. Smoke send (report/alerts webhook)
6. Record result file

---

## 5) Git Branch Policy

- `main`: stable only
- `release/phase1-lock`: current production baseline
- `hotfix/*`: urgent bug fixes
- `feature/*`: normal changes

Merge rule:
- feature -> release branch first
- verify checklist
- then release -> main

---

## 6) Commit Policy

- One purpose per commit
- Commit message format:
  - `fix(report): align minmin final reconcile path`
  - `ops(token): tighten preflight logging`
  - `docs(runbook): add release checklist`
- Never commit:
  - `.env`
  - token exports
  - DB files
  - runtime reports/artifacts/logs

---

## 7) Incident Playbook (quick)

If report numbers look wrong:
1. Check report file timestamp (stale?)
2. Run reconcile on same shop/date
3. If stale/unparsable -> re-render same date
4. Re-run reconcile
5. Only then investigate code

If live run skipped:
1. Check token preflight
2. Refresh/push from Apps Script
3. Confirm server token status
4. Re-run job


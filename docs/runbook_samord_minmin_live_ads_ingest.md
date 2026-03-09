# Runbook — SAMORD/MINMIN Live Ads Ingest

This runbook explains how to run the live ads ingest pipeline (plan + mapping) in dry-run or live mode.

## Prerequisites
- Live mode only: `SHOPEE_PARTNER_ID` and `SHOPEE_PARTNER_KEY` set in `.env`
- Live mode only: `SHOPEE_API_HOST` set (default: https://partner.shopeemobile.com)
- Live mode only: Tokens stored via `shopee exchange-code`
- `config/shops.yaml` includes `shopee_shop_id` for SAMORD/MINMIN

## Phase 1 .env template
Copy the template and fill in required values:
```powershell
Copy-Item collaboration/env/.env.phase1.template .env
```

Required fields:
- `DISCORD_WEBHOOK_REPORT_URL`, `DISCORD_WEBHOOK_ALERTS_URL`
- `SHOPEE_PARTNER_ID`, `SHOPEE_PARTNER_KEY`
- `SHOPEE_SAMORD_SHOP_ID`, `SHOPEE_MINMIN_SHOP_ID`

Optional:
- `DISCORD_WEBHOOK_ACTIONS_URL` (if empty, report webhook is reused for actions)
- `REPORT_BASE_URL`, `REPORT_ACCESS_TOKEN`

Notes:
- Do NOT store access/refresh tokens in env. Tokens are stored in the DB via `shopee exchange-code`.

## Dry-run (no HTTP, no DB writes)
```powershell
python -m dotori_shopee_automation.cli ads live ingest --shop samord --date 2026-02-03 --dry-run --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml
```

Expected output (example):
```
planned_calls: shop_info, ads_daily
mapping_coverage: mapped=1 unmapped=1 missing=[shop_info]
call=shop_info method=GET path=/api/v2/shop/get_shop_info mapping=missing
call=ads_daily method=GET path=/api/v2/marketing/TODO_REPLACE_ME mapping=daily
```

- exits 0
- no DB rows created

## Strict mapping (fail on unmapped calls)
```powershell
python -m dotori_shopee_automation.cli ads live ingest --shop samord --date 2026-02-03 --dry-run --strict-mapping --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml
```

Expected:
- exits non-zero if the plan contains unmapped calls
- output includes `strict_mapping_missing: ...`

## Fixtures mode (local verification, no credentials)
```powershell
python -m dotori_shopee_automation.cli ads live ingest-all --date 2026-02-03 --only-shops samord,minmin --transport fixtures --fixtures-dir tests/fixtures/shopee_ads --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml
```

## One-off preview run (fixtures-first)
```powershell
python -m dotori_shopee_automation.cli ops phase1 preview --date 2026-02-03 --only-shops samord,minmin --transport fixtures --fixtures-dir tests/fixtures/shopee_ads --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml --reports-dir collaboration/reports --no-send-discord
```

Live mode (requires credentials + allow-network):
```powershell
python -m dotori_shopee_automation.cli ops phase1 preview --date 2026-02-03 --only-shops samord,minmin --transport live --allow-network --send-discord
```

## Live preview (manual, right now)
1) Copy env file:
```powershell
Copy-Item collaboration/env/.env.phase1.local.example collaboration/env/.env.phase1.local
```

2) Fill required values (webhooks, partner id/key, shop ids, ads endpoints).
3) Seed tokens (tokens are stored in DB, not env):
```powershell
python -m dotori_shopee_automation.cli shopee exchange-code --shop samord --code <AUTH_CODE>
python -m dotori_shopee_automation.cli shopee exchange-code --shop minmin --code <AUTH_CODE>
```

4) Verify readiness using env-file:
```powershell
python -m dotori_shopee_automation.cli ops phase1 verify --env-file collaboration/env/.env.phase1.local --shops samord,minmin
```

5) Run live preview (manual, right now):
```powershell
python -m dotori_shopee_automation.cli ops phase1 preview --env-file collaboration/env/.env.phase1.local --transport live --allow-network --date 2026-02-03 --only-shops samord,minmin
```

Notes:
- Discord sending is opt-in (`--send-discord`).
- Network is opt-in (`--allow-network`).
- Tokens live in the DB; never store access/refresh tokens in `.env`.

## Apps Script token export (ScriptProperties)
If you already have tokens managed in Google Apps Script, export them safely and import into the DB.

Apps Script export example (do not log JSON contents):
```javascript
function exportShopeeTokensToDrive_SAMORD_MINMIN(){
  const SProps = PropertiesService.getScriptProperties();
  const all = SProps.getProperties();
  const out = {};
  const targetShopIds = [497412318, 567655304]; // samord, minmin
  targetShopIds.forEach(sid=>{
    const k = "SHOPEE_TOKEN_DATA_" + sid;
    if(all[k]) out[k] = JSON.parse(all[k]);
  });
  const content = JSON.stringify(out, null, 2);
  const file = DriveApp.createFile("shopee_tokens_export_phase1.json", content, MimeType.PLAIN_TEXT);
  Logger.log("Exported to Drive. fileId=" + file.getId());
}
```

Important:
- Do NOT `Logger.log` the JSON body (token leak risk).
- Download the file, import locally, then delete/restrict sharing.

Import into DB:
```powershell
python -m dotori_shopee_automation.cli ops phase1 token appsscript import --env-file collaboration/env/.env.phase1.local --file <downloaded_json> --shops samord,minmin
python -m dotori_shopee_automation.cli ops phase1 token appsscript status --env-file collaboration/env/.env.phase1.local --shops samord,minmin
```

### Format B (raw ScriptProperties export)
If your export is a raw properties map (values are JSON strings), the importer supports it.

Apps Script export example (Format B, do not log JSON contents):
```javascript
function exportShopeeTokensRawProperties(){
  const SProps = PropertiesService.getScriptProperties();
  const all = SProps.getProperties();
  const out = {};
  const targetShopIds = [497412318, 567655304]; // samord, minmin
  targetShopIds.forEach(sid=>{
    const k = "SHOPEE_TOKEN_DATA_" + sid;
    if(all[k]) out[k] = all[k]; // keep as raw JSON string
  });
  const content = JSON.stringify(out, null, 2);
  const file = DriveApp.createFile("shopee_tokens_export_raw.json", content, MimeType.PLAIN_TEXT);
  Logger.log("Exported to Drive. fileId=" + file.getId());
}
```

## Preflight: access_expires_in_sec=-1
If preflight shows `access_expires_in_sec=-1`, it means **unknown expiry** (missing/parse error),
not necessarily a real expiration. Recommended actions:
1) Run `diag_TOKEN(shop_id)` in Apps Script to refresh + store a valid expiry.
2) Immediately re-run `exportShopeeTokensToDrive_Normalized()` and download.

If you must collect evidence urgently (not recommended), opt-in with:
```powershell
python -m dotori_shopee_automation.cli ops phase1 token appsscript preflight `
  --env-file collaboration/env/.env.phase1.local `
  --token-file shopee_tokens_export.json `
  --shops samord,minmin `
  --min-access-ttl-sec 600 `
  --allow-unknown-expiry
```

## Token Gate: TTL Low -> Auto Pause -> Auto Resume
Recommended env defaults:
- `DOTORI_STRICT_PREFLIGHT=1`
- `DOTORI_MIN_ACCESS_TTL_SEC=1200`
- `DOTORI_TOKEN_ALERT_COOLDOWN_SEC=21600`
- `DOTORI_TOKEN_RESOLVED_COOLDOWN_SEC=21600`

What alerts look like:
- `[SAMORD][ALERT] TOKEN_TTL_LOW ...` (cooldown applies)
- `[SAMORD][ALERT] TOKEN_TTL_OK (resolved) ...` (cooldown applies)

Operator steps:
1) In Apps Script run token refresh/export and push/import to server.
```powershell
python -m dotori_shopee_automation.cli ops phase1 token appsscript import `
  --env-file collaboration/env/.env.phase1.local `
  --file shopee_tokens_export.json `
  --shops samord,minmin
```
2) Confirm gate status is healthy:
```powershell
python -m dotori_shopee_automation.cli ops phase1 token status `
  --env-file collaboration/env/.env.phase1.local `
  --shops samord,minmin `
  --db collaboration/phase1_live.db `
  --min-access-ttl-sec 1200
```
3) Next scheduled job auto-resumes if gate is `ok`; resolved alert is emitted once per cooldown window.

## Token mode (passive)
To avoid token rotation risk, use passive mode:
```powershell
python -m dotori_shopee_automation.cli ops phase1 preview --token-mode passive --transport live --allow-network --env-file collaboration/env/.env.phase1.local
```

Notes:
- Passive mode disables refresh; if access token is expired you must re-export from Apps Script.

## Ops smoke (fixtures, no network)
```powershell
python -m dotori_shopee_automation.cli ops smoke ads-live-fixtures --date 2026-02-03 --only-shops samord,minmin --transport fixtures --fixtures-dir tests/fixtures/shopee_ads --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml --reports-dir collaboration/reports --no-send-discord
```

Expected output snippet:
```
smoke_ads_live_start date=2026-02-03 shops=samord,minmin transport=fixtures
planned_calls: shop_info, ads_daily, ads_snapshot
mapping_coverage: mapped=3 unmapped=0 missing=[]
shop=samord date=2026-02-03 calls_ok=3 calls_fail=0
upserted campaigns=2 daily=2 snapshots=2
shop=minmin date=2026-02-03 calls_ok=3 calls_fail=0
upserted campaigns=2 daily=2 snapshots=2
report_path shop=samord path=collaboration/reports/samord/daily/2026-02-03_final.html
report_path shop=minmin path=collaboration/reports/minmin/daily/2026-02-03_final.html
alerts_total active=0 opened=0 resolved=0 notified=0
smoke_ok=1
```

## Phase 1 readiness check (no network)
```powershell
python -m dotori_shopee_automation.cli ops readiness phase1 --shops samord,minmin
```

Interpretation:
- `ready=0 missing=...` means required config/token(s) are missing.
- Fix missing items (webhooks + tokens), then re-run and expect `ready=1`.

Next actions when ready:
- `ops check discord --send --channel both`
- `ops check shopee-ping --transport live --allow-network`

## Phase 1 Verify (single command)
Safe default (no network, no send):
```powershell
python -m dotori_shopee_automation.cli ops phase1 verify --shops samord,minmin
```

Send Discord test (requires webhooks):
```powershell
python -m dotori_shopee_automation.cli ops phase1 verify --shops samord,minmin --send-discord --channel both
```

Live ping (requires allow-network):
```powershell
python -m dotori_shopee_automation.cli ops phase1 verify --shops samord,minmin --ping-live --allow-network
```

## Phase 1 token DB finder
Use this if tokens are missing or you suspect the wrong `DATABASE_URL`.
```powershell
python -m dotori_shopee_automation.cli ops phase1 token-db find --only-shops samord,minmin
```

Expected output (shape):
```
token_db_candidate path=... token_store=1 samord_token=1 minmin_token=1
recommended_database_url=sqlite:///...
```

Notes:
- Tokens live in the DB (`shopee_tokens`), not in `.env`.
- The finder prints booleans only; never prints token values.

## Phase 1 one-command capture
This wraps verify + preview (and optional probe-on-failure) and saves a markdown capture file.
```powershell
python -m dotori_shopee_automation.cli ops phase1 capture --date 2026-02-03 --only-shops samord,minmin --transport fixtures --fixtures-dir tests/fixtures/shopee_ads --plan collaboration/plans/ads_ingest_minimal.yaml --mapping collaboration/mappings/ads_mapping.yaml --reports-dir collaboration/reports --no-send-discord
```

Live capture (requires allow-network):
```powershell
python -m dotori_shopee_automation.cli ops phase1 capture --env-file collaboration/env/.env.phase1.local --date 2026-02-03 --only-shops samord,minmin --transport live --allow-network --no-send-discord
```

Notes:
- Network is opt-in (`--allow-network`).
- Discord is opt-in (`--send-discord`).
- Capture writes a markdown summary and prints `capture_md path=...`.

## Live evidence + support packet (one command)
Required files:
- `collaboration/env/.env.phase1.local`
- `shopee_tokens_export.json` (Apps Script export; local only; gitignored)

Command template:
```powershell
DATABASE_URL=sqlite:///./collaboration/phase1_live.db `
python -m dotori_shopee_automation.cli ops phase1 evidence run `
  --env-file collaboration/env/.env.phase1.local `
  --token-file shopee_tokens_export.json `
  --date YYYY-MM-DD `
  --shops samord,minmin `
  --transport live `
  --allow-network `
  --token-mode passive `
  --artifacts-root collaboration/artifacts/shopee_api `
  --out collaboration/results/phase1_failures_YYYY-MM-DD_live.md `
  --evidence-out collaboration/results/phase1_evidence_YYYY-MM-DD_live.md `
  --support-packet `
  --support-zip collaboration/results/phase1_support_packet_YYYY-MM-DD_live.zip `
  --support-md collaboration/results/phase1_support_request_YYYY-MM-DD_live.md
```

Notes:
- Passive mode avoids rotating refresh tokens outside Apps Script.
- If access token expires, re-export from Apps Script and rerun.
- Never share raw token files.
- Evidence summary now filters artifacts created during the current run (run_started_ms).
  To summarize a specific window manually: `ops phase1 artifacts summarize-failures --since-ms ... --until-ms ...`.

Tip:
- For LIVE `shopee-ping`/`phase1 preview`, you can pass `--token-file shopee_tokens_export.json`
  to sync tokens into DB first (passive; no refresh calls).

## Auth Debug (safe fingerprint)
Use this when you need to compare Apps Script token/sign behavior with Python.

Python (live evidence run with fingerprints):
```powershell
python -m dotori_shopee_automation.cli ops phase1 evidence run `
  --env-file collaboration/env/.env.phase1.local `
  --token-file shopee_tokens_export.json `
  --date YYYY-MM-DD `
  --shops samord,minmin `
  --transport live `
  --allow-network `
  --token-mode passive `
  --auth-debug
```

Apps Script (do not log raw tokens):
```javascript
function tokenFingerprint(shopId){
  const tok = ShopeeTokenManager.getAccessToken(shopId);
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, tok, Utilities.Charset.UTF_8);
  const hex = bytes.map(b => (b<0?b+256:b).toString(16).padStart(2,'0')).join('');
  Logger.log('token_len=' + tok.length + ' token_sha256_8=' + hex.slice(0,8));
}
```

## Signature parity (offline)
Generate Python sign fingerprints (sha8 only) and compare with Apps Script logs.

Python:
```powershell
python -m dotori_shopee_automation.cli ops phase1 auth sign-fingerprint `
  --env-file collaboration/env/.env.phase1.local `
  --token-file shopee_tokens_export.json `
  --shops samord,minmin `
  --out collaboration/env/auth_sign_fingerprint.json
```

Apps Script (run once, paste logs into a local file):
```javascript
function diag_sign_fingerprint_samord_minmin(){
  const ts = 1700000000;
  const paths = ["/api/v2/shop/get_shop_info", "/api/v2/ads/get_all_cpc_ads_daily_performance"];
  const shops = [{ name: "samord", id: 497412318 }, { name: "minmin", id: 567655304 }];
  function sha8(s){
    const raw = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, s, Utilities.Charset.UTF_8);
    return raw.map(b => (b<0?b+256:b).toString(16).padStart(2,"0")).join("").slice(0,8);
  }
  function hmacHex(m,k){
    return Utilities.computeHmacSha256Signature(m,k,Utilities.Charset.UTF_8)
      .map(b=>(b<0?b+256:b).toString(16).padStart(2,"0")).join("");
  }
  Logger.log(`partner_id=${PARTNER_ID}`);
  Logger.log(`partner_key_sha8=${sha8(PARTNER_KEY)}`);
  shops.forEach(s => {
    const tok = ShopeeTokenManager.getAccessToken(s.id);
    Logger.log(`shop=${s.name} shop_id=${s.id} token_len=${tok.length} token_sha8=${sha8(tok)}`);
    paths.forEach(p => {
      const signIn = `${PARTNER_ID}${p}${ts}${tok}${s.id}`;
      const sign = hmacHex(signIn, PARTNER_KEY);
      Logger.log(`shop=${s.name} path=${p} ts=${ts} sign_input_sha8=${sha8(signIn)} sign_sha8=${sha8(sign)}`);
    });
  });
}
```
Source file in repo: `collaboration/appsscript/diag_sign_fingerprint.gs`

Compare:
```powershell
python -m dotori_shopee_automation.cli ops phase1 auth sign-parity `
  --python-file collaboration/env/auth_sign_fingerprint.json `
  --appsscript-txt collaboration/env/appsscript_sign_fingerprint.txt
```

## Baseline check (live, opt-in)
Use this to verify `/api/v2/shop/get_shop_info` auth only.
```powershell
python -m dotori_shopee_automation.cli ops phase1 baseline shop-info `
  --env-file collaboration/env/.env.phase1.local `
  --token-file shopee_tokens_export.json `
  --shops samord,minmin `
  --token-mode passive `
  --allow-network `
  --save-failure-artifacts
```

## Phase 1 Ads endpoint sweep (optional)
Try multiple Ads endpoint candidates and compare outcomes.
```powershell
python -m dotori_shopee_automation.cli ops phase1 ads-endpoint sweep --date 2026-02-03 --only-shops samord,minmin --transport live --allow-network --env-file collaboration/env/.env.phase1.local --candidates collaboration/endpoints/ads_candidates.yaml --artifacts-dir collaboration/artifacts/shopee_api --analyze
```

## Live ingest (once endpoints are confirmed)
```powershell
python -m dotori_shopee_automation.cli ads live ingest --shop samord --date 2026-02-03 --plan collaboration/plans/ads_probe.yaml --mapping collaboration/mappings/ads_mapping.yaml
```

For both shops:
```powershell
python -m dotori_shopee_automation.cli ads live ingest-all --date 2026-02-03 --only-shops samord,minmin --plan collaboration/plans/ads_probe.yaml --mapping collaboration/mappings/ads_mapping.yaml
```

## Where data goes
- `ads_campaign`, `ads_campaign_daily`, `ads_campaign_snapshot` tables are updated (upsert).
- Reports and alerts use the same DB tables; daily/weekly reports will reflect new data.

## Notes
- Mapping is controlled via `collaboration/mappings/ads_mapping.yaml`.
- Plan is controlled via `collaboration/plans/ads_probe.yaml`.
- Default is **no artifact saving**. Enable `--save-artifacts` only when you need sample payloads.

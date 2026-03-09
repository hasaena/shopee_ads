# Runbook — SAMORD / MINMIN Shopee Probe

This runbook helps you safely run Shopee plan probes for SAMORD and MINMIN to capture redacted artifacts for Ads Provider development.

## 1) shops.yaml: enable only SAMORD/MINMIN
Edit `config/shops.yaml` and set:
```yaml
- shop_key: samord
  label: SAMORD
  enabled: true
  shopee_shop_id: 123456

- shop_key: minmin
  label: MINMIN
  enabled: true
  shopee_shop_id: 654321
```
Set all other shops to `enabled: false` if you want `run-plan-all` to target only these.

## 2) .env (examples only)
```
SHOPEE_PARTNER_ID=1000
SHOPEE_PARTNER_KEY=your_partner_key_here
SHOPEE_API_HOST=https://partner.shopeemobile.com
SHOPEE_REDIRECT_URL=https://example.com/callback
DATABASE_URL=sqlite:///./dotori.db
```

Environment variable names to verify (no values shown):
- `SHOPEE_PARTNER_ID`
- `SHOPEE_PARTNER_KEY`
- `SHOPEE_API_HOST`
- `SHOPEE_REDIRECT_URL`
- `DISCORD_WEBHOOK_REPORT_URL`
- `DISCORD_WEBHOOK_ALERTS_URL`
- `DATABASE_URL`

## 3) (Optional) Discord webhook check
```powershell
python -m dotori_shopee_automation.cli discord-test --channel report --text "hello from dotori" --shop samord
```

## 3.5) Smoke: one command verification
Dry-run (no HTTP):
```powershell
python -m dotori_shopee_automation.cli ops smoke --no-live-http --only-shops samord,minmin
```

Live HTTP (requires tokens + partner credentials):
```powershell
python -m dotori_shopee_automation.cli ops smoke --live-http --send-discord --only-shops samord,minmin
```

Green checklist:
- Enabled shops listed correctly
- Discord start + done messages delivered
- Probe summary md/csv created under `collaboration/outputs/probe_summaries/{YYYYMMDD}/`
- No secrets printed in console output

## 4) Run shop_info plan (dry-run → real)
Dry-run preview:
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop samord --plan collaboration/plans/shop_info.yaml --dry-run
```

Real execution:
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop samord --plan collaboration/plans/shop_info.yaml
```

## 5) Fill ads_probe.yaml and run
Open `collaboration/plans/ads_probe.yaml` and replace the `TODO_REPLACE_ME` paths with actual Shopee Ads/Marketing endpoints from the Open Platform console.

Example run (all enabled shops):
```powershell
python -m dotori_shopee_automation.cli shopee run-plan-all --plan collaboration/plans/ads_probe.yaml --vars date=2026-02-01 --vars date_from=2026-02-01 --vars date_to=2026-02-02 --vars timestamp=1706832000 --no-print
```

If you want to target only SAMORD/MINMIN regardless of enabled flags:
```powershell
python -m dotori_shopee_automation.cli shopee run-plan-all --plan collaboration/plans/ads_probe.yaml --only-shops samord,minmin --vars date=2026-02-01 --vars date_from=2026-02-01 --vars date_to=2026-02-02 --vars timestamp=1706832000 --no-print
```

## 6) One-command probe suite (plan → analyze → discord)
Shop info (quick check):
```powershell
python -m dotori_shopee_automation.cli shopee probe-suite --date 20260203 --only-shops samord,minmin --plan collaboration/plans/shop_info.yaml
```

Ads probe (after filling endpoints):
```powershell
python -m dotori_shopee_automation.cli shopee probe-suite --date 20260203 --only-shops samord,minmin --plan collaboration/plans/ads_probe.yaml --send-discord --channel report
```

Outputs are stored under:
```
collaboration/outputs/probe_summaries/{YYYYMMDD}/probe_summary_{YYYYMMDD}.md
collaboration/outputs/probe_summaries/{YYYYMMDD}/probe_summary_{YYYYMMDD}.csv
```

## Round 1 (SAMORD/MINMIN)
1) Run shop_info plan to validate auth + basic access:
```powershell
python -m dotori_shopee_automation.cli shopee probe-suite --date 20260203 --only-shops samord,minmin --plan collaboration/plans/shop_info.yaml
```

2) Run ads_probe plan after filling endpoints:
```powershell
python -m dotori_shopee_automation.cli shopee probe-suite --date 20260203 --only-shops samord,minmin --plan collaboration/plans/ads_probe.yaml --send-discord --channel report
```

3) Share outputs with the architect:
- `collaboration/outputs/probe_summaries/{YYYYMMDD}/probe_summary_{YYYYMMDD}.md`
- `collaboration/outputs/probe_summaries/{YYYYMMDD}/probe_summary_{YYYYMMDD}.csv`

## 7) Artifact path rules
Artifacts are saved to:
```
collaboration/artifacts/shopee_api/{shop_key}/{YYYYMMDD}/{ts}_{call_name}_{safe_path}.json
```

For the next task (Ads Provider v1), share the entire folder for each shop and date so parsing can be built against real payloads.

## 8) Analyze probe results
Generate markdown + CSV summaries for a given date:
```powershell
python -m dotori_shopee_automation.cli shopee probe-analyze --date 20260201 --only-shops samord,minmin
```

Generate only markdown (with schema hints) to a custom output dir:
```powershell
python -m dotori_shopee_automation.cli shopee probe-analyze --date 20260201 --only-shops samord,minmin --format md --include-schema-hints --out-dir collaboration/artifacts
```

Send a compact summary to Discord:
```powershell
python -m dotori_shopee_automation.cli shopee probe-analyze --date 20260201 --only-shops samord,minmin --send-discord --channel report
```

Quick console list:
```powershell
python -m dotori_shopee_automation.cli shopee probe-list --date 20260201 --only-shops samord,minmin
```

Summaries are saved under `collaboration/artifacts/` as:
- `probe_summary_YYYYMMDD.md`
- `probe_summary_YYYYMMDD.csv`

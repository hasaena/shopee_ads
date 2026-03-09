# Ads Rate-Limit Runbook (Phase1)

Last updated: 2026-03-05 (Asia/Ho_Chi_Minh)

## Scope

- Shops: `samord,minmin` only.
- This runbook is for Ads API rate-limit cooldown state (`ads_rate_limit_total_api`, 429, related cases).

## What changed

- This change does **not** modify Discord report/alert send logic.
- It only changes how Ads probe handles rate-limit:
  - Persist cooldown state in one fixed file.
  - Skip repeated calls while cooldown is active.
  - Expose current cooldown state in ops status.

## Typical symptom

When GMS/Group ads endpoints are blocked by rate-limit:

- Product-level rows may still exist.
- Group/Shop/Auto details may collapse into fallback (`SHOP_TOTAL` / non-product aggregate).
- Probe verdict can be `unknown(reason=rate_limited|cooldown_active)`.

This can be **normal** during cooldown windows.

## Production wiring (required)

1) Prepare persistent directory:

```bash
sudo mkdir -p /var/lib/dotori_shopee_automation
sudo chown -R dotori:dotori /var/lib/dotori_shopee_automation
sudo chmod 775 /var/lib/dotori_shopee_automation
```

2) Add env:

```bash
DOTORI_ADS_RATE_LIMIT_STATE_PATH=/var/lib/dotori_shopee_automation/ads_rate_limit_state.json
```

3) Restart service:

```bash
sudo systemctl restart dotori_shopee_automation
```

4) Verify status:

```bash
curl -s http://127.0.0.1:8000/ops/phase1/status
```

## How to check quickly

### API status

- `GET /ops/phase1/status`
- `python -m dotori_shopee_automation.cli ops phase1 status dump --shops samord,minmin --pretty`

Check section:

- `ads_rate_limit.<shop>.cooldown_active`
- `ads_rate_limit.<shop>.cooldown_until_utc`
- `ads_rate_limit.<shop>.last_api_error`
- `ads_rate_limit.<shop>.state_path`

## Operator decision flow

1) **Preferred**: wait until `cooldown_until_utc`.
2) Run a single manual probe when needed:

```bash
python -m dotori_shopee_automation.cli ops phase1 ads campaign-probe \
  --only-shops samord,minmin \
  --mode live \
  --days 1 \
  --out collaboration/artifacts/manual_probe \
  --redact \
  --ignore-cooldown
```

3) Last resort (not recommended): remove state file and retry once.

```bash
rm -f /var/lib/dotori_shopee_automation/ads_rate_limit_state.json
```

## Normal vs abnormal

### Normal

- `cooldown_active=true` and `last_api_error` is rate-limit related.
- System skips repeated calls and continues other healthy jobs.

### Abnormal

- `cooldown_active=false` for long period but same calls keep failing.
- Token/auth errors mixed with rate-limit errors.
- State path unwritable or missing (no state persistence).

## Security notes

- Never log raw access/refresh tokens or partner key.
- Only token metadata (sha8/len) is allowed in evidence.

# Shopee Plan Runner

The plan runner executes a batch of Shopee API calls across one or many shops, saving redacted artifacts for later parser/provider development.

## Plan file format (v1)
```yaml
version: 1
name: ads_probe
defaults:
  method: GET
  save: true
calls:
  - name: shop_info
    path: /api/v2/shop/get_shop_info
    params: {}

  - name: ads_daily_report
    path: /api/v2/marketing/REPLACE_ME
    params:
      date: "{{date}}"
```

Template variables use `{{var}}` in string values. Built-ins:
- `shop_id`
- `shop_key`
- `now_iso`
- `today`

CLI `--vars key=value` overrides built-ins.

## Run for one shop
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop shop_a --plan collaboration/plans/ads_probe.yaml --vars date=2026-02-01
```

## Run for all enabled shops
```powershell
python -m dotori_shopee_automation.cli shopee run-plan-all --plan collaboration/plans/ads_probe.yaml --vars date=2026-02-01
```

## Only specific shops
```powershell
python -m dotori_shopee_automation.cli shopee run-plan-all --plan collaboration/plans/ads_probe.yaml --only-shops samord,minmin --vars date=2026-02-01
```

## Continue on error
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop shop_a --plan collaboration/plans/ads_probe.yaml --continue-on-error
```

## Dry-run (no HTTP)
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop shop_a --plan collaboration/plans/ads_probe.yaml --dry-run
```

## Saving artifacts
By default artifacts are stored at:
```
collaboration/artifacts/shopee_api/{shop_key}/{YYYYMMDD}/{ts}_{call_name}_{safe_path}.json
```

Override the root:
```powershell
python -m dotori_shopee_automation.cli shopee run-plan --shop shop_a --plan collaboration/plans/ads_probe.yaml --save-root D:\temp\shopee_api
```

## Redaction
- Any key containing `token` is redacted.
- Request meta also redacts `partner_key`, `access_token`, `refresh_token`.
- Saved artifacts always include a `__meta` block plus the redacted response.

# Shopee API Explorer

Use the Shopee CLI group to discover endpoints, validate access, and capture redacted payloads for future parsing.

## Prerequisites
- Set `SHOPEE_PARTNER_ID`, `SHOPEE_PARTNER_KEY`, `SHOPEE_API_HOST`, `SHOPEE_REDIRECT_URL` in `.env`.
- Ensure your shop config has `shopee_shop_id` for each shop.

## Auth flow (once per shop)
Generate auth URL:
```powershell
python -m dotori_shopee_automation.cli shopee auth-url --shop shop_a --redirect https://example.com/callback
```

After the user approves, exchange the code:
```powershell
python -m dotori_shopee_automation.cli shopee exchange-code --shop shop_a --code YOUR_CODE
```

## Quick health check
```powershell
python -m dotori_shopee_automation.cli shopee ping --shop shop_a
```

## Call any endpoint (GET)
Shop info:
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method GET --path /api/v2/shop/get_shop_info
```

Order list (example params):
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method GET --path /api/v2/order/get_order_list --params time_range_field=update_time --params time_from=1700000000 --params time_to=1700003600 --params page_size=50
```

## Call any endpoint (POST)
Inline JSON:
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method POST --path /api/v2/ads/get_ads_performance --json "{\"period\":\"D\",\"start_time\":1700000000,\"end_time\":1700086400}"
```

From file:
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method POST --path /api/v2/ads/get_ads_performance --json @payload.json
```

## Save responses (redacted)
Default save location (auto filename):
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method GET --path /api/v2/shop/get_shop_info --save
```

Custom save path:
```powershell
python -m dotori_shopee_automation.cli shopee call --shop shop_a --method GET --path /api/v2/shop/get_shop_info --save-path collaboration/artifacts/shopee_api/shop_a/shop_info.json
```

## Notes
- Output and saved JSON are redacted to remove `access_token`, `refresh_token`, and any key containing `token`.
- Use `--no-print` to skip console output while still saving.

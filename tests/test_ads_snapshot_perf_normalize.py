from __future__ import annotations

from dotori_shopee_automation.ads.provider_live_plan import (
    _merge_ads_daily_breakdown_with_shop_total,
    _normalize_ads_daily_payload,
    _normalize_ads_snapshot_perf_payload,
)


def test_normalize_snapshot_perf_response_list_shape() -> None:
    payload = {
        "error": "",
        "message": "",
        "response": [
            {
                "date": "16-02-2026",
                "impression": 1990,
                "clicks": 52,
                "direct_order": 1,
                "broad_order": 2,
                "direct_gmv": 445000,
                "broad_gmv": 890000,
                "expense": 71235,
            }
        ],
    }

    out = _normalize_ads_snapshot_perf_payload(payload, ts_iso="2026-02-16T10:15:16+00:00")
    records = out.get("response", {}).get("records")
    assert isinstance(records, list)
    assert len(records) == 1
    row = records[0]
    assert row.get("campaign_id") == "SHOP_TOTAL"
    assert row.get("campaign_name") == "SHOP_TOTAL"
    assert row.get("spend_today") == 71235
    assert row.get("impressions_today") == 1990
    assert row.get("clicks_today") == 52
    assert row.get("orders_today") == 1
    assert row.get("gmv_today") == 445000
    assert row.get("ts") == "2026-02-16T10:15:16+00:00"


def test_normalize_daily_response_list_shape_shop_total() -> None:
    payload = {
        "error": "",
        "message": "",
        "response": [
            {
                "date": "16-02-2026",
                "impression": 1990,
                "clicks": 52,
                "direct_order": 1,
                "broad_order": 2,
                "direct_gmv": 445000,
                "broad_gmv": 890000,
                "expense": 71235,
            }
        ],
    }

    out = _normalize_ads_daily_payload(payload)
    records = out.get("response", {}).get("records")
    assert isinstance(records, list)
    assert len(records) == 1
    row = records[0]
    assert row.get("campaign_id") == "SHOP_TOTAL"
    assert row.get("campaign_name") == "SHOP_TOTAL"
    assert row.get("spend") == 71235
    assert row.get("impressions") == 1990
    assert row.get("clicks") == 52
    assert row.get("orders") == 1
    assert row.get("gmv") == 445000


def test_normalize_daily_response_campaign_list_metrics_shape() -> None:
    payload = {
        "error": "",
        "message": "",
        "response": {
            "campaign_list": [
                {
                    "campaign_id": 184826948,
                    "campaign_name": "Product A",
                    "metrics_list": [
                        {
                            "date": "01-03-2026",
                            "impression": 1819,
                            "click": 47,
                            "expense": 84838,
                            "direct_order": 0,
                            "broad_order": 0,
                            "direct_gmv": 0,
                            "broad_gmv": 0,
                        }
                    ],
                },
                {
                    "campaign_id": 184991968,
                    "common_info": {"ad_name": "Product B"},
                    "metrics_list": [
                        {
                            "date": "01-03-2026",
                            "impression": 1235,
                            "clicks": 59,
                            "expense": 100000,
                            "order": 1,
                            "revenue": 250000,
                        }
                    ],
                },
            ]
        },
    }

    out = _normalize_ads_daily_payload(payload)
    records = out.get("response", {}).get("records")
    assert isinstance(records, list)
    assert len(records) == 2

    row_a = records[0]
    assert row_a.get("campaign_id") == "184826948"
    assert row_a.get("campaign_name") == "Product A"
    assert row_a.get("spend") == 84838
    assert row_a.get("impressions") == 1819
    assert row_a.get("clicks") == 47
    assert row_a.get("orders") == 0
    assert row_a.get("gmv") == 0.0

    row_b = records[1]
    assert row_b.get("campaign_id") == "184991968"
    assert row_b.get("campaign_name") == "Product B"
    assert row_b.get("spend") == 100000
    assert row_b.get("impressions") == 1235
    assert row_b.get("clicks") == 59
    assert row_b.get("orders") == 1
    assert row_b.get("gmv") == 250000


def test_merge_ads_daily_breakdown_keeps_shop_total_row() -> None:
    base_payload = {
        "error": "",
        "message": "",
        "response": [
            {
                "date": "01-03-2026",
                "campaign_id": "SHOP_TOTAL",
                "campaign_name": "SHOP_TOTAL",
                "impression": 3000,
                "clicks": 100,
                "orders": 4,
                "gmv": 500000,
                "expense": 150000,
            }
        ],
    }
    breakdown_payload = {
        "error": "",
        "message": "",
        "response": {
            "campaign_list": [
                {
                    "campaign_id": "184826948",
                    "campaign_name": "Product A",
                    "metrics_list": [
                        {
                            "date": "01-03-2026",
                            "impression": 1819,
                            "click": 47,
                            "expense": 84838,
                            "direct_order": 0,
                            "broad_order": 0,
                            "direct_gmv": 0,
                            "broad_gmv": 0,
                        }
                    ],
                }
            ]
        },
    }

    merged = _merge_ads_daily_breakdown_with_shop_total(
        base_payload=base_payload,
        breakdown_payload=breakdown_payload,
    )
    assert isinstance(merged, dict)
    records = merged.get("response", {}).get("records")
    assert isinstance(records, list)
    assert len(records) == 2
    campaign_ids = {str(row.get("campaign_id")) for row in records if isinstance(row, dict)}
    assert "184826948" in campaign_ids
    assert "SHOP_TOTAL" in campaign_ids

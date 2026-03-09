from __future__ import annotations

import json
from datetime import date as date_cls
from pathlib import Path

from dotori_shopee_automation.ads.provider_live_plan import (
    _fetch_campaign_daily_breakdown_payload,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def test_campaign_breakdown_auto_fallback_uses_direct_when_id_list_forbidden(
    tmp_path: Path,
) -> None:
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        fixtures_dir / "campaign_id_list_forbidden_samord.json",
        {
            "error": "forbidden",
            "message": "forbidden",
            "response": {},
        },
    )
    _write_json(
        fixtures_dir / "product_campaign_daily_performance_samord_2026-02-16.json",
        {
            "error": "",
            "message": "",
            "response": [
                {
                    "date": "16-02-2026",
                    "campaign_id": "cmp_a",
                    "campaign_name": "Campaign A",
                    "impression": 1200,
                    "clicks": 40,
                    "direct_order": 1,
                    "broad_order": 1,
                    "direct_gmv": 800000,
                    "broad_gmv": 200000,
                    "expense": 41000,
                },
                {
                    "date": "16-02-2026",
                    "campaign_id": "cmp_b",
                    "campaign_name": "Campaign B",
                    "impression": 980,
                    "clicks": 28,
                    "direct_order": 1,
                    "broad_order": 0,
                    "direct_gmv": 360000,
                    "broad_gmv": 0,
                    "expense": 22000,
                },
            ],
        },
    )

    payload, meta = _fetch_campaign_daily_breakdown_payload(
        client=None,
        shop_key="samord",
        shop_id=497412318,
        access_token=None,
        target_date=date_cls(2026, 2, 16),
        fixtures_dir=fixtures_dir,
        max_campaigns=50,
        chunk_size=50,
        try_alt_endpoints=True,
    )

    assert isinstance(payload, dict)
    assert meta.get("ok") is True
    assert meta.get("selected_endpoint") == "get_product_campaign_daily_performance_direct"
    assert int(meta.get("blocked_403") or 0) == 1

    endpoint_results = meta.get("endpoint_results") or []
    assert len(endpoint_results) >= 2
    assert endpoint_results[0]["endpoint"] == "get_product_campaign_daily_performance_with_id_list"
    assert int(endpoint_results[0]["ok"] or 0) == 0
    assert endpoint_results[1]["endpoint"] == "get_product_campaign_daily_performance_direct"
    assert int(endpoint_results[1]["ok"] or 0) == 1


def test_campaign_breakdown_no_alt_endpoints_stops_after_primary_failure(
    tmp_path: Path,
) -> None:
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        fixtures_dir / "campaign_id_list_forbidden_samord.json",
        {
            "error": "forbidden",
            "message": "forbidden",
            "response": {},
        },
    )

    payload, meta = _fetch_campaign_daily_breakdown_payload(
        client=None,
        shop_key="samord",
        shop_id=497412318,
        access_token=None,
        target_date=date_cls(2026, 2, 16),
        fixtures_dir=fixtures_dir,
        max_campaigns=50,
        chunk_size=50,
        try_alt_endpoints=False,
    )

    assert payload is None
    assert meta.get("ok") is False
    assert meta.get("selected_endpoint") is None
    endpoint_results = meta.get("endpoint_results") or []
    assert len(endpoint_results) == 1
    assert endpoint_results[0]["endpoint"] == "get_product_campaign_daily_performance_with_id_list"
    assert int(endpoint_results[0]["ok"] or 0) == 0

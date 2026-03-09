from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation.ops.phase1_evidence_runner import (
    _build_rate_limit_summary,
    _build_verdict_md,
)


def test_build_rate_limit_summary_reads_trace_rows(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw" / "samord"
    raw_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "error": "ads_rate_limit_total_api",
        "message": "too many requests",
        "request_id": "rid_1",
        "__trace": {
            "path": "/api/v2/ads/get_gms_campaign_performance",
            "method": "GET",
            "called_at_utc": "2026-03-04T03:00:00+00:00",
            "params": {"start_date": "01-03-2026"},
            "http_status": 429,
            "api_error": "ads_rate_limit_total_api",
            "api_message": "too many requests",
            "request_id": "rid_1",
            "retry_after_sec": 120,
            "rate_limited": 1,
            "skipped_by_cooldown": 0,
            "skipped_by_budget": 0,
        },
    }
    (raw_root / "gms_campaign_performance_try_01.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    summary = _build_rate_limit_summary(raw_root.parent, max_rows=50, cooldown_state_path=tmp_path / "rate_limit_state.json")
    assert len(summary["rows"]) == 1
    first = summary["rows"][0]
    assert first["endpoint"] == "/api/v2/ads/get_gms_campaign_performance"
    assert first["http_status"] == 429
    assert first["request_id"] == "rid_1"
    assert summary["http_status_distribution"].get("429") == 1
    assert summary["api_error_distribution"].get("ads_rate_limit_total_api") == 1
    assert summary["retry_after_seen"] == 1
    assert str(summary["cooldown_state_path"]).endswith("rate_limit_state.json")


def test_build_verdict_md_writes_required_sections(tmp_path: Path) -> None:
    verdict_path = tmp_path / "verdict.md"
    shop_results = [
        SimpleNamespace(
            shop_key="samord",
            registry_rows=[
                {
                    "ad_name": "Campaign A",
                    "daily_budget": "120000",
                    "total_budget": "",
                    "item_count": 2,
                }
            ],
            gms_campaign_ids={"g1"},
            preflight_ok=True,
            preflight_reason="",
            meta_probe_ok=True,
            meta_probe_reason="",
            gms_ok=True,
            gms_probe_reason="",
        ),
        SimpleNamespace(
            shop_key="minmin",
            registry_rows=[{"ad_name": "", "daily_budget": "", "total_budget": "", "item_count": 0}],
            gms_campaign_ids=set(),
            preflight_ok=False,
            preflight_reason="token_invalid",
            meta_probe_ok=False,
            meta_probe_reason="preflight_failed",
            gms_ok=False,
            gms_probe_reason="preflight_failed",
        ),
    ]
    rate_summary = {
        "http_status_distribution": {"200": 10, "429": 4},
        "api_error_distribution": {"-": 10, "ads_rate_limit_total_api": 4},
        "retry_after_seen": 1,
        "cooldown_state_path": str(tmp_path / "rate_limit_state.json"),
    }

    _build_verdict_md(
        output_path=verdict_path,
        shop_results=shop_results,
        rate_summary=rate_summary,
    )
    text = verdict_path.read_text(encoding="utf-8")
    assert "Product-level campaign meta" in text
    assert "GMS" in text
    assert "Rate limit" in text
    assert "shop=samord: ad_name=yes" in text
    assert "shop=samord: campaign_budget=yes" in text
    assert "shop=minmin: ad_name=unknown (reason=token_invalid)" in text
    assert "shop=minmin: campaign_id=unknown (reason=token_invalid)" in text
    assert "api_error distribution" in text
    assert "cooldown_state_path" in text
    assert "Retry-After seen? yes" in text

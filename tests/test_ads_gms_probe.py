from __future__ import annotations

import csv
import json
from pathlib import Path

from dotori_shopee_automation.ads.campaign_probe import run_gms_probe
from dotori_shopee_automation.cli import ops_phase1_ads_gms_probe
from dotori_shopee_automation.config import get_settings


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
            ]
        ),
        encoding="utf-8",
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_gms_probe_fixtures_success_registry_and_verdict(tmp_path: Path, monkeypatch) -> None:
    shops_path = tmp_path / "shops.yaml"
    out_dir = tmp_path / "out_success"
    fixture_path = Path("tests/fixtures/gms_performance_success.json")
    _write_shops(shops_path)

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

    ops_phase1_ads_gms_probe(
        only_shops="samord,minmin",
        mode="fixtures",
        days=7,
        out=str(out_dir),
        redact=True,
        json_fixture=str(fixture_path),
        max_gms_calls_per_shop=1,
        force_once=False,
        rate_limit_state=None,
        env_file=None,
    )

    registry_csv = out_dir / "normalized" / "gms_campaign_registry.csv"
    summary_md = out_dir / "summary.md"
    assert registry_csv.exists()
    assert summary_md.exists()

    rows = _read_csv(registry_csv)
    assert len(rows) >= 3
    assert all(str(row.get("campaign_id") or "").strip() for row in rows)
    assert any(str(row.get("campaign_name") or "").strip() for row in rows)
    assert any(str(row.get("daily_budget") or "").strip() for row in rows)

    summary_text = summary_md.read_text(encoding="utf-8")
    assert "gms_campaign_level_supported=yes" in summary_text
    assert "gms_name_supported=yes" in summary_text
    assert "gms_budget_supported=yes" in summary_text


def test_gms_probe_fixtures_rate_limited_verdict_unknown(tmp_path: Path) -> None:
    fixture_path = Path("tests/fixtures/gms_performance_rate_limited.json")
    fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    target_shops = [
        type("S", (), {"shop_key": "samord", "label": "SAMORD", "shopee_shop_id": 497412318})(),
        type("S", (), {"shop_key": "minmin", "label": "MINMIN", "shopee_shop_id": 567655304})(),
    ]
    settings = type(
        "Settings",
        (),
        {"shopee_partner_id": 1, "shopee_partner_key": "k", "shopee_api_host": "https://example.com"},
    )()
    result = run_gms_probe(
        settings=settings,
        target_shops=target_shops,
        mode="fixtures",
        days=7,
        out_dir=tmp_path / "out_rl",
        redact=True,
        fixture_payload=fixture_payload,
        max_gms_calls_per_shop=1,
        force_once=False,
        rate_limit_state_path=tmp_path / "rate_limit_state.json",
    )

    bits = result.get("verdict_bits") or {}
    assert bits.get("gms_campaign_level_supported") == "unknown"
    assert bits.get("gms_name_supported") == "unknown"
    assert bits.get("gms_budget_supported") == "unknown"

    shop_results = result.get("shop_results") or []
    assert len(shop_results) == 2
    assert all(row.rate_limit_hit for row in shop_results)

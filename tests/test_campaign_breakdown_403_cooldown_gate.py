from __future__ import annotations

import json
from datetime import date as date_cls, datetime, timedelta, timezone
from pathlib import Path

import httpx

from dotori_shopee_automation.ads.models import AdsCampaignDaily
from dotori_shopee_automation.ads.provider_live_plan import ingest_ads_live
from dotori_shopee_automation.config import get_settings, load_shops
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.token_store import upsert_token


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 123456",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def test_campaign_breakdown_403_cooldown_gate_skips_breakdown_calls(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "cooldown.db"
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    monkeypatch.setenv("DOTORI_ADS_CAMPAIGN_BREAKDOWN_403_COOLDOWN_HOURS", "24")
    get_settings.cache_clear()
    init_db()

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_live",
                "calls:",
                "  - name: ads_daily",
                "    path: /api/v2/marketing/ads_daily",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )
    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"

    calls = {"id_list": 0, "direct_daily": 0, "all_cpc": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/marketing/ads_daily":
            return httpx.Response(200, json=_load_fixture("ads_daily_shop_level_list_ok.json"))
        if request.url.path == "/api/v2/ads/get_product_level_campaign_id_list":
            calls["id_list"] += 1
            return httpx.Response(403, json={"error": "forbidden", "message": "forbidden"})
        if request.url.path == "/api/v2/ads/get_product_campaign_daily_performance":
            calls["direct_daily"] += 1
            return httpx.Response(403, json={"error": "forbidden", "message": "forbidden"})
        if request.url.path == "/api/v2/ads/get_all_cpc_ads_daily_performance":
            calls["all_cpc"] += 1
            return httpx.Response(403, json={"error": "forbidden", "message": "forbidden"})
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)

    shop_cfg = load_shops()[0]

    with SessionLocal() as session:
        upsert_token(
            session,
            shop_cfg.shop_key,
            shop_cfg.shopee_shop_id or 0,
            "ACCESS_TOKEN",
            "REFRESH_TOKEN",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        session.add(
            EventLog(
                level="INFO",
                message="ads_campaign_breakdown_status",
                meta_json=json.dumps(
                    {
                        "shop_key": shop_cfg.shop_key,
                        "shop_label": shop_cfg.label,
                        "shop_id": shop_cfg.shopee_shop_id,
                        "date": "2026-02-16",
                        "blocked_403": 1,
                        "status": "skipped",
                        "reason": "all_cpc_request_failed",
                        "selected_endpoint": None,
                        "attempted_endpoints": [
                            "get_product_campaign_daily_performance_with_id_list",
                            "get_product_campaign_daily_performance_direct",
                            "get_all_cpc_ads_daily_performance",
                        ],
                        "cooldown_until_utc": (
                            datetime.now(timezone.utc) + timedelta(hours=12)
                        ).isoformat(),
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()

    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-16"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )

    assert result.calls_ok == 1
    assert result.calls_fail == 0
    assert result.campaigns == 1
    assert result.daily == 1
    assert calls == {"id_list": 0, "direct_daily": 0, "all_cpc": 0}

    with SessionLocal() as session:
        daily_rows = session.query(AdsCampaignDaily).all()
        assert len(daily_rows) == 1
        assert daily_rows[0].campaign_id == "SHOP_TOTAL"
        status_rows = (
            session.query(EventLog)
            .filter(EventLog.message == "ads_campaign_breakdown_status")
            .order_by(EventLog.id.desc())
            .all()
        )
        assert status_rows
        assert any('"status": "cooldown_skip"' in (row.meta_json or "") for row in status_rows)

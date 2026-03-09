from __future__ import annotations

from datetime import datetime, timedelta, timezone, date as date_cls
from pathlib import Path
import json

import httpx

from dotori_shopee_automation.ads.provider_live_plan import ingest_ads_live
from dotori_shopee_automation.ads.models import AdsCampaign
from dotori_shopee_automation.config import get_settings, load_shops
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.token_store import upsert_token


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_shops(path: Path, shops: list[dict]) -> None:
    lines: list[str] = []
    for shop in shops:
        lines.append(f"- shop_key: {shop['shop_key']}")
        lines.append(f"  label: {shop['label']}")
        lines.append(f"  enabled: {str(shop.get('enabled', True)).lower()}")
        if "shopee_shop_id" in shop:
            lines.append(f"  shopee_shop_id: {shop['shopee_shop_id']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def _setup_env(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "ads_live.db"
    shops_path = tmp_path / "shops.yaml"
    _write_shops(
        shops_path,
        [
            {
                "shop_key": "samord",
                "label": "SAMORD",
                "enabled": True,
                "shopee_shop_id": 123456,
            }
        ],
    )
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    get_settings.cache_clear()
    init_db()


def _parse_campaign_id_list(value: str) -> list[str]:
    s = (value or "").strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        decoded = json.loads(s)
        if isinstance(decoded, list):
            return [str(x) for x in decoded]
    if "," in s:
        return [part.strip() for part in s.split(",") if part.strip()]
    return [s]


def test_campaign_setting_info_chunking_merges_records(monkeypatch, tmp_path, capsys) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.setenv("ADS_CAMPAIGN_LIST_MAX_IDS", "200")

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_live",
                "calls:",
                "  - name: ads_campaign_list",
                "    path: /api/v2/ads/get_product_level_campaign_setting_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    ids = [str(i) for i in range(1, 121)]  # 120 ids -> 3 chunks (50/50/20)
    setting_calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/ads/get_product_level_campaign_id_list":
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "message": "",
                    "response": {"campaign_id_list": ids},
                },
            )
        if request.url.path == "/api/v2/ads/get_product_level_campaign_setting_info":
            raw = request.url.params.get("campaign_id_list") or ""
            requested = _parse_campaign_id_list(str(raw))
            setting_calls.append(requested)
            # Chunk size must remain 50.
            assert len(requested) <= 50
            records = [
                {
                    "campaign_id": cid,
                    "campaign_name": f"C{cid}",
                    "status": "ongoing",
                    "daily_budget": "100.00",
                }
                for cid in requested
            ]
            return httpx.Response(
                200,
                json={"error": 0, "message": "", "response": {"records": records}},
            )
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
        session.commit()

    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
    ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )

    out = capsys.readouterr().out
    assert "campaign_setting_info_chunks" in out
    assert "chunks=3" in out
    assert "chunk_size=50" in out
    assert len(setting_calls) == 3

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 120


from __future__ import annotations

import json
from pathlib import Path

from dotori_shopee_automation.ads.campaign_probe import run_gms_probe
from dotori_shopee_automation.ads.models import Phase1AdsGmsCampaignRegistry
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


def test_ads_gms_ingest_smoke(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "gms_ingest.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    get_settings.cache_clear()
    init_db()

    fixture_payload = json.loads(
        Path("tests/fixtures/gms_performance_success.json").read_text(encoding="utf-8")
    )
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
        out_dir=tmp_path / "out",
        redact=True,
        fixture_payload=fixture_payload,
        max_gms_calls_per_shop=1,
        force_once=False,
        sync_db=True,
        rate_limit_state_path=tmp_path / "rate_limit_state.json",
    )
    assert int(result.get("db_upserted") or 0) >= 3

    with SessionLocal() as session:
        rows = session.query(Phase1AdsGmsCampaignRegistry).all()
    assert len(rows) >= 3
    assert any((row.campaign_name or "").strip() for row in rows)
    assert any(row.daily_budget is not None for row in rows)

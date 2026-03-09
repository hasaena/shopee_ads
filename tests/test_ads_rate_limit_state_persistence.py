from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation import cli
from dotori_shopee_automation.ads.campaign_probe import run_campaign_probe
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token


def _latest_preflight_trace(out_dir: Path, shop_key: str) -> tuple[dict, dict]:
    files = sorted((out_dir / "raw" / shop_key).glob("preflight_*.json"))
    assert files
    payload = json.loads(files[-1].read_text(encoding="utf-8"))
    trace = payload.get("__trace")
    assert isinstance(trace, dict)
    return payload, trace


def _write_phase1_shops(path: Path) -> None:
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


def test_campaign_probe_rate_limit_state_persists_across_runs(tmp_path: Path) -> None:
    state_path = tmp_path / "shared_ads_rate_limit_state.json"
    settings = SimpleNamespace()
    target_shops = [
        SimpleNamespace(shop_key="samord", label="SAMORD", enabled=True, shopee_shop_id=497412318),
        SimpleNamespace(shop_key="minmin", label="MINMIN", enabled=True, shopee_shop_id=567655304),
    ]

    run1 = tmp_path / "run1"
    result1 = run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode="fixtures",
        days=7,
        out_dir=run1,
        redact=True,
        fixture_payload=None,
        max_requests_per_shop=3,
        sync_db=False,
        ignore_cooldown=False,
        rate_limit_state_path=state_path,
    )
    assert result1["rate_limit_state_path"] == str(state_path)
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "samord" in (state.get("shops") or {})
    assert "minmin" in (state.get("shops") or {})

    _, trace1 = _latest_preflight_trace(run1, "samord")
    assert int(trace1.get("rate_limited") or 0) == 1
    assert int(trace1.get("skipped_by_cooldown") or 0) == 0

    run2 = tmp_path / "run2"
    run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode="fixtures",
        days=7,
        out_dir=run2,
        redact=True,
        fixture_payload=None,
        max_requests_per_shop=3,
        sync_db=False,
        ignore_cooldown=False,
        rate_limit_state_path=state_path,
    )
    _, trace2 = _latest_preflight_trace(run2, "samord")
    assert int(trace2.get("skipped_by_cooldown") or 0) == 1
    assert str(trace2.get("api_error") or "") == "local_rate_limited"


def test_status_dump_payload_includes_ads_rate_limit_state(monkeypatch, tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    reports_dir = tmp_path / "reports"
    db_path = tmp_path / "status.db"
    rate_limit_state_path = tmp_path / "ads_rate_limit_state.json"
    reports_dir.mkdir(parents=True, exist_ok=True)
    _write_phase1_shops(shops_path)

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("DOTORI_ADS_RATE_LIMIT_STATE_PATH", str(rate_limit_state_path))
    get_settings.cache_clear()

    now = datetime.now(timezone.utc)
    init_db()
    session = SessionLocal()
    try:
        upsert_token(
            session,
            "samord",
            497412318,
            "A" * 40,
            "R" * 40,
            now + timedelta(hours=2),
            now + timedelta(days=30),
        )
        upsert_token(
            session,
            "minmin",
            567655304,
            "B" * 40,
            "S" * 40,
            now + timedelta(hours=2),
            now + timedelta(days=30),
        )
        session.commit()
    finally:
        session.close()

    state_payload = {
        "shops": {
            "samord": {
                "cooldown_until_utc": (now + timedelta(minutes=30)).isoformat(),
                "last_rate_limited_at_utc": now.isoformat(),
                "last_http_status": 403,
                "last_error": "ads_rate_limit_total_api",
            },
            "minmin": {
                "cooldown_until_utc": (now - timedelta(minutes=1)).isoformat(),
                "last_rate_limited_at_utc": (now - timedelta(minutes=10)).isoformat(),
                "last_http_status": 429,
                "last_error": "error_rate_limit",
            },
        }
    }
    rate_limit_state_path.write_text(
        json.dumps(state_payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    payload = cli._build_phase1_status_payload_for_cli(
        shops_value="samord,minmin",
        reports_dir=str(reports_dir),
    )
    ads_rate_limit = payload.get("ads_rate_limit")
    assert isinstance(ads_rate_limit, dict)
    assert sorted(ads_rate_limit.keys()) == ["minmin", "samord"]
    assert ads_rate_limit["samord"]["cooldown_active"] is True
    assert ads_rate_limit["samord"]["last_api_error"] == "ads_rate_limit_total_api"
    assert ads_rate_limit["samord"]["last_http_status"] == 403
    assert ads_rate_limit["samord"]["state_path"] == str(rate_limit_state_path)
    assert ads_rate_limit["minmin"]["cooldown_active"] is False
    ads_rate_limit_config = payload.get("ads_rate_limit_config") or {}
    assert ads_rate_limit_config["state_path_effective"] == str(rate_limit_state_path)
    assert ads_rate_limit_config["state_path_source"] == "env"
    assert ads_rate_limit_config["parent_dir_exists"] is True
    assert ads_rate_limit_config["parent_dir_writable"] is True

    get_settings.cache_clear()

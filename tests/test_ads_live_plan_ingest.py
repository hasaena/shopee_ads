from datetime import datetime, timedelta, timezone, date as date_cls
from pathlib import Path
import json

import httpx
import pytest
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.ads.provider_live_plan import ingest_ads_live
from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.config import get_settings, load_shops
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.token_store import upsert_token


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "shopee_ads"
runner = CliRunner()


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _write_shops(path: Path, shops: list[dict]) -> None:
    lines: list[str] = []
    for shop in shops:
        lines.append(f"- shop_key: {shop['shop_key']}")
        lines.append(f"  label: {shop['label']}")
        lines.append(f"  enabled: {str(shop.get('enabled', True)).lower()}")
        if "shopee_shop_id" in shop:
            lines.append(f"  shopee_shop_id: {shop['shopee_shop_id']}")
    path.write_text("\n".join(lines), encoding="utf-8")


def _setup_env(tmp_path, monkeypatch, shops: list[dict] | None = None) -> None:
    db_path = tmp_path / "ads_live.db"
    shops_path = tmp_path / "shops.yaml"
    if shops is None:
        shops = [
            {
                "shop_key": "samord",
                "label": "SAMORD",
                "enabled": True,
                "shopee_shop_id": 123456,
            }
        ]
    _write_shops(shops_path, shops)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    get_settings.cache_clear()
    init_db()


def test_ads_live_ingest_idempotent(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
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
                "  - name: ads_snapshot",
                "    path: /api/v2/marketing/ads_snapshot",
                "    params: {}",
                "  - name: ads_denied",
                "    path: /api/v2/marketing/ads_denied",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/marketing/ads_daily":
            return httpx.Response(200, json=_load_fixture("ads_daily_ok.json"))
        if request.url.path == "/api/v2/marketing/ads_snapshot":
            return httpx.Response(200, json=_load_fixture("ads_snapshot_ok.json"))
        if request.url.path == "/api/v2/marketing/ads_denied":
            return httpx.Response(200, json=_load_fixture("ads_denied.json"))
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)

    shops = load_shops()
    shop_cfg = shops[0]

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

    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )
    assert result.calls_ok == 2
    assert result.calls_fail == 1
    assert result.campaigns == 2
    assert result.daily == 2
    assert result.snapshots == 1

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2
        assert session.query(AdsCampaignSnapshot).count() == 1

    result_again = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )
    assert result_again.campaigns == 2
    assert result_again.daily == 2
    assert result_again.snapshots == 1

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2
        assert session.query(AdsCampaignSnapshot).count() == 1


def test_ads_live_ingest_shop_level_daily_list_injects_shop_total_idempotent(
    monkeypatch, tmp_path
) -> None:
    _setup_env(tmp_path, monkeypatch)
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

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/marketing/ads_daily":
            return httpx.Response(200, json=_load_fixture("ads_daily_shop_level_list_ok.json"))
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    shops = load_shops()
    shop_cfg = shops[0]

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

    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
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
    assert result.snapshots == 0

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 1
        assert session.query(AdsCampaignDaily).count() == 1
        daily_row = session.query(AdsCampaignDaily).one()
        assert daily_row.campaign_id == "SHOP_TOTAL"

    result_again = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )
    assert result_again.calls_ok == 1
    assert result_again.calls_fail == 0
    assert result_again.campaigns == 1
    assert result_again.daily == 1
    assert result_again.snapshots == 0

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 1
        assert session.query(AdsCampaignDaily).count() == 1


def test_ads_live_ingest_daily_rows_align_to_target_date(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
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

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/marketing/ads_daily":
            return httpx.Response(200, json=_load_fixture("ads_daily_ok.json"))
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    shops = load_shops()
    shop_cfg = shops[0]

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
    target_day = date_cls.fromisoformat("2026-02-25")
    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=target_day,
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        client_factory=lambda _: client,
    )
    assert result.calls_ok == 1
    assert result.daily == 2

    with SessionLocal() as session:
        rows = (
            session.query(AdsCampaignDaily)
            .order_by(AdsCampaignDaily.campaign_id.asc())
            .all()
        )
        assert len(rows) == 2
        assert all(row.date == target_day for row in rows)


def test_ads_live_ingest_campaign_breakdown_403_softfail_keeps_shop_total(
    monkeypatch, tmp_path
) -> None:
    _setup_env(tmp_path, monkeypatch)
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

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/marketing/ads_daily":
            return httpx.Response(200, json=_load_fixture("ads_daily_shop_level_list_ok.json"))
        if request.url.path in (
            "/api/v2/ads/get_product_level_campaign_id_list",
            "/api/v2/ads/get_product_campaign_daily_performance",
            "/api/v2/ads/get_all_cpc_ads_daily_performance",
        ):
            return httpx.Response(403, json={"error": "forbidden", "message": "forbidden"})
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    shops = load_shops()
    shop_cfg = shops[0]

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
    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
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

    with SessionLocal() as session:
        daily_rows = session.query(AdsCampaignDaily).all()
        assert len(daily_rows) == 1
        assert daily_rows[0].campaign_id == "SHOP_TOTAL"
        status_rows = (
            session.query(EventLog)
            .filter(EventLog.message == "ads_campaign_breakdown_status")
            .all()
        )
        assert status_rows
        assert any('"blocked_403": 1' in (row.meta_json or "") for row in status_rows)


def test_ads_live_ingest_snapshot_perf_injects_shop_total_uppercase(
    monkeypatch, tmp_path
) -> None:
    _setup_env(tmp_path, monkeypatch)
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_live",
                "calls:",
                "  - name: ads_snapshot",
                "    path: /api/v2/ads/get_all_cpc_ads_daily_performance",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

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

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/ads/get_all_cpc_ads_daily_performance":
            return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    shops = load_shops()
    shop_cfg = shops[0]

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
    assert result.daily == 0
    assert result.snapshots == 1

    with SessionLocal() as session:
        assert session.query(AdsCampaignSnapshot).count() == 1
        snapshot_row = session.query(AdsCampaignSnapshot).one()
        assert snapshot_row.campaign_id == "SHOP_TOTAL"
        assert (
            session.query(AdsCampaign)
            .filter_by(campaign_id="shop_total")
            .count()
            == 0
        )
        assert (
            session.query(AdsCampaign)
            .filter_by(campaign_id="SHOP_TOTAL")
            .count()
            == 1
        )


def test_dry_run_no_transport_no_db(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
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
    shops = load_shops()
    shop_cfg = shops[0]

    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=True,
        client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
    )
    assert result.calls_ok == 0
    assert result.calls_fail == 0

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 0
        assert session.query(AdsCampaignDaily).count() == 0
        assert session.query(AdsCampaignSnapshot).count() == 0


def test_dry_run_prints_planned_calls_and_coverage(monkeypatch, tmp_path, capsys) -> None:
    _setup_env(tmp_path, monkeypatch)
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_live",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
                "  - name: ads_daily",
                "    path: /api/v2/marketing/ads_daily",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
    shops = load_shops()
    shop_cfg = shops[0]

    ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=True,
        client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
    )

    out = capsys.readouterr().out
    assert "planned_calls: shop_info, ads_daily" in out
    assert "mapping_coverage:" in out
    assert "missing=[]" in out


def test_strict_mapping_fails_without_network_or_db_writes(monkeypatch, tmp_path, capsys) -> None:
    _setup_env(tmp_path, monkeypatch)
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_live",
                "calls:",
                "  - name: unmapped_call",
                "    path: /api/v2/marketing/unmapped",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
    shops = load_shops()
    shop_cfg = shops[0]

    with pytest.raises(ValueError):
        ingest_ads_live(
            shop_cfg=shop_cfg,
            settings=get_settings(),
            target_date=date_cls.fromisoformat("2026-02-03"),
            plan_path=plan_path,
            mapping_path=mapping_path,
            save_artifacts=False,
            dry_run=True,
            strict_mapping=True,
            client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
        )

    out = capsys.readouterr().out
    assert "strict_mapping_missing" in out

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 0
        assert session.query(AdsCampaignDaily).count() == 0
        assert session.query(AdsCampaignSnapshot).count() == 0


def test_ingest_all_only_shops_two_shops(monkeypatch, tmp_path) -> None:
    _setup_env(
        tmp_path,
        monkeypatch,
        shops=[
            {
                "shop_key": "samord",
                "label": "SAMORD",
                "enabled": True,
                "shopee_shop_id": 111,
            },
            {
                "shop_key": "minmin",
                "label": "MINMIN",
                "enabled": True,
                "shopee_shop_id": 222,
            },
            {
                "shop_key": "other",
                "label": "OTHER",
                "enabled": True,
                "shopee_shop_id": 333,
            },
        ],
    )
    monkeypatch.setattr(
        cli_module,
        "_build_shopee_client",
        lambda settings: (_ for _ in ()).throw(RuntimeError("should not call")),
    )

    plan_path = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"
    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"

    result = runner.invoke(
        app,
        [
            "ads",
            "live",
            "ingest-all",
            "--date",
            "2026-02-03",
            "--only-shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--fixtures-dir",
            str(FIXTURE_ROOT),
            "--plan",
            str(plan_path),
            "--mapping",
            str(mapping_path),
        ],
    )
    assert result.exit_code == 0
    assert "shop=samord" in result.stdout
    assert "shop=minmin" in result.stdout
    assert "shop=other" not in result.stdout

    with SessionLocal() as session:
        assert session.query(AdsCampaign).filter_by(shop_key="samord").count() > 0
        assert session.query(AdsCampaign).filter_by(shop_key="minmin").count() > 0
        assert session.query(AdsCampaign).filter_by(shop_key="other").count() == 0
        assert session.query(AdsCampaignDaily).filter_by(shop_key="samord").count() > 0
        assert session.query(AdsCampaignDaily).filter_by(shop_key="minmin").count() > 0
        assert session.query(AdsCampaignDaily).filter_by(shop_key="other").count() == 0


def test_save_artifacts_redaction_in_fixtures_mode(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    fake_payload = _load_fixture("ads_daily_ok_with_fake_secrets.json")
    (fixtures_dir / "ads_daily.json").write_text(
        json.dumps(fake_payload),
        encoding="utf-8",
    )

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
    shops = load_shops()
    shop_cfg = shops[0]
    save_root = tmp_path / "artifacts"

    ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=True,
        dry_run=False,
        fixtures_dir=fixtures_dir,
        save_root=save_root,
        client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
    )

    artifact_files = list(save_root.rglob("*.json"))
    assert artifact_files
    contents = "\n".join(
        path.read_text(encoding="utf-8") for path in artifact_files
    )
    assert "FAKE_ACCESS_TOKEN_SHOULD_BE_REDACTED" not in contents
    assert "FAKE_BEARER_TOKEN_SHOULD_BE_REDACTED" not in contents
    assert "FAKE_PARTNER_KEY_SHOULD_BE_REDACTED" not in contents


def test_ads_live_ingest_fixtures_minimal_plan_snapshot_idempotent(
    monkeypatch, tmp_path
) -> None:
    _setup_env(tmp_path, monkeypatch)
    plan_path = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"
    mapping_path = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
    shops = load_shops()
    shop_cfg = shops[0]

    result = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        fixtures_dir=FIXTURE_ROOT,
        client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
    )
    assert result.calls_ok == 3
    assert result.calls_fail == 0
    assert result.campaigns == 2
    assert result.daily == 2
    assert result.snapshots == 2

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2
        assert session.query(AdsCampaignSnapshot).count() == 2

    result_again = ingest_ads_live(
        shop_cfg=shop_cfg,
        settings=get_settings(),
        target_date=date_cls.fromisoformat("2026-02-03"),
        plan_path=plan_path,
        mapping_path=mapping_path,
        save_artifacts=False,
        dry_run=False,
        fixtures_dir=FIXTURE_ROOT,
        client_factory=lambda _: (_ for _ in ()).throw(RuntimeError("should not call")),
    )
    assert result_again.calls_ok == 3
    assert result_again.calls_fail == 0
    assert result_again.campaigns == 2
    assert result_again.daily == 2
    assert result_again.snapshots == 2

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2
        assert session.query(AdsCampaignSnapshot).count() == 2

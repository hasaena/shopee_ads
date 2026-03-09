from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily
from dotori_shopee_automation.ads.reporting import render_daily_html
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import Base


def _seed_db(db_path: Path) -> None:
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)
    Base.metadata.create_all(bind=engine)
    with Session(engine) as session:
        session.add(
            AdsCampaign(
                shop_key="minmin",
                campaign_id="SHOP_TOTAL",
                campaign_name="SHOP_TOTAL",
                status="on",
                daily_budget=Decimal("300000"),
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="minmin",
                campaign_id="SHOP_TOTAL",
                date=date(2026, 3, 5),
                spend=Decimal("200000"),
                impressions=2500,
                clicks=120,
                orders=4,
                gmv=Decimal("1400000"),
            )
        )
        session.commit()


def _write_rendered_report(path: Path) -> None:
    data = {
        "shop_key": "minmin",
        "shop_label": "MINMIN",
        "date": date(2026, 3, 5),
        "kind": "final",
        "generated_at": None,
        "as_of": None,
        "totals": {
            "spend": Decimal("200000"),
            "impressions": 2500,
            "clicks": 120,
            "orders": 3,  # intentionally mismatched vs DB
            "gmv": Decimal("1200000"),  # intentionally mismatched vs DB
        },
        "kpis": {},
        "scorecard": {
            "spend": Decimal("200000"),
            "impressions": 2500,
            "clicks": 120,
            "orders": 3,
            "gmv": Decimal("1200000"),
            "roas": Decimal("6"),
            "ctr": Decimal("0.048"),
            "cpc": Decimal("1666.67"),
            "cvr": Decimal("0.025"),
        },
        "campaign_performance": [],
        "campaign_performance_total": 0,
        "campaign_breakdown_note": "fixture",
        "snapshot_fallback": {"used": 0, "rows": []},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_daily_html(data), encoding="utf-8")


def _write_raw_fixture(raw_root: Path) -> None:
    target = raw_root / "sample_run" / "raw" / "minmin" / "ads_daily_payload_2026-03-05.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "shop_key": "minmin",
        "date": "2026-03-05",
        "rows": [
            {
                "campaign_id": "SHOP_TOTAL",
                "spend": 200000,
                "impressions": 2500,
                "clicks": 120,
                "orders": 4,
                "gmv": 1400000,
            }
        ],
    }
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_report_reconcile_cli_outputs_md_and_json(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "reconcile.db"
    reports_dir = tmp_path / "reports"
    artifacts_dir = tmp_path / "artifacts_out"
    raw_root = tmp_path / "raw_artifacts"
    _seed_db(db_path)
    _write_rendered_report(reports_dir / "minmin" / "daily" / "2026-03-05_final.html")
    _write_raw_fixture(raw_root)

    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", "config/shops.yaml")
    get_settings.cache_clear()

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "report",
        "reconcile",
        "--shop",
        "minmin",
        "--kind",
        "final",
        "--date",
        "2026-03-05",
        "--db",
        str(db_path),
        "--reports-dir",
        str(reports_dir),
        "--artifacts-dir",
        str(artifacts_dir),
        "--raw-artifacts-root",
        str(raw_root),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "reconcile_done shop=minmin kind=final date=2026-03-05" in result.stdout

    json_path = artifacts_dir / "reconcile_minmin_final_2026-03-05.json"
    md_path = artifacts_dir / "reconcile_minmin_final_2026-03-05.md"
    assert json_path.exists()
    assert md_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["shop"] == "minmin"
    assert payload["kind"] == "final"
    assert payload["report_exists"] == 1
    by_metric = {row["metric"]: row for row in payload["comparison"]}
    assert by_metric["spend"]["db_aggregated_value"] == "200000"
    assert by_metric["gmv"]["db_aggregated_value"] == "1400000"
    assert by_metric["gmv"]["rendered_value"] == "1200000"


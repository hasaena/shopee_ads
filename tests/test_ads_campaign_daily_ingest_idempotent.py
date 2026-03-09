from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOKEN_FILE = (
    REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"
)


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 497412318",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 567655304",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def test_campaign_daily_breakdown_ingest_idempotent(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "task086.db"
    reports_dir = tmp_path / "reports"
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        fixtures_dir / "shop_info.json",
        {"error": 0, "response": {"shop_name": "fixture"}},
    )
    _write_json(
        fixtures_dir / "ads_daily.json",
        {
            "error": "",
            "message": "",
            "response": [
                {
                    "date": "16-02-2026",
                    "impression": 999,
                    "clicks": 22,
                    "direct_order": 0,
                    "broad_order": 0,
                    "direct_gmv": 0,
                    "broad_gmv": 0,
                    "expense": 12345,
                }
            ],
        },
    )
    _write_json(
        fixtures_dir / "campaign_id_list_samord.json",
        {
            "error": "",
            "message": "",
            "response": {
                "campaign_list": [
                    {"campaign_id": "s_cmp_1", "campaign_name": "Samord Hero"},
                    {"campaign_id": "s_cmp_2", "campaign_name": "Samord Search"},
                    {"campaign_id": "s_cmp_3", "campaign_name": "Samord Retarget"},
                ]
            },
        },
    )
    _write_json(
        fixtures_dir / "campaign_id_list_minmin.json",
        {
            "error": "",
            "message": "",
            "response": {
                "campaign_list": [
                    {"campaign_id": "m_cmp_1", "campaign_name": "Minmin Hero"},
                    {"campaign_id": "m_cmp_2", "campaign_name": "Minmin Search"},
                    {"campaign_id": "m_cmp_3", "campaign_name": "Minmin Retarget"},
                ]
            },
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
                    "campaign_id": "s_cmp_1",
                    "campaign_name": "Samord Hero",
                    "impression": 1200,
                    "clicks": 30,
                    "direct_order": 1,
                    "broad_order": 1,
                    "direct_gmv": 500000,
                    "broad_gmv": 320000,
                    "expense": 42000,
                },
                {
                    "date": "16-02-2026",
                    "campaign_id": "s_cmp_2",
                    "campaign_name": "Samord Search",
                    "impression": 900,
                    "clicks": 24,
                    "direct_order": 1,
                    "broad_order": 0,
                    "direct_gmv": 250000,
                    "broad_gmv": 0,
                    "expense": 22000,
                },
                {
                    "date": "16-02-2026",
                    "campaign_id": "s_cmp_3",
                    "campaign_name": "Samord Retarget",
                    "impression": 700,
                    "clicks": 18,
                    "direct_order": 0,
                    "broad_order": 1,
                    "direct_gmv": 0,
                    "broad_gmv": 210000,
                    "expense": 14000,
                },
            ],
        },
    )
    _write_json(
        fixtures_dir / "product_campaign_daily_performance_minmin_2026-02-16.json",
        {
            "error": "",
            "message": "",
            "response": [
                {
                    "date": "16-02-2026",
                    "campaign_id": "m_cmp_1",
                    "campaign_name": "Minmin Hero",
                    "impression": 1100,
                    "clicks": 28,
                    "direct_order": 1,
                    "broad_order": 1,
                    "direct_gmv": 360000,
                    "broad_gmv": 290000,
                    "expense": 36000,
                },
                {
                    "date": "16-02-2026",
                    "campaign_id": "m_cmp_2",
                    "campaign_name": "Minmin Search",
                    "impression": 820,
                    "clicks": 21,
                    "direct_order": 0,
                    "broad_order": 1,
                    "direct_gmv": 0,
                    "broad_gmv": 170000,
                    "expense": 20000,
                },
                {
                    "date": "16-02-2026",
                    "campaign_id": "m_cmp_3",
                    "campaign_name": "Minmin Retarget",
                    "impression": 620,
                    "clicks": 16,
                    "direct_order": 0,
                    "broad_order": 0,
                    "direct_gmv": 0,
                    "broad_gmv": 0,
                    "expense": 9000,
                },
            ],
        },
    )

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "schedule",
        "run-once",
        "--job",
        "daily-final",
        "--date",
        "2026-02-17",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--token-file",
        str(TOKEN_FILE),
        "--fixtures-dir",
        str(fixtures_dir),
        "--plan",
        "collaboration/plans/ads_ingest_daily_final.yaml",
        "--mapping",
        "collaboration/mappings/ads_mapping.yaml",
        "--reports-dir",
        str(reports_dir),
        "--no-send-discord",
    ]

    first = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert "phase1_schedule_run_once_ok=1" in first.stdout
    # 3 campaign rows + 1 SHOP_TOTAL row per shop.
    assert _count_rows(db_path, "ads_campaign_daily") == 8

    second = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert "phase1_schedule_run_once_ok=1" in second.stdout
    assert _count_rows(db_path, "ads_campaign_daily") == 8

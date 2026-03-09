from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 111",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 222",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_ops_phase1_reports_doctor(tmp_path: Path) -> None:
    html_path = tmp_path / "sample.html"
    html_path.write_text(
        "\n".join(
            [
                "<!doctype html>",
                "<html>",
                "<head>",
                "<meta charset='utf-8'>",
                "<title>Sample Report</title>",
                "<style>body{font-family:Arial}</style>",
                "</head>",
                "<body>",
                "<h1>Report</h1>",
                "<table><tr><th>A</th><td>1</td></tr></table>",
                "</body>",
                "</html>",
            ]
        ),
        encoding="utf-8",
    )

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "reports",
        "doctor",
        "--path",
        str(html_path),
    ]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "report_doctor " in result.stdout
    assert "meta_charset_ok=1" in result.stdout
    assert "report_doctor_ok=1" in result.stdout


def test_ops_phase1_reports_find_nonzero_day(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "phase1.db"

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE ads_campaign_snapshot (
                id INTEGER PRIMARY KEY,
                shop_key TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                spend_today NUMERIC NOT NULL,
                impressions_today INTEGER NOT NULL,
                clicks_today INTEGER NOT NULL,
                orders_today INTEGER NOT NULL,
                gmv_today NUMERIC NOT NULL
            );
            CREATE TABLE ads_campaign_daily (
                id INTEGER PRIMARY KEY,
                shop_key TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                date TEXT NOT NULL,
                spend NUMERIC NOT NULL,
                impressions INTEGER NOT NULL,
                clicks INTEGER NOT NULL,
                orders INTEGER NOT NULL,
                gmv NUMERIC NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO ads_campaign_snapshot
            (shop_key, campaign_id, ts, spend_today, impressions_today, clicks_today, orders_today, gmv_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("samord", "SHOP_TOTAL", "2026-02-20T12:00:00+07:00", 120.0, 1000, 40, 2, 400.0),
        )
        conn.execute(
            """
            INSERT INTO ads_campaign_snapshot
            (shop_key, campaign_id, ts, spend_today, impressions_today, clicks_today, orders_today, gmv_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("minmin", "SHOP_TOTAL", "2026-02-19T12:00:00+07:00", 90.0, 800, 30, 1, 220.0),
        )
        conn.execute(
            """
            INSERT INTO ads_campaign_daily
            (shop_key, campaign_id, date, spend, impressions, clicks, orders, gmv)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("samord", "SHOP_TOTAL", "2026-02-20", 120.0, 1000, 40, 2, 400.0),
        )
        conn.commit()
    finally:
        conn.close()

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "reports",
        "find-nonzero-day",
        "--shops",
        "samord,minmin",
        "--lookback-days",
        "14",
    ]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "nonzero_day_found shop=samord date=2026-02-20" in result.stdout
    assert "source=daily" in result.stdout
    assert "nonzero_day_found shop=minmin date=2026-02-19" in result.stdout
    assert "source=snapshot" in result.stdout
    assert "nonzero_day_scan_ok=1" in result.stdout

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import init_db


REPO_ROOT = Path(__file__).resolve().parents[1]


def _init_db_file(db_url: str) -> Path:
    prev_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url
    get_settings.cache_clear()
    init_db()
    try:
        path = Path(get_settings().database_url.replace("sqlite:///", ""))
    finally:
        if prev_db is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = prev_db
        get_settings.cache_clear()
    return path


def _seed_legacy_rows(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ads_campaign (shop_key, campaign_id, campaign_name, status, daily_budget)
            VALUES
            ('samord', 'SHOP_TOTAL', 'SHOP_TOTAL', NULL, NULL),
            ('samord', 'shop_total', 'legacy lower', NULL, NULL),
            ('minmin', 'Shop_Total', 'legacy mixed', NULL, NULL),
            ('samord', 'cmp_1', 'Campaign 1', NULL, NULL)
            """
        )
        cur.execute(
            """
            INSERT INTO ads_campaign_daily (shop_key, campaign_id, date, spend, impressions, clicks, orders, gmv)
            VALUES
            ('samord', 'SHOP_TOTAL', '2026-02-19', 1, 1, 1, 1, 1),
            ('samord', 'shop_total', '2026-02-19', 2, 2, 2, 2, 2),
            ('minmin', 'Shop_Total', '2026-02-19', 3, 3, 3, 3, 3),
            ('samord', 'cmp_1', '2026-02-19', 4, 4, 4, 4, 4)
            """
        )
        cur.execute(
            """
            INSERT INTO ads_campaign_snapshot (
              shop_key, campaign_id, ts, spend_today, impressions_today, clicks_today, orders_today, gmv_today
            )
            VALUES
            ('samord', 'SHOP_TOTAL', '2026-02-19T13:00:00+07:00', 1, 1, 1, 1, 1),
            ('samord', 'shop_total', '2026-02-19T13:00:00+07:00', 2, 2, 2, 2, 2),
            ('minmin', 'Shop_Total', '2026-02-19T13:00:00+07:00', 3, 3, 3, 3, 3)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _count(conn: sqlite3.Connection, sql: str) -> int:
    cur = conn.execute(sql)
    row = cur.fetchone()
    return int(row[0] if row else 0)


def _run_normalize(db_url: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "db",
        "normalize-shop-total",
    ]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_ops_phase1_db_normalize_shop_total_cleanup_and_idempotent(tmp_path: Path) -> None:
    db_url = f"sqlite:///{(tmp_path / 'legacy.db').as_posix()}"
    db_path = _init_db_file(db_url)
    _seed_legacy_rows(db_path)

    first = _run_normalize(db_url)
    assert first.returncode == 0, first.stdout + first.stderr
    stdout = first.stdout
    assert "normalize_shop_total_ok=1" in stdout
    assert "after ads_campaign_shop_total_lower=0" in stdout
    assert "daily_shop_total_lower=0" in stdout
    assert "snapshot_shop_total_lower=0" in stdout
    m_updated = re.search(r"rows_updated_total=(\d+)", stdout)
    m_deleted = re.search(r"rows_deleted_total=(\d+)", stdout)
    assert m_updated is not None
    assert m_deleted is not None
    assert int(m_updated.group(1)) > 0
    assert int(m_deleted.group(1)) > 0

    conn = sqlite3.connect(str(db_path))
    try:
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign WHERE lower(trim(campaign_id))='shop_total' AND trim(campaign_id)<>'SHOP_TOTAL'") == 0
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_daily WHERE lower(trim(campaign_id))='shop_total' AND trim(campaign_id)<>'SHOP_TOTAL'") == 0
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_snapshot WHERE lower(trim(campaign_id))='shop_total' AND trim(campaign_id)<>'SHOP_TOTAL'") == 0

        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign WHERE campaign_id='SHOP_TOTAL'") == 2
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_daily WHERE campaign_id='SHOP_TOTAL'") == 2
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_snapshot WHERE campaign_id='SHOP_TOTAL'") == 2

        # Duplicate conflict case is resolved: samord keeps only a single SHOP_TOTAL row per key.
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign WHERE shop_key='samord' AND lower(trim(campaign_id))='shop_total'") == 1
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_daily WHERE shop_key='samord' AND date='2026-02-19' AND lower(trim(campaign_id))='shop_total'") == 1
        assert _count(conn, "SELECT COUNT(*) FROM ads_campaign_snapshot WHERE shop_key='samord' AND ts='2026-02-19T13:00:00+07:00' AND lower(trim(campaign_id))='shop_total'") == 1
    finally:
        conn.close()

    second = _run_normalize(db_url)
    assert second.returncode == 0, second.stdout + second.stderr
    stdout2 = second.stdout
    assert "normalize_shop_total_ok=1" in stdout2
    assert "rows_updated_total=0" in stdout2
    assert "rows_deleted_total=0" in stdout2

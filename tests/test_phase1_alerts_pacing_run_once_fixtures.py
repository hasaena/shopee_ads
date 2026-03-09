from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_OPEN = REPO_ROOT / "tests" / "fixtures" / "shopee_ads_alerts_pacing" / "open"
FIXTURES_RESOLVED = (
    REPO_ROOT / "tests" / "fixtures" / "shopee_ads_alerts_pacing" / "resolved"
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


def _count_rows(db_path: Path, sql: str, params: tuple = ()) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _read_outbox_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _run_alerts_once(
    *,
    env: dict[str, str],
    db_path: Path,
    fixtures_dir: Path,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "alerts",
        "run-once",
        "--shops",
        "samord,minmin",
        "--db",
        str(db_path),
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(fixtures_dir),
        "--as-of",
        "2026-02-03T14:00:00+07:00",
    ]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_phase1_alerts_pacing_run_once_fixtures_open_cooldown_resolve(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "alerts.db"
    outbox_path = tmp_path / "discord_outbox.txt"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DISCORD_DRY_RUN"] = "1"
    env["DISCORD_OUTBOX_PATH"] = str(outbox_path)
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)
    env.pop("ADS_CAMPAIGN_LIST_PATH", None)

    # Run 1: open pacing incidents + notify
    r1 = _run_alerts_once(env=env, db_path=db_path, fixtures_dir=FIXTURES_OPEN)
    assert r1.returncode == 0, r1.stdout + r1.stderr
    assert "phase1_alerts_run_once_ok=1" in r1.stdout
    assert "budget_coverage shop=samord" in r1.stdout
    assert "budget_coverage shop=minmin" in r1.stdout

    # Budget must be ingested for pacing alerts to be meaningful.
    for shop_key in ("samord", "minmin"):
        total = _count_rows(
            db_path, "SELECT COUNT(*) FROM ads_campaign WHERE shop_key = ?", (shop_key,)
        )
        with_budget = _count_rows(
            db_path,
            "SELECT COUNT(*) FROM ads_campaign WHERE shop_key = ? AND daily_budget IS NOT NULL",
            (shop_key,),
        )
        assert total > 0
        assert with_budget == total

    pacing_open = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM ads_incident WHERE incident_type = ? AND status = 'OPEN'",
        ("pacing_overspend",),
    )
    assert pacing_open == 2

    health_any = _count_rows(
        db_path, "SELECT COUNT(*) FROM ads_incident WHERE incident_type LIKE 'health_%'"
    )
    assert health_any == 0

    outbox_1 = _read_outbox_lines(outbox_path)
    assert any(
        line.startswith("[SAMORD][ALERT]") or line.startswith("[MINMIN][ALERT]")
        for line in outbox_1
    )

    # Run 2: within cooldown (same as_of) -> no new notify
    r2 = _run_alerts_once(env=env, db_path=db_path, fixtures_dir=FIXTURES_OPEN)
    assert r2.returncode == 0, r2.stdout + r2.stderr
    assert "phase1_alerts_run_once_ok=1" in r2.stdout

    outbox_2 = _read_outbox_lines(outbox_path)
    assert len(outbox_2) == len(outbox_1)

    # Run 3: resolved fixtures -> resolve incidents + notify resolved
    r3 = _run_alerts_once(env=env, db_path=db_path, fixtures_dir=FIXTURES_RESOLVED)
    assert r3.returncode == 0, r3.stdout + r3.stderr
    assert "phase1_alerts_run_once_ok=1" in r3.stdout

    pacing_resolved = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM ads_incident WHERE incident_type = ? AND status = 'RESOLVED'",
        ("pacing_overspend",),
    )
    assert pacing_resolved == 2

    outbox_3 = _read_outbox_lines(outbox_path)
    assert len(outbox_3) > len(outbox_2)
    assert any("RESOLVED" in line for line in outbox_3)

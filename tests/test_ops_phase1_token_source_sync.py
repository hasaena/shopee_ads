from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOKENS_DIR = REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens"


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


def test_evidence_runner_token_sync_from_file(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    db_path = tmp_path / "token_sync.db"
    token_file = TOKENS_DIR / "shopee_tokens_export_example.json"
    env_file = REPO_ROOT / "collaboration" / "env" / ".env.phase1.local.example"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        str(env_file),
        "--token-file",
        str(token_file),
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--skip-sweep",
        "--artifacts-root",
        str(REPO_ROOT / "tests" / "fixtures" / "phase1_failure_artifacts"),
        "--out",
        str(tmp_path / "phase1_failures_task048.md"),
        "--evidence-out",
        str(tmp_path / "phase1_evidence_task048.md"),
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
    assert "token_sync_from_file_ok=1" in result.stdout
    assert "token_source=db shop=samord" in result.stdout
    assert "token_source=db shop=minmin" in result.stdout
    assert "preflight_ok=1" in result.stdout
    assert "evidence_ok=1" in result.stdout

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM shopee_tokens")
        count = cursor.fetchone()[0]
    finally:
        conn.close()

    assert count == 2

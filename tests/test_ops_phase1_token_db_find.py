from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _create_db(path: Path, with_tokens: bool) -> None:
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    if with_tokens:
        cur.execute(
            "\n".join(
                [
                    "CREATE TABLE shopee_tokens (",
                    " shop_key TEXT PRIMARY KEY,",
                    " shop_id INTEGER NOT NULL,",
                    " access_token TEXT NOT NULL,",
                    " refresh_token TEXT NOT NULL,",
                    " access_token_expires_at TEXT",
                    ")",
                ]
            )
        )
        cur.execute(
            "INSERT INTO shopee_tokens (shop_key, shop_id, access_token, refresh_token) VALUES (?, ?, ?, ?)",
            ("samord", 111, "ACCESS", "REFRESH"),
        )
        cur.execute(
            "INSERT INTO shopee_tokens (shop_key, shop_id, access_token, refresh_token) VALUES (?, ?, ?, ?)",
            ("minmin", 222, "ACCESS", "REFRESH"),
        )
    else:
        cur.execute("CREATE TABLE other_table (id INTEGER)")
    conn.commit()
    conn.close()


def test_ops_phase1_token_db_find_recommends_best(tmp_path) -> None:
    db_empty = tmp_path / "empty.db"
    db_tokens = tmp_path / "tokens.db"
    _create_db(db_empty, with_tokens=False)
    _create_db(db_tokens, with_tokens=True)

    env = os.environ.copy()

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "token-db",
        "find",
        "--only-shops",
        "samord,minmin",
        "--scan-root",
        str(tmp_path),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    expected_fragment = db_tokens.resolve().as_posix()
    assert expected_fragment in result.stdout

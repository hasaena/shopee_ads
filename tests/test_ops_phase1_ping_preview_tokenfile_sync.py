from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
TOKEN_FILE = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_example.json"
)
ENV_FILE = REPO_ROOT / "collaboration" / "env" / ".env.phase1.local.example"


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


def test_ops_phase1_ping_preview_tokenfile_sync(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{(tmp_path / 'token_sync.db').as_posix()}"

    ping_cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "check",
        "shopee-ping",
        "--shops",
        "samord,minmin",
        "--env-file",
        str(ENV_FILE),
        "--token-file",
        str(TOKEN_FILE),
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
    ]
    ping_result = subprocess.run(
        ping_cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert ping_result.returncode == 0, ping_result.stdout + ping_result.stderr
    assert "token_sync_from_file_ok=1 shops=samord,minmin" in ping_result.stdout
    assert "shop=samord ping_ok=1" in ping_result.stdout
    assert "shop=minmin ping_ok=1" in ping_result.stdout

    preview_cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "preview",
        "--env-file",
        str(ENV_FILE),
        "--token-file",
        str(TOKEN_FILE),
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--reports-dir",
        str(tmp_path / "reports"),
        "--no-send-discord",
    ]
    preview_result = subprocess.run(
        preview_cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert preview_result.returncode == 0, (
        preview_result.stdout + preview_result.stderr
    )
    assert "token_sync_from_file_ok=1 shops=samord,minmin" in preview_result.stdout
    assert "phase1_preview_ok=1" in preview_result.stdout

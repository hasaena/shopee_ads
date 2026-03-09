from __future__ import annotations

import json
import os
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


def _write_env(path: Path) -> None:
    lines = [
        "SHOPEE_PARTNER_ID=2010863",
        "SHOPEE_PARTNER_KEY=TEST_PARTNER_KEY",
        "SHOPEE_SAMORD_SHOP_ID=497412318",
        "SHOPEE_MINMIN_SHOP_ID=567655304",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_ops_phase1_auth_sign_fingerprint(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    env_path = tmp_path / ".env.phase1.local"
    out_path = tmp_path / "auth_sign_fingerprint.json"
    _write_shops(shops_path)
    _write_env(env_path)

    token_file = TOKENS_DIR / "shopee_tokens_export_valid.json"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "auth",
        "sign-fingerprint",
        "--env-file",
        str(env_path),
        "--token-file",
        str(token_file),
        "--shops",
        "samord,minmin",
        "--out",
        str(out_path),
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
    assert out_path.exists()

    data = json.loads(out_path.read_text(encoding="utf-8"))
    assert data.get("partner_id") == 2010863
    assert "partner_key_sha8" in data
    assert data.get("timestamp") == 1700000000
    assert "/api/v2/shop/get_shop_info" in data.get("paths", [])
    assert "shops" in data
    assert "samord" in data["shops"]
    assert "minmin" in data["shops"]
    samord_paths = data["shops"]["samord"].get("paths", {})
    assert "/api/v2/shop/get_shop_info" in samord_paths
    assert "sign_input_sha8" in samord_paths["/api/v2/shop/get_shop_info"]
    assert "sign_sha8" in samord_paths["/api/v2/shop/get_shop_info"]

    stdout = result.stdout
    content = out_path.read_text(encoding="utf-8")
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "TEST_PARTNER_KEY" not in stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in content
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in content
    assert "TEST_PARTNER_KEY" not in content

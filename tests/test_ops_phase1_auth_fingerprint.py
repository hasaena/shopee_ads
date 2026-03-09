from __future__ import annotations

import hashlib
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


def _sha8(value: str | None) -> str:
    if not value:
        return "-"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _extract_token_map(data: dict) -> dict[str, dict]:
    token_map: dict[str, dict] = {}
    for key, value in data.items():
        shop_id = None
        key_str = str(key)
        if key_str.isdigit():
            shop_id = key_str
        elif key_str.startswith("SHOPEE_TOKEN_DATA_"):
            suffix = key_str.replace("SHOPEE_TOKEN_DATA_", "", 1)
            if suffix.isdigit():
                shop_id = suffix
        if not shop_id:
            continue
        payload = value
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(payload, dict):
            token_map[shop_id] = payload
    return token_map


def _build_compare_file(path: Path, token_file: Path) -> None:
    data = json.loads(token_file.read_text(encoding="utf-8"))
    token_map = _extract_token_map(data)
    samord = token_map["497412318"]
    minmin = token_map["567655304"]
    compare = {
        "partner_id": 2010863,
        "partner_key_sha8": _sha8("TEST_PARTNER_KEY"),
        "shops": {
            "samord": {
                "shop_id": 497412318,
                "token_len": len(samord.get("access_token", "")),
                "token_sha8": _sha8(samord.get("access_token")),
            },
            "minmin": {
                "shop_id": 567655304,
                "token_len": len(minmin.get("access_token", "")),
                "token_sha8": _sha8(minmin.get("access_token")),
            },
        },
    }
    path.write_text(json.dumps(compare, indent=2), encoding="utf-8")


def _run_cli(env_path: Path, token_file: Path, compare_path: Path, env: dict) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "auth",
        "fingerprint",
        "--env-file",
        str(env_path),
        "--token-file",
        str(token_file),
        "--shops",
        "samord,minmin",
        "--compare-to",
        str(compare_path),
    ]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_ops_phase1_auth_fingerprint_parity_ok(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    env_path = tmp_path / ".env.phase1.local"
    compare_path = tmp_path / "appsscript_fingerprint.json"
    _write_shops(shops_path)
    _write_env(env_path)

    token_file = TOKENS_DIR / "shopee_tokens_export_valid.json"
    _build_compare_file(compare_path, token_file)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    result = _run_cli(env_path, token_file, compare_path, env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "parity_ok=1" in result.stdout
    assert "shop=samord" in result.stdout and "token_match=1" in result.stdout
    assert "shop=minmin" in result.stdout and "token_match=1" in result.stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    assert "TEST_PARTNER_KEY" not in result.stdout


def test_ops_phase1_auth_fingerprint_parity_mismatch(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    env_path = tmp_path / ".env.phase1.local"
    compare_path = tmp_path / "appsscript_fingerprint.json"
    _write_shops(shops_path)
    _write_env(env_path)

    token_file = TOKENS_DIR / "shopee_tokens_export_valid.json"
    _build_compare_file(compare_path, token_file)
    compare = json.loads(compare_path.read_text(encoding="utf-8"))
    compare["shops"]["minmin"]["token_sha8"] = "deadbeef"
    compare_path.write_text(json.dumps(compare, indent=2), encoding="utf-8")

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    result = _run_cli(env_path, token_file, compare_path, env)

    assert result.returncode == 2, result.stdout + result.stderr
    assert "parity_ok=0" in result.stdout
    assert "compare_shop minmin ok=0" in result.stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    assert "TEST_PARTNER_KEY" not in result.stdout

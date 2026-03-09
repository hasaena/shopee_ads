from __future__ import annotations

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


def _run_preflight(tmp_path: Path, allow_unknown: bool) -> subprocess.CompletedProcess:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    token_file = TOKENS_DIR / "shopee_tokens_export_missing_access_expiry.json"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "token",
        "appsscript",
        "preflight",
        "--token-file",
        str(token_file),
        "--shops",
        "samord,minmin",
        "--min-access-ttl-sec",
        "600",
    ]
    if allow_unknown:
        cmd.append("--allow-unknown-expiry")

    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_preflight_unknown_expiry_fails_by_default(tmp_path: Path) -> None:
    result = _run_preflight(tmp_path, allow_unknown=False)

    assert result.returncode == 2, result.stdout + result.stderr
    assert "access_expiry_kind=unknown" in result.stdout
    assert "token_verdict=unknown" in result.stdout
    assert "preflight_ok=0" in result.stdout
    assert "AT_FAKE_SAMORD_NO_EXP" not in result.stdout
    assert "RT_FAKE_MINMIN_NO_EXP" not in result.stdout


def test_preflight_unknown_expiry_allows_opt_in(tmp_path: Path) -> None:
    result = _run_preflight(tmp_path, allow_unknown=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "access_expiry_kind=unknown" in result.stdout
    assert "token_verdict=unknown" in result.stdout
    assert "warning=unknown_access_expiry_allowed" in result.stdout
    assert "preflight_ok=1" in result.stdout
    assert "AT_FAKE_SAMORD_NO_EXP" not in result.stdout
    assert "RT_FAKE_MINMIN_NO_EXP" not in result.stdout

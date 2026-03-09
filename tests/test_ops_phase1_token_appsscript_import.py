from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_example.json"
)
RAW_FIXTURE_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_raw_properties_example.json"
)


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
            ]
        ),
        encoding="utf-8",
    )


def _run_import(tmp_path: Path, fixture_path: Path) -> str:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    env_path = tmp_path / ".env.phase1.local"
    env_path.write_text(
        "\n".join(
            [
                "SHOPEE_SAMORD_SHOP_ID=497412318",
                "SHOPEE_MINMIN_SHOP_ID=567655304",
            ]
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "tokens.db"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "token",
        "appsscript",
        "import",
        "--env-file",
        str(env_path),
        "--file",
        str(fixture_path),
        "--shops",
        "samord,minmin",
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
    assert "token_appsscript_import_ok=1 imported_total=2" in result.stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in result.stdout
    return db_path.as_posix()

    readiness_cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "readiness",
        "phase1",
        "--shops",
        "samord,minmin",
        "--env-file",
        str(env_path),
    ]

    readiness = subprocess.run(
        readiness_cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = readiness.stdout
    assert "shop=samord" in stdout
    assert "token_access=1" in stdout
    assert "token_refresh=1" in stdout


def test_ops_phase1_token_appsscript_import_format_a(tmp_path) -> None:
    _run_import(tmp_path, FIXTURE_PATH)


def test_ops_phase1_token_appsscript_import_format_b(tmp_path) -> None:
    _run_import(tmp_path, RAW_FIXTURE_PATH)

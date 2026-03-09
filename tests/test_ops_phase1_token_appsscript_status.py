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


def test_ops_phase1_token_appsscript_status_refresh_expiry(tmp_path) -> None:
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

    import_cmd = [
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
        str(FIXTURE_PATH),
        "--shops",
        "samord,minmin",
    ]
    result = subprocess.run(
        import_cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    status_cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "token",
        "appsscript",
        "status",
        "--env-file",
        str(env_path),
        "--shops",
        "samord,minmin",
    ]
    status = subprocess.run(
        status_cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert status.returncode == 0
    stdout = status.stdout
    assert "refresh_expires_in_sec=" in stdout
    assert "refresh_expires_in_sec=-1" not in stdout

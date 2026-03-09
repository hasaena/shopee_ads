from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 111",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 222",
            ]
        ),
        encoding="utf-8",
    )


def test_ops_check_discord_dry_run(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DISCORD_WEBHOOK_REPORT_URL"] = "https://discord.local/report"
    env["DISCORD_WEBHOOK_ALERTS_URL"] = "https://discord.local/alerts"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "check",
        "discord",
        "--shops",
        "samord,minmin",
        "--dry-run",
        "--channel",
        "both",
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
    stdout = result.stdout
    assert "[SAMORD][ACTION]" in stdout
    assert "[MINMIN][ACTION]" in stdout
    assert "https://discord.local/report" not in stdout
    assert "https://discord.local/alerts" not in stdout


def test_ops_check_discord_send_missing_webhook(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("DISCORD_WEBHOOK_REPORT_URL", None)
    env.pop("DISCORD_WEBHOOK_ALERTS_URL", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "check",
        "discord",
        "--shops",
        "samord,minmin",
        "--send",
        "--channel",
        "both",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    stdout = result.stdout
    assert "webhook_not_configured" in stdout


def test_ops_check_shopee_ping_fixtures(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "check",
        "shopee-ping",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
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
    stdout = result.stdout
    assert "shop=samord ping_ok=1" in stdout
    assert "shop=minmin ping_ok=1" in stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_BE_REDACTED" not in stdout


def test_ops_check_shopee_ping_live_requires_allow_network(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "check",
        "shopee-ping",
        "--shops",
        "samord,minmin",
        "--transport",
        "live",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    stdout = result.stdout
    assert "allow_network_required" in stdout or "network_disabled" in stdout

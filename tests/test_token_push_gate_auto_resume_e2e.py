from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


REPO_ROOT = Path(__file__).resolve().parents[1]
TOKEN_FILE = (
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
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
                "- shop_key: extra",
                "  label: EXTRA",
                "  enabled: true",
                "  shopee_shop_id: 999999999",
            ]
        ),
        encoding="utf-8",
    )


def _base_env(tmp_path: Path, db_path: Path) -> dict[str, str]:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    env["OPS_TOKEN"] = "test-ops-token"
    env["SHOPEE_SAMORD_SHOP_ID"] = "497412318"
    env["SHOPEE_MINMIN_SHOP_ID"] = "567655304"
    env["DISCORD_DRY_RUN"] = "1"
    env["DISCORD_OUTBOX_PATH"] = str(tmp_path / "discord_outbox.log")
    env.pop("ALLOW_NETWORK", None)
    env.pop("DOTORI_STRICT_PREFLIGHT", None)
    env.pop("DOTORI_MIN_ACCESS_TTL_SEC", None)
    env.pop("DOTORI_TOKEN_ALERT_COOLDOWN_SEC", None)
    env.pop("DOTORI_TOKEN_RESOLVED_COOLDOWN_SEC", None)
    return env


def _run_cli(env: dict[str, str], args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "-m", "dotori_shopee_automation.cli", *args]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _run_rehearsal(
    env: dict[str, str],
    *,
    db_path: Path,
    summary_path: Path,
    reports_dir: Path,
) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        env,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-26",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--discord-mode",
            "dry-run",
            "--strict-preflight",
            "--min-access-ttl-sec",
            "1200",
            "--token-alert-cooldown-sec",
            "60",
            "--token-resolved-cooldown-sec",
            "60",
            "--reports-dir",
            str(reports_dir),
            "--summary-out",
            str(summary_path),
        ],
    )


def test_token_push_gate_auto_resume_e2e(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "token_push_resume.db"
    reports_dir = tmp_path / "reports"
    env = _base_env(tmp_path, db_path)

    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    seeded = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "token",
            "seed-expired",
            "--shops",
            "samord,minmin",
            "--db",
            str(db_path),
        ],
    )
    assert seeded.returncode == 0, seeded.stdout + seeded.stderr
    assert "token_seed_expired_ok=1" in seeded.stdout

    fail_summary = tmp_path / "rehearsal_fail.json"
    fail_run = _run_rehearsal(
        env,
        db_path=db_path,
        summary_path=fail_summary,
        reports_dir=reports_dir,
    )
    assert fail_run.returncode == 0, fail_run.stdout + fail_run.stderr
    assert "preflight_gate_ok=0" in fail_run.stdout
    assert "skipped_due_to_token=1" in fail_run.stdout
    assert "planned_calls_in_fail=0" in fail_run.stdout
    assert "discord_token_alert_dry_run=1 shop=samord" in fail_run.stdout
    assert "discord_token_alert_dry_run=1 shop=minmin" in fail_run.stdout

    payload = {"tokens": json.loads(TOKEN_FILE.read_text(encoding="utf-8"))}
    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer test-ops-token"},
        )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data.get("ok") is True
    assert int(data.get("imported_total", 0)) == 2
    assert sorted(data.get("updated_shops", [])) == ["minmin", "samord"]
    token_fingerprints = data.get("token_fingerprints")
    assert isinstance(token_fingerprints, dict)
    assert sorted(token_fingerprints.keys()) == ["minmin", "samord"]
    for shop_key in ("samord", "minmin"):
        fp = token_fingerprints.get(shop_key) or {}
        assert int(fp.get("token_len", 0)) > 0
        assert isinstance(fp.get("token_sha8"), str)
        assert len(fp["token_sha8"]) == 8

    body_text = response.text
    fixture_raw = payload["tokens"]
    assert fixture_raw["SHOPEE_TOKEN_DATA_497412318"]["access_token"] not in body_text
    assert fixture_raw["SHOPEE_TOKEN_DATA_497412318"]["refresh_token"] not in body_text

    resume_summary = tmp_path / "rehearsal_resume.json"
    resume_run = _run_rehearsal(
        env,
        db_path=db_path,
        summary_path=resume_summary,
        reports_dir=reports_dir,
    )
    assert resume_run.returncode == 0, resume_run.stdout + resume_run.stderr
    assert "preflight_gate_ok=1" in resume_run.stdout
    assert "skipped_due_to_token=0" in resume_run.stdout
    assert (
        "discord_token_resolved_dry_run=1 shop=samord" in resume_run.stdout
        or "discord_token_resolved_cooldown_skip=1 shop=samord" in resume_run.stdout
    )
    assert (
        "discord_token_resolved_dry_run=1 shop=minmin" in resume_run.stdout
        or "discord_token_resolved_cooldown_skip=1 shop=minmin" in resume_run.stdout
    )

    rerun_summary = tmp_path / "rehearsal_resume_rerun.json"
    rerun = _run_rehearsal(
        env,
        db_path=db_path,
        summary_path=rerun_summary,
        reports_dir=reports_dir,
    )
    assert rerun.returncode == 0, rerun.stdout + rerun.stderr
    assert (
        "discord_token_resolved_cooldown_skip=1 shop=samord" in rerun.stdout
        or "discord_token_resolved_dry_run=1 shop=samord" in rerun.stdout
    )
    assert (
        "discord_token_resolved_cooldown_skip=1 shop=minmin" in rerun.stdout
        or "discord_token_resolved_dry_run=1 shop=minmin" in rerun.stdout
    )

    get_settings.cache_clear()

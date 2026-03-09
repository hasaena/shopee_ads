from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


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
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)
    env.pop("ADS_CAMPAIGN_LIST_PATH", None)
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


def _import_tokens(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        env,
        [
            "ops",
            "phase1",
            "token",
            "appsscript",
            "import",
            "--file",
            str(TOKEN_FILE),
            "--shops",
            "samord,minmin",
        ],
    )


def _run_rehearsal(
    env: dict[str, str],
    *,
    db_path: Path,
    reports_dir: Path,
    summary_path: Path,
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
            "--reports-dir",
            str(reports_dir),
            "--summary-out",
            str(summary_path),
            "--discord-mode",
            "dry-run",
            "--strict-preflight",
            "--min-access-ttl-sec",
            "1200",
            "--token-alert-cooldown-sec",
            "21600",
            "--token-resolved-cooldown-sec",
            "21600",
        ],
    )


def _run_token_status(env: dict[str, str], *, db_path: Path) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        env,
        [
            "ops",
            "phase1",
            "token",
            "status",
            "--shops",
            "samord,minmin",
            "--db",
            str(db_path),
            "--min-access-ttl-sec",
            "1200",
        ],
    )


def test_token_preflight_gate_pass_allows_run(tmp_path: Path) -> None:
    db_path = tmp_path / "pass.db"
    env = _base_env(tmp_path, db_path)
    imported = _import_tokens(env)
    assert imported.returncode == 0, imported.stdout + imported.stderr

    reports_dir = tmp_path / "reports_pass"
    summary_path = tmp_path / "summary_pass.json"
    result = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=summary_path,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "preflight_gate_ok=1" in result.stdout
    assert "skipped_due_to_token=0" in result.stdout
    assert "phase1_go_live_rehearsal_ok=1" in result.stdout
    assert "planned_calls:" in result.stdout
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    assert data.get("skipped_due_to_token") is False
    assert data.get("ok") is True


def test_token_preflight_gate_fail_skips_and_alerts_once_with_cooldown(tmp_path: Path) -> None:
    db_path = tmp_path / "fail.db"
    env = _base_env(tmp_path, db_path)
    outbox = tmp_path / "discord_outbox.log"
    env["DISCORD_OUTBOX_PATH"] = str(outbox)
    seed = _run_cli(
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
    assert seed.returncode == 0, seed.stdout + seed.stderr
    assert "token_seed_expired_ok=1" in seed.stdout

    reports_dir = tmp_path / "reports_fail"
    summary_path = tmp_path / "summary_fail.json"
    first = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=summary_path,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert "preflight_gate_ok=0" in first.stdout
    assert "skipped_due_to_token=1" in first.stdout
    assert "planned_calls_in_fail=0" in first.stdout
    assert "phase1_go_live_rehearsal_ok=0 reason=skipped_due_to_token" in first.stdout
    assert "phase1_alerts_run_once_start" not in first.stdout
    assert "planned_calls:" not in first.stdout
    assert "discord_token_alert_dry_run=1 shop=samord" in first.stdout
    assert "discord_token_alert_dry_run=1 shop=minmin" in first.stdout

    first_outbox_lines = outbox.read_text(encoding="utf-8").splitlines()
    assert len(first_outbox_lines) == 2
    assert any(line.startswith("[SAMORD][ALERT] TOKEN_TTL_LOW") for line in first_outbox_lines)
    assert any(line.startswith("[MINMIN][ALERT] TOKEN_TTL_LOW") for line in first_outbox_lines)

    second_summary = tmp_path / "summary_fail_rerun.json"
    second = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=second_summary,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert "preflight_gate_ok=0" in second.stdout
    assert "skipped_due_to_token=1" in second.stdout
    assert "discord_token_alert_cooldown_skip=1 shop=samord" in second.stdout
    assert "discord_token_alert_cooldown_skip=1 shop=minmin" in second.stdout

    second_outbox_lines = outbox.read_text(encoding="utf-8").splitlines()
    assert len(second_outbox_lines) == 2

    gate_json = summary_path.parent / "preflight_gate_summary.json"
    gate_md = summary_path.parent / "preflight_gate_summary.md"
    assert gate_json.exists()
    assert gate_md.exists()


def test_token_preflight_gate_resume_emits_resolved_once_and_status(tmp_path: Path) -> None:
    db_path = tmp_path / "resume.db"
    env = _base_env(tmp_path, db_path)
    outbox = tmp_path / "discord_outbox.log"
    env["DISCORD_OUTBOX_PATH"] = str(outbox)

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

    reports_dir = tmp_path / "reports_resume"
    fail_summary = tmp_path / "summary_resume_fail.json"
    first = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=fail_summary,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert "preflight_gate_ok=0" in first.stdout
    assert "discord_token_alert_dry_run=1 shop=samord" in first.stdout
    assert "discord_token_alert_dry_run=1 shop=minmin" in first.stdout

    imported = _import_tokens(env)
    assert imported.returncode == 0, imported.stdout + imported.stderr

    resume_summary = tmp_path / "summary_resume_ok.json"
    second = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=resume_summary,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert "preflight_gate_ok=1" in second.stdout
    assert "skipped_due_to_token=0" in second.stdout
    assert "phase1_go_live_rehearsal_ok=1" in second.stdout
    assert "discord_token_resolved_dry_run=1 shop=samord" in second.stdout
    assert "discord_token_resolved_dry_run=1 shop=minmin" in second.stdout
    assert "planned_calls:" in second.stdout

    gate_json = resume_summary.parent / "preflight_gate_summary.json"
    gate_payload = json.loads(gate_json.read_text(encoding="utf-8"))
    assert gate_payload.get("resolved_emitted") is True

    resume_rerun_summary = tmp_path / "summary_resume_rerun.json"
    third = _run_rehearsal(
        env,
        db_path=db_path,
        reports_dir=reports_dir,
        summary_path=resume_rerun_summary,
    )
    assert third.returncode == 0, third.stdout + third.stderr
    assert "preflight_gate_ok=1" in third.stdout
    assert "discord_token_resolved_cooldown_skip=1 shop=samord" in third.stdout
    assert "discord_token_resolved_cooldown_skip=1 shop=minmin" in third.stdout

    gate_payload_rerun = json.loads(gate_json.read_text(encoding="utf-8"))
    assert gate_payload_rerun.get("resolved_cooldown_skipped") is True

    token_status = _run_token_status(env, db_path=db_path)
    assert token_status.returncode == 0, token_status.stdout + token_status.stderr
    assert "token_status shop=samord" in token_status.stdout
    assert "token_status shop=minmin" in token_status.stdout
    assert "gate_state=ok" in token_status.stdout
    assert "EXPIRED_ACCESS_" not in token_status.stdout

    outbox_lines = outbox.read_text(encoding="utf-8").splitlines()
    token_lines = [line for line in outbox_lines if "TOKEN_TTL_" in line]
    assert len(token_lines) == 4
    assert any("TOKEN_TTL_LOW" in line for line in token_lines)
    assert any("TOKEN_TTL_OK (resolved)" in line for line in token_lines)

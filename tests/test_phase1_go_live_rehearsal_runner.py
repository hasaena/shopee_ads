from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
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


def test_go_live_rehearsal_exit_2_when_db_tokens_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_tokens.db"
    reports_dir = tmp_path / "reports"
    summary_path = tmp_path / "summary_missing.json"
    env = _base_env(tmp_path, db_path)

    result = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--reports-dir",
            str(reports_dir),
            "--summary-out",
            str(summary_path),
        ],
    )

    assert result.returncode == 2, result.stdout + result.stderr
    assert "preflight_ok=0" in result.stdout
    assert "phase1_go_live_rehearsal_ok=0 reason=preflight_failed" in result.stdout
    assert summary_path.exists()
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    assert data.get("ok") is False
    assert data.get("reason") == "preflight_failed"


def test_go_live_rehearsal_fixtures_success_and_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "rehearsal_ok.db"
    reports_dir = tmp_path / "reports_ok"
    summary_path = tmp_path / "summary_ok.json"
    bundle_path = tmp_path / "rehearsal_ok_bundle.zip"
    env = _base_env(tmp_path, db_path)

    imported = _import_tokens(env)
    assert imported.returncode == 0, imported.stdout + imported.stderr

    result = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--reports-dir",
            str(reports_dir),
            "--summary-out",
            str(summary_path),
            "--bundle-out",
            str(bundle_path),
            "--discord-mode",
            "off",
        ],
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "preflight_ok=1" in result.stdout
    assert "phase1_alerts_run_once_ok=1" in result.stdout
    assert result.stdout.count("phase1_schedule_run_once_ok=1") >= 2
    assert f"bundle_path={bundle_path}" in result.stdout
    assert "bundle_files=" in result.stdout
    assert "bundle_size=" in result.stdout
    assert "phase1_go_live_rehearsal_ok=1" in result.stdout
    assert summary_path.exists()
    assert bundle_path.exists()

    data = json.loads(summary_path.read_text(encoding="utf-8"))
    assert data.get("ok") is True
    reports = data.get("reports", {})
    assert reports.get("midday_ok") is True
    assert reports.get("final_ok") is True
    report_paths = reports.get("report_paths") or []
    assert len(report_paths) >= 4
    for row in report_paths:
        assert row.get("exists") is True
        assert int(row.get("size") or 0) > 0
    bundle = data.get("bundle") or {}
    assert Path(str(bundle.get("path"))).resolve() == bundle_path.resolve()
    assert int(bundle.get("files") or 0) >= 5
    assert int(bundle.get("size") or 0) > 0

    with zipfile.ZipFile(bundle_path) as zf:
        names = set(zf.namelist())
    assert "summary.json" in names
    html_files = [name for name in names if name.endswith(".html")]
    assert len(html_files) >= 4


def test_phase1_token_status_hides_raw_token_values(tmp_path: Path) -> None:
    db_path = tmp_path / "token_status.db"
    env = _base_env(tmp_path, db_path)

    imported = _import_tokens(env)
    assert imported.returncode == 0, imported.stdout + imported.stderr

    status = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "token",
            "status",
            "--shops",
            "samord,minmin",
            "--min-access-ttl-sec",
            "900",
        ],
    )
    assert status.returncode == 0, status.stdout + status.stderr
    assert "token_source=db" in status.stdout
    assert "token_len=" in status.stdout
    assert "token_sha8=" in status.stdout
    assert "token_verdict=ok" in status.stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in status.stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in status.stdout


def test_go_live_rehearsal_live_requires_allow_network(tmp_path: Path) -> None:
    db_path = tmp_path / "live_guard.db"
    reports_dir = tmp_path / "reports"
    summary_path = tmp_path / "summary.json"
    env = _base_env(tmp_path, db_path)

    result = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--transport",
            "live",
            "--db",
            str(db_path),
            "--reports-dir",
            str(reports_dir),
            "--summary-out",
            str(summary_path),
            "--discord-mode",
            "off",
        ],
    )

    assert result.returncode == 2, result.stdout + result.stderr
    assert "live_transport_requires_allow_network=1" in result.stdout

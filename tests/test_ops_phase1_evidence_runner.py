from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SECRET_MARKERS = [
    "PARTNER_KEY_SHOULD_NOT_PRINT",
    "ACCESS_TOKEN_SHOULD_NOT_PRINT",
    "REFRESH_TOKEN_SHOULD_NOT_PRINT",
    "AUTHORIZATION_SHOULD_NOT_PRINT",
    "AUTH_SHOULD_NOT_PRINT",
    "COOKIE_SHOULD_NOT_PRINT",
    "CLIENT_SECRET_SHOULD_NOT_PRINT",
    "SIGN_SHOULD_NOT_PRINT",
    "TOKEN_SHOULD_NOT_PRINT",
]


def test_ops_phase1_evidence_runner_success_skip_sweep() -> None:
    out_path = (
        REPO_ROOT
        / "collaboration"
        / "results"
        / "phase1_failures_2026-02-03_task037.md"
    )
    evidence_path = (
        REPO_ROOT
        / "collaboration"
        / "results"
        / "phase1_evidence_2026-02-03_task037.md"
    )
    if out_path.exists():
        out_path.unlink()
    if evidence_path.exists():
        evidence_path.unlink()

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        "collaboration/env/.env.phase1.local.example",
        "--token-file",
        "tests/fixtures/appsscript_tokens/shopee_tokens_export_example.json",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--skip-sweep",
        "--artifacts-root",
        "tests/fixtures/phase1_failure_artifacts",
        "--out",
        str(out_path),
        "--evidence-out",
        str(evidence_path),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert out_path.exists()
    assert evidence_path.exists()
    assert "evidence_ok=1" in result.stdout
    assert "verdict=" in result.stdout
    assert "shop=minmin verdict=no_evidence" in result.stdout
    report = evidence_path.read_text(encoding="utf-8")
    assert "Phase1 Evidence Report" in report
    assert "preflight_ok=1" in report
    assert "summarize_failures_saved=" in report
    for marker in SECRET_MARKERS:
        assert marker not in report


def test_ops_phase1_evidence_runner_token_expired() -> None:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        "collaboration/env/.env.phase1.local.example",
        "--token-file",
        "tests/fixtures/appsscript_tokens/shopee_tokens_export_expired.json",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--skip-sweep",
        "--artifacts-root",
        "tests/fixtures/phase1_failure_artifacts",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2, result.stdout + result.stderr
    assert "evidence_ok=0 reason=token_expired_or_short_ttl" in result.stdout


def test_ops_phase1_evidence_runner_live_gate() -> None:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        "collaboration/env/.env.phase1.local.example",
        "--token-file",
        "tests/fixtures/appsscript_tokens/shopee_tokens_export_example.json",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "live",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "allow_network_required" in result.stdout


def test_ops_phase1_evidence_runner_support_packet(tmp_path: Path) -> None:
    out_path = tmp_path / "phase1_failures.md"
    evidence_path = tmp_path / "phase1_evidence.md"
    support_zip = tmp_path / "support_packet.zip"
    support_md = tmp_path / "support_request.md"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        "collaboration/env/.env.phase1.local.example",
        "--token-file",
        "tests/fixtures/appsscript_tokens/shopee_tokens_export_example.json",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--skip-sweep",
        "--artifacts-root",
        "tests/fixtures/phase1_failure_artifacts",
        "--out",
        str(out_path),
        "--evidence-out",
        str(evidence_path),
        "--support-packet",
        "--support-zip",
        str(support_zip),
        "--support-md",
        str(support_md),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert support_zip.exists()
    assert support_md.exists()
    assert "support_packet_ok=1" in result.stdout

    import zipfile

    with zipfile.ZipFile(support_zip, "r") as zipf:
        names = zipf.namelist()
        assert any(name.startswith("evidence/phase1_evidence") for name in names)
        assert any(name.startswith("evidence/phase1_failures") for name in names)

    content = support_md.read_text(encoding="utf-8")
    assert "## Endpoints Observed (unique paths)" in content
    assert "## Failing Endpoints (http >= 400)" in content
    section = content.split("## Failing Endpoints (http >= 400)", 1)[1]
    assert "/api/v2/shop/get_shop_info" not in section


def test_evidence_run_support_packet_fails_on_secret_leak(tmp_path: Path) -> None:
    out_path = tmp_path / "phase1_failures.md"
    evidence_path = tmp_path / "phase1_evidence.md"
    support_zip = tmp_path / "support_packet.zip"
    support_md = tmp_path / "support_request.md"
    artifacts_root = tmp_path / "artifacts"

    leak_path = artifacts_root / "samord" / "2026-02-03" / "leak.json"
    leak_path.parent.mkdir(parents=True, exist_ok=True)
    leak_path.write_text('{"access_token":"REAL_TOKEN"}', encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "run",
        "--env-file",
        "collaboration/env/.env.phase1.local.example",
        "--token-file",
        "tests/fixtures/appsscript_tokens/shopee_tokens_export_example.json",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--skip-sweep",
        "--artifacts-root",
        str(artifacts_root),
        "--out",
        str(out_path),
        "--evidence-out",
        str(evidence_path),
        "--support-packet",
        "--support-zip",
        str(support_zip),
        "--support-md",
        str(support_md),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, result.stdout + result.stderr
    assert "error=secret_leak_detected" in result.stdout
    assert not support_zip.exists()

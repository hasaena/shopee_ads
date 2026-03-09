from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_ops_phase1_support_packet_happy_path(tmp_path: Path) -> None:
    evidence_path = tmp_path / "phase1_evidence_2026-02-03.md"
    failures_path = tmp_path / "phase1_failures_2026-02-03.md"
    out_zip = tmp_path / "support_packet.zip"
    out_md = tmp_path / "support_request.md"

    _write_text(
        evidence_path,
        "\n".join(
            [
                "# Phase1 Evidence Report",
                "date: 2026-02-03",
                "shops: samord,minmin",
            ]
        ),
    )
    _write_text(
        failures_path,
        "\n".join(
            [
                "# Phase1 Failure Artifact Summary",
                "",
                "| shop | call_name | path | http | api_error | api_message | request_id | hint |",
                "|---|---|---|---:|---:|---|---|---|",
                "| samord | ads_daily | /api/v2/ads/get_all_cpc_ads_daily_performance | 403 | auth_failed | denied | req-ads | ads_permission_or_scope |",
            ]
        ),
    )

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "support-packet",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        "tests/fixtures/phase1_failure_artifacts",
        "--evidence-file",
        str(evidence_path),
        "--failures-file",
        str(failures_path),
        "--out-zip",
        str(out_zip),
        "--out-md",
        str(out_md),
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
    assert out_zip.exists()
    assert out_md.exists()

    with zipfile.ZipFile(out_zip, "r") as zipf:
        names = zipf.namelist()
        assert "evidence/phase1_failures_2026-02-03.md" in names
        assert any(name.startswith("artifacts/samord/2026-02-03/") for name in names)


def test_ops_phase1_support_packet_missing_input(tmp_path: Path) -> None:
    failures_path = tmp_path / "phase1_failures_2026-02-03.md"
    _write_text(
        failures_path,
        "\n".join(
            [
                "# Phase1 Failure Artifact Summary",
                "| shop | call_name | path | http | api_error | api_message | request_id | hint |",
            ]
        ),
    )

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "support-packet",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        "tests/fixtures/phase1_failure_artifacts",
        "--evidence-file",
        str(tmp_path / "missing.md"),
        "--failures-file",
        str(failures_path),
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
    assert "error=missing_input_file" in result.stdout


def test_ops_phase1_support_packet_secret_leak(tmp_path: Path) -> None:
    evidence_path = tmp_path / "phase1_evidence_2026-02-03.md"
    failures_path = tmp_path / "phase1_failures_2026-02-03.md"
    artifacts_root = tmp_path / "artifacts"

    _write_text(evidence_path, "# Phase1 Evidence Report")
    _write_text(
        failures_path,
        "\n".join(
            [
                "# Phase1 Failure Artifact Summary",
                "",
                "| shop | call_name | path | http | api_error | api_message | request_id | hint |",
                "|---|---|---|---:|---:|---|---|---|",
                "| samord | ads_daily | /api/v2/ads/get_all_cpc_ads_daily_performance | 403 | auth_failed | denied | req-ads | ads_permission_or_scope |",
            ]
        ),
    )
    leak_path = artifacts_root / "samord" / "2026-02-03" / "leak.json"
    _write_text(leak_path, '{"access_token":"REAL_TOKEN"}')

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "evidence",
        "support-packet",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        str(artifacts_root),
        "--evidence-file",
        str(evidence_path),
        "--failures-file",
        str(failures_path),
        "--out-zip",
        str(tmp_path / "support_packet.zip"),
        "--out-md",
        str(tmp_path / "support_request.md"),
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
    assert "error=secret_leak_detected" in result.stdout

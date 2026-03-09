from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_artifact(path: Path) -> None:
    payload = {
        "request_meta": {
            "path": "/api/v2/ads/get_all_cpc_ads_daily_performance",
        },
        "response_meta": {
            "http_status": 403,
        },
        "parsed_error": {
            "api_error": "invalid_acceess_token",
            "api_message": "Invalid access_token, please have a check.",
            "request_id": "req-1",
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _count_rows(path: Path) -> int:
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("| samord |") or line.startswith("| minmin |"):
            count += 1
    return count


def test_phase1_artifacts_summarize_failures_window_filter(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    for shop in ("samord", "minmin"):
        shop_root = root / shop / "2026-02-03"
        _write_artifact(
            shop_root
            / "1000_ads_daily_api_v2_ads_get_all_cpc_ads_daily_performance.json"
        )
        _write_artifact(
            shop_root
            / "2000_ads_daily_api_v2_ads_get_all_cpc_ads_daily_performance.json"
        )

    out_all = tmp_path / "summary_all.md"
    cmd_all = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "artifacts",
        "summarize-failures",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        str(root),
        "--out",
        str(out_all),
    ]
    result_all = subprocess.run(
        cmd_all,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result_all.returncode == 0, result_all.stdout + result_all.stderr
    assert _count_rows(out_all) == 4

    out_filtered = tmp_path / "summary_filtered.md"
    cmd_filtered = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "artifacts",
        "summarize-failures",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        str(root),
        "--since-ms",
        "1500",
        "--out",
        str(out_filtered),
    ]
    result_filtered = subprocess.run(
        cmd_filtered,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result_filtered.returncode == 0, (
        result_filtered.stdout + result_filtered.stderr
    )
    assert "artifact_filter since_ms=1500" in result_filtered.stdout
    assert _count_rows(out_filtered) == 2

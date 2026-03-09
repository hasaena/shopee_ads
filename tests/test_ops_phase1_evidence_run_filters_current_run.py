from __future__ import annotations

import json
from pathlib import Path

import typer

from dotori_shopee_automation import cli


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


def _count_rows(path: Path) -> int:
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("| samord |") or line.startswith("| minmin |"):
            count += 1
    return count


def test_ops_phase1_evidence_run_filters_current_run(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    artifacts_root = tmp_path / "artifacts"
    for shop in ("samord", "minmin"):
        shop_root = artifacts_root / shop / "2026-02-03"
        _write_artifact(
            shop_root
            / "1000_ads_daily_api_v2_ads_get_all_cpc_ads_daily_performance.json"
        )
        _write_artifact(
            shop_root
            / "2000_ads_daily_api_v2_ads_get_all_cpc_ads_daily_performance.json"
        )

    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env_file = REPO_ROOT / "collaboration" / "env" / ".env.phase1.local.example"
    token_file = (
        REPO_ROOT
        / "tests"
        / "fixtures"
        / "appsscript_tokens"
        / "shopee_tokens_export_example.json"
    )

    out_path = tmp_path / "phase1_failures_2026-02-03.md"
    evidence_path = tmp_path / "phase1_evidence_2026-02-03.md"

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv(
        "DATABASE_URL",
        f"sqlite:///{(tmp_path / 'evidence.db').as_posix()}",
    )
    monkeypatch.setattr(cli.time_module, "time", lambda: 1.5)

    try:
        cli.ops_phase1_evidence_run(
            env_file=str(env_file),
            token_file=str(token_file),
            date_value="2026-02-03",
            shops="samord,minmin",
            transport="fixtures",
            skip_sweep=True,
            artifacts_root=str(artifacts_root),
            out=str(out_path),
            evidence_out=str(evidence_path),
            no_preview=True,
        )
    except typer.Exit as exc:
        assert exc.exit_code == 0

    stdout = capsys.readouterr().out
    assert "run_started_ms=1500" in stdout
    assert "summarize_filter since_ms=1500" in stdout
    assert _count_rows(out_path) == 2

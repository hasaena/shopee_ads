from pathlib import Path
import json
from pathlib import Path

from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app


runner = CliRunner()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_probe_analyze_outputs_and_fallback(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    save_root = tmp_path / "artifacts" / "shopee_api"
    date_value = "20260201"

    success_path = (
        save_root
        / "samord"
        / date_value
        / "1700000000123_shop_info_api_v2_shop_get_shop_info.json"
    )
    _write_json(
        success_path,
        {
            "__meta": {
                "shop_key": "samord",
                "call_name": "shop_info",
                "path": "/api/v2/shop/get_shop_info",
                "http_status": 200,
                "shopee_error": 0,
            },
            "error": 0,
            "access_token": "ACCESS_TOKEN_SECRET",
            "response": {"shop_id": 123, "shop_name": "Demo"},
        },
    )

    fail_path = (
        save_root
        / "minmin"
        / date_value
        / "1700000000999_ads_report_campaign_daily_api_v2_marketing_report.json"
    )
    _write_json(
        fail_path,
        {
            "error": "authorization=SECRET_VALUE",
            "sign": "SHOULD_NOT_SHOW",
        },
    )

    result = runner.invoke(
        app,
        [
            "shopee",
            "probe-analyze",
            "--date",
            date_value,
            "--save-root",
            str(save_root),
            "--only-shops",
            "samord,minmin",
        ],
    )
    assert result.exit_code == 0
    assert "saved_markdown=" in result.output
    assert "saved_csv=" in result.output

    output_root = Path("collaboration") / "artifacts"
    md_path = output_root / f"probe_summary_{date_value}.md"
    csv_path = output_root / f"probe_summary_{date_value}.csv"
    assert md_path.exists()
    assert csv_path.exists()

    csv_text = csv_path.read_text(encoding="utf-8")
    assert "ads_report_campaign_daily" in csv_text
    assert "✅" not in csv_text
    assert "❌" not in csv_text
    assert "SECRET_VALUE" not in csv_text
    assert "ACCESS_TOKEN_SECRET" not in csv_text

    list_result = runner.invoke(
        app,
        [
            "shopee",
            "probe-list",
            "--date",
            date_value,
            "--save-root",
            str(save_root),
            "--only-shops",
            "minmin",
        ],
    )
    assert list_result.exit_code == 0
    assert "SECRET_VALUE" not in list_result.output
    assert "Next actions" in md_path.read_text(encoding="utf-8")


def test_probe_analyze_format_selection(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    save_root = tmp_path / "artifacts" / "shopee_api"
    date_value = "20260201"
    artifact_path = (
        save_root
        / "samord"
        / date_value
        / "1700000000123_shop_info_api_v2_shop_get_shop_info.json"
    )
    _write_json(
        artifact_path,
        {
            "__meta": {
                "shop_key": "samord",
                "call_name": "shop_info",
                "path": "/api/v2/shop/get_shop_info",
                "http_status": 200,
                "shopee_error": 0,
            },
            "error": 0,
        },
    )

    result_md = runner.invoke(
        app,
        [
            "shopee",
            "probe-analyze",
            "--date",
            date_value,
            "--save-root",
            str(save_root),
            "--format",
            "md",
        ],
    )
    assert result_md.exit_code == 0
    md_path = Path("collaboration") / "artifacts" / f"probe_summary_{date_value}.md"
    csv_path = Path("collaboration") / "artifacts" / f"probe_summary_{date_value}.csv"
    assert md_path.exists()
    assert not csv_path.exists()

    result_csv = runner.invoke(
        app,
        [
            "shopee",
            "probe-analyze",
            "--date",
            date_value,
            "--save-root",
            str(save_root),
            "--format",
            "csv",
        ],
    )
    assert result_csv.exit_code == 0
    assert csv_path.exists()


def test_probe_analyze_send_discord(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    save_root = tmp_path / "artifacts" / "shopee_api"
    date_value = "20260201"
    artifact_path = (
        save_root
        / "samord"
        / date_value
        / "1700000000123_shop_info_api_v2_shop_get_shop_info.json"
    )
    _write_json(
        artifact_path,
        {
            "__meta": {
                "shop_key": "samord",
                "call_name": "shop_info",
                "path": "/api/v2/shop/get_shop_info",
                "http_status": 200,
                "shopee_error": 0,
            },
            "error": 0,
            "authorization": "SHOULD_NOT_SHOW",
        },
    )

    sent = {}

    def fake_send(channel, text, shop_label=None, webhook_url=None):
        sent["channel"] = channel
        sent["text"] = text

    monkeypatch.setattr(cli_module, "send", fake_send)

    result = runner.invoke(
        app,
        [
            "shopee",
            "probe-analyze",
            "--date",
            date_value,
            "--save-root",
            str(save_root),
            "--send-discord",
            "--channel",
            "report",
        ],
    )
    assert result.exit_code == 0
    assert sent["channel"] == "report"
    assert "PROBE" in sent["text"]
    assert "SHOULD_NOT_SHOW" not in sent["text"]

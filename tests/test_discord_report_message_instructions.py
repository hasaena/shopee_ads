from __future__ import annotations

from pathlib import Path

from dotori_shopee_automation.scheduler import _build_daily_report_discord_message


def test_report_message_includes_instructions_and_local_file_for_localhost() -> None:
    output_path = Path("D:/python/myproject/dotori_shopee_automation/collaboration/reports/samord/daily/2026-02-25_midday.html")
    message = _build_daily_report_discord_message(
        summary="Bao cao Ads midday 2026-02-25: spend=1.00",
        report_url="http://localhost:8000/reports/samord/daily/2026-02-25_midday.html",
        output_path=output_path,
    )

    assert "Discord không preview file .html" in message
    assert "link report trỏ đến localhost" in message
    assert f"File cục bộ: {output_path.resolve()}" in message


def test_report_message_always_contains_local_file_line() -> None:
    output_path = Path("D:/tmp/minmin_final.html")
    message = _build_daily_report_discord_message(
        summary="Bao cao Ads final 2026-02-24: no_data=1 rows=0",
        report_url="https://reports.example.com/reports/minmin/daily/2026-02-24_final.html",
        output_path=output_path,
    )

    assert "Discord không preview file .html" in message
    assert "link report trỏ đến localhost" not in message
    assert f"File cục bộ: {output_path.resolve()}" in message

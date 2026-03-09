from __future__ import annotations

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import build_report_url


def test_build_report_url_includes_token_and_masks_for_logs(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_BASE_URL", "https://reports.example.com/base")
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "tok en/+")
    get_settings.cache_clear()

    url, url_log = build_report_url("reports/daily-midday/samord/2026-02-25_midday.html")

    assert (
        url
        == "https://reports.example.com/base/reports/daily-midday/samord/2026-02-25_midday.html"
        "?token=tok+en%2F%2B"
    )
    assert (
        url_log
        == "https://reports.example.com/base/reports/daily-midday/samord/2026-02-25_midday.html"
        "?token=***"
    )
    get_settings.cache_clear()


def test_build_report_url_returns_none_when_base_missing(monkeypatch) -> None:
    monkeypatch.delenv("REPORT_BASE_URL", raising=False)
    monkeypatch.delenv("REPORT_ACCESS_TOKEN", raising=False)
    get_settings.cache_clear()

    url, url_log = build_report_url("reports/daily-final/minmin/2026-02-24_final.html")
    assert url is None
    assert url_log is None
    get_settings.cache_clear()


def test_build_report_url_avoids_double_reports_segment(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_BASE_URL", "https://reports.example.com/reports")
    monkeypatch.delenv("REPORT_ACCESS_TOKEN", raising=False)
    get_settings.cache_clear()

    url, url_log = build_report_url("reports/samord/daily/2026-02-25_midday.html")
    assert url == "https://reports.example.com/reports/samord/daily/2026-02-25_midday.html"
    assert url_log == "https://reports.example.com/reports/samord/daily/2026-02-25_midday.html"
    get_settings.cache_clear()

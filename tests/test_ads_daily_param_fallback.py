from __future__ import annotations

from dotori_shopee_automation.ads import provider_live_plan as live_plan


def test_build_ads_daily_params_range() -> None:
    params = live_plan._build_ads_daily_params("2026-02-03", "range")
    assert params == {"start_date": "2026-02-03", "end_date": "2026-02-03"}


def test_build_ads_daily_params_date() -> None:
    params = live_plan._build_ads_daily_params("2026-02-03", "date")
    assert params == {"date": "2026-02-03"}


def test_ads_daily_fallback_range_to_date_on_date_required() -> None:
    seen: list[list[str]] = []

    def request_fn(params: dict[str, str]) -> dict:
        seen.append(sorted(params.keys()))
        if len(seen) == 1:
            return {
                "error": "error_param",
                "message": "date is required",
                "request_id": "r1",
            }
        return {"error": 0, "message": "", "request_id": "r2"}

    payload, params, mode, fmt, attempts = live_plan._call_ads_daily_with_fallback(
        request_fn=request_fn,
        date_iso="2026-02-03",
        initial_mode="range",
        initial_format="iso",
    )

    assert payload.get("error") in (None, 0, "0")
    assert params == {"date": "2026-02-03"}
    assert mode == "date"
    assert fmt == "iso"
    assert attempts == 2
    assert seen == [["end_date", "start_date"], ["date"]]


def test_ads_daily_fallback_date_to_range_on_end_date_required() -> None:
    seen: list[list[str]] = []

    def request_fn(params: dict[str, str]) -> dict:
        seen.append(sorted(params.keys()))
        if len(seen) == 1:
            return {
                "error": "error_param",
                "message": "end_date required",
                "request_id": "r1",
            }
        return {"error": 0, "message": "", "request_id": "r2"}

    payload, params, mode, fmt, attempts = live_plan._call_ads_daily_with_fallback(
        request_fn=request_fn,
        date_iso="2026-02-03",
        initial_mode="date",
        initial_format="iso",
    )

    assert payload.get("error") in (None, 0, "0")
    assert params == {"start_date": "2026-02-03", "end_date": "2026-02-03"}
    assert mode == "range"
    assert fmt == "iso"
    assert attempts == 2
    assert seen == [["date"], ["end_date", "start_date"]]


def test_ads_daily_fallback_iso_to_dmy_on_format_hint() -> None:
    seen: list[dict[str, str]] = []

    def request_fn(params: dict[str, str]) -> dict:
        seen.append(dict(params))
        if len(seen) == 1:
            return {
                "error": "error_param",
                "message": "start_date: is invalid, please pass DD-MM-YYYY format",
            }
        return {"error": 0, "message": ""}

    _payload, _params, _mode, fmt, attempts = live_plan._call_ads_daily_with_fallback(
        request_fn=request_fn,
        date_iso="2026-02-03",
        initial_mode="range",
        initial_format="iso",
    )

    assert attempts == 2
    assert fmt == "dmy"
    assert seen[0]["start_date"] == "2026-02-03"
    assert seen[1]["start_date"] == "03-02-2026"

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_cls, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import json
import os
import hashlib
import re
from typing import Any, Callable
from urllib.parse import quote_plus

from .mapping import (
    MappingConfig,
    CallMapping,
    cast_value,
    coverage_for_plan,
    extract_path,
    load_mapping,
    parse_date_value,
    parse_datetime_value,
)
from .models import AdsAccountBalanceSnapshot
from .provider_base import DailyMetric, SnapshotMetric, Campaign
from .service import (
    _campaigns_from_daily,
    _campaigns_from_snapshot,
    _upsert_campaigns,
    _upsert_daily,
    _upsert_snapshot,
)
from ..config import resolve_timezone
from ..db import EventLog, SessionLocal, init_db
from ..shopee.auth import refresh_access_token
from ..shopee.client import ShopeeClient
from ..shopee.plan import (
    build_artifact_path,
    build_builtin_vars,
    interpolate_data,
    load_plan,
    safe_name,
    safe_path,
)
from ..shopee.signing import build_sign_base, sign_hmac_sha256_hex
from ..shopee.redact import redact_secrets, redact_text
from ..shopee.token_store import get_token, needs_refresh, upsert_token


@dataclass(frozen=True)
class LiveIngestResult:
    calls_ok: int
    calls_fail: int
    campaigns: int
    daily: int
    snapshots: int
    call_failures: list["CallFailure"] = field(default_factory=list)
    failure_artifacts_saved: int = 0
    failure_artifacts_dir: str | None = None


@dataclass(frozen=True)
class CallFailure:
    call_name: str
    http_status: int | None
    api_error: object | None
    api_message: str | None
    request_id: str | None


@dataclass(frozen=True)
class CallResult:
    call_name: str
    ok: bool
    payload: dict[str, Any] | None
    error: str | None
    http_status: int | None
    api_error: object | None
    api_message: str | None
    request_id: str | None
    response_text_head: str | None


def _build_ads_daily_params(date_str: str, mode: str) -> dict[str, str]:
    if mode == "range":
        return {"start_date": date_str, "end_date": date_str}
    if mode == "date":
        return {"date": date_str}
    raise ValueError(f"unknown ads_daily params mode: {mode}")


def _to_dd_mm_yyyy(date_str: str) -> str:
    try:
        parsed = date_cls.fromisoformat(date_str)
    except ValueError:
        return date_str
    return f"{parsed.day:02d}-{parsed.month:02d}-{parsed.year:04d}"


def _format_ads_daily_date(date_iso: str, fmt: str) -> str:
    if fmt == "iso":
        return date_iso
    if fmt == "dmy":
        return _to_dd_mm_yyyy(date_iso)
    raise ValueError(f"unknown ads_daily date format: {fmt}")


def _ads_daily_retry_action(
    *,
    api_error: object | None,
    api_message: object | None,
    attempted_mode: str,
    attempted_format: str,
) -> tuple[str, str] | None:
    # Shopee may accept either (start_date,end_date) or (date) depending on endpoint/version.
    # Some endpoints also require DD-MM-YYYY instead of YYYY-MM-DD.
    # Retry only on explicit param errors.
    if str(api_error or "").strip() != "error_param":
        return None
    msg = str(api_message or "").lower()
    required_markers = ("required", "require", "missing", "invalid")

    # Format hints first: "DD-MM-YYYY format" etc.
    if attempted_format == "iso" and "dd-mm-yyyy" in msg:
        return attempted_mode, "dmy"
    if attempted_format == "dmy" and "yyyy-mm-dd" in msg:
        return attempted_mode, "iso"

    if not any(marker in msg for marker in required_markers):
        return None
    if attempted_mode == "range":
        if re.search(r"\bdate\b", msg):
            return "date", attempted_format
        return None
    if attempted_mode == "date":
        if re.search(r"\bend_date\b", msg) or re.search(r"\bstart_date\b", msg):
            return "range", attempted_format
        return None
    return None


def _call_ads_daily_with_fallback(
    *,
    request_fn: Callable[[dict[str, str]], dict],
    date_iso: str,
    initial_mode: str = "range",
    initial_format: str = "dmy",
    max_attempts: int = 3,
) -> tuple[dict, dict[str, str], str, str, int]:
    mode = initial_mode
    fmt = initial_format
    last_payload: dict = {}
    last_params: dict[str, str] = {}
    attempts = 0
    for _ in range(max_attempts):
        date_str = _format_ads_daily_date(date_iso, fmt)
        params = _build_ads_daily_params(date_str, mode)
        last_params = params
        last_payload = request_fn(params)
        attempts += 1
        api_error, api_message, _request_id = _extract_api_fields(last_payload)
        action = _ads_daily_retry_action(
            api_error=api_error,
            api_message=api_message,
            attempted_mode=mode,
            attempted_format=fmt,
        )
        if not action:
            return last_payload, last_params, mode, fmt, attempts
        next_mode, next_fmt = action
        if next_mode == mode and next_fmt == fmt:
            break
        mode = next_mode
        fmt = next_fmt
    return last_payload, last_params, mode, fmt, attempts


def _is_shopee_ok(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    err = payload.get("error")
    if err is None:
        err = payload.get("error_code")
    if err == "":
        err = None
    return err in (None, 0, "0")


def _is_configured_api_path(value: str | None) -> bool:
    if not value:
        return False
    s = str(value).strip()
    if not s:
        return False
    return "TODO_REPLACE_ME" not in s.upper()


def _resolve_ads_snapshot_perf_path() -> str:
    """
    Live alerts need campaign performance metrics (spend/impr/click/orders/gmv).
    Prefer a dedicated snapshot performance path if configured; otherwise fall back to
    the daily path, and finally a safe default.
    """
    perf_path = os.environ.get("ADS_SNAPSHOT_PERF_PATH", "").strip()
    if _is_configured_api_path(perf_path):
        return perf_path

    daily_path = os.environ.get("ADS_DAILY_PATH", "").strip()
    if _is_configured_api_path(daily_path):
        return daily_path

    return "/api/v2/ads/get_all_cpc_ads_daily_performance"


def _ads_snapshot_wants_performance(planned_api_path: str | None) -> tuple[bool, bool]:
    """
    Decide how to handle the plan's `ads_snapshot` call in live mode.

    Returns (use_perf_flow, override_path):
    - use_perf_flow: run the ads-daily-style param fallback + normalize-to-snapshot-schema
    - override_path: replace the planned path with the resolved perf path (when plan points to an ID list)

    We only override the path when the plan looks like it targets a campaign-id-list endpoint
    (common in the alerts plan). Generic plans/tests may use their own snapshot endpoints and
    should remain unchanged.
    """
    path = (planned_api_path or "").strip()
    if not _is_configured_api_path(path):
        # Misconfigured or empty snapshot path; prefer a safe perf default.
        return True, True

    lower = path.lower()
    is_perf = "performance" in lower
    is_id_list = ("campaign_id_list" in lower or "campaignidlist" in lower) and not is_perf
    if is_id_list:
        return True, True
    if is_perf:
        # Already targeting a perf-ish endpoint; keep the path but still normalize.
        return True, False
    return False, False


def _normalize_campaign_id(raw: object | None) -> str:
    if raw is None:
        return "SHOP_TOTAL"
    value = str(raw).strip()
    if not value:
        return "SHOP_TOTAL"
    if value.lower() == "shop_total":
        return "SHOP_TOTAL"
    return value


def _select_orders_metric(item: dict[str, Any]) -> object | None:
    """
    Normalize order metric with a consistent scope.

    Policy:
    - Prefer explicit order fields when provided by endpoint.
    - Otherwise choose scope by `DOTORI_ADS_METRIC_SCOPE`:
      - `direct` (default): direct_* first, then broad_* fallback
      - `broad`: broad_* first, then direct_* fallback
    """
    explicit = item.get("orders") or item.get("order") or item.get("orders_cnt")
    if explicit is not None:
        return explicit
    scope = os.environ.get("DOTORI_ADS_METRIC_SCOPE", "direct").strip().lower()
    if scope == "broad":
        for key in ("broad_order", "broad_item_sold", "direct_order", "direct_item_sold"):
            value = item.get(key)
            if value is not None:
                return value
        return None
    for key in ("direct_order", "direct_item_sold", "broad_order", "broad_item_sold"):
        value = item.get(key)
        if value is not None:
            return value
    return None


def _select_gmv_metric(item: dict[str, Any]) -> object | None:
    """
    Normalize GMV metric with a consistent scope.

    Policy:
    - Prefer explicit gmv/revenue/sales fields when available.
    - Otherwise choose scope by `DOTORI_ADS_METRIC_SCOPE`:
      - `direct` (default): direct_gmv first, then broad_gmv fallback
      - `broad`: broad_gmv first, then direct_gmv fallback
    """
    explicit = item.get("gmv") or item.get("revenue") or item.get("sales")
    if explicit is not None:
        return explicit
    scope = os.environ.get("DOTORI_ADS_METRIC_SCOPE", "direct").strip().lower()
    if scope == "broad":
        broad_gmv = item.get("broad_gmv")
        if broad_gmv is not None:
            return broad_gmv
        direct_gmv = item.get("direct_gmv")
        if direct_gmv is not None:
            return direct_gmv
        return None
    direct_gmv = item.get("direct_gmv")
    if direct_gmv is not None:
        return direct_gmv
    broad_gmv = item.get("broad_gmv")
    if broad_gmv is not None:
        return broad_gmv
    return None


def _normalize_ads_snapshot_perf_payload(
    payload: dict[str, Any] | None, *, ts_iso: str
) -> dict[str, Any]:
    """
    Normalize a live "performance" response into the snapshot schema expected by mapping:
    - ensure `response.records` exists
    - ensure keys: `campaign_id`, `campaign_name`, `spend_today`, `impressions_today`,
      `clicks_today`, `orders_today`, `gmv_today`, `ts`
    """
    if not isinstance(payload, dict):
        return {}
    resp = payload.get("response")
    if isinstance(resp, list):
        # Some Shopee ads performance endpoints return `response` as a raw list.
        # Normalize to dict/list shape expected by mapping (`response.records`).
        payload = dict(payload)
        payload["response"] = {"records": resp}
        resp = payload.get("response")
    if not isinstance(resp, dict):
        return payload

    records = resp.get("records")
    if not isinstance(records, list):
        # Some endpoints use alternative keys; normalize to records.
        for alt_key in ("record_list", "data", "result", "items", "list"):
            alt = resp.get(alt_key)
            if isinstance(alt, list):
                records = alt
                resp = dict(resp)
                resp["records"] = records
                payload = dict(payload)
                payload["response"] = resp
                break

    records = resp.get("records")
    if not isinstance(records, list):
        return payload

    for item in records:
        if not isinstance(item, dict):
            continue
        # Common id/name shapes
        campaign_id = _normalize_campaign_id(
            item.get("campaign_id") or item.get("campaignId") or item.get("id")
        )
        item["campaign_id"] = campaign_id
        if campaign_id == "SHOP_TOTAL":
            item["campaign_name"] = "SHOP_TOTAL"
        elif not item.get("campaign_name"):
            item["campaign_name"] = (
                item.get("campaignName")
                or item.get("name")
                or item.get("ad_name")
                or "SHOP_TOTAL"
            )

        # Metric key variations: daily endpoints commonly use spend/impressions/clicks/orders/gmv.
        if "spend_today" not in item or item.get("spend_today") is None:
            item["spend_today"] = (
                item.get("spend")
                or item.get("cost")
                or item.get("spend_amt")
                or item.get("expense")
            )
        if "impressions_today" not in item or item.get("impressions_today") is None:
            item["impressions_today"] = (
                item.get("impressions")
                or item.get("impression")
                or item.get("views")
            )
        if "clicks_today" not in item or item.get("clicks_today") is None:
            item["clicks_today"] = item.get("clicks") or item.get("click")
        if "orders_today" not in item or item.get("orders_today") is None:
            item["orders_today"] = _select_orders_metric(item)
        if "gmv_today" not in item or item.get("gmv_today") is None:
            item["gmv_today"] = _select_gmv_metric(item)

        if "ts" not in item or item.get("ts") in (None, ""):
            item["ts"] = ts_iso

    return payload


def _flatten_campaign_metrics_records(
    resp: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """
    Normalize campaign-list shaped daily responses into flat campaign metric rows.

    Some Shopee ads endpoints return:
      response.campaign_list[].metrics_list[]
    instead of response.records[].
    """
    campaign_list = resp.get("campaign_list")
    if not isinstance(campaign_list, list):
        return None

    rows: list[dict[str, Any]] = []
    metric_scalar_keys = (
        "date",
        "start_date",
        "end_date",
        "expense",
        "spend",
        "cost",
        "spend_amt",
        "impression",
        "impressions",
        "views",
        "click",
        "clicks",
        "order",
        "orders",
        "orders_cnt",
        "direct_order",
        "broad_order",
        "gmv",
        "revenue",
        "sales",
        "direct_gmv",
        "broad_gmv",
    )

    for campaign in campaign_list:
        if not isinstance(campaign, dict):
            continue
        campaign_id = _normalize_campaign_id(
            campaign.get("campaign_id")
            or campaign.get("campaignId")
            or campaign.get("id")
        )
        campaign_name = (
            campaign.get("campaign_name")
            or campaign.get("campaignName")
            or campaign.get("ad_name")
        )
        common_info = campaign.get("common_info")
        if not campaign_name and isinstance(common_info, dict):
            campaign_name = (
                common_info.get("ad_name")
                or common_info.get("campaign_name")
                or common_info.get("campaignName")
            )

        metrics_list = (
            campaign.get("metrics_list")
            or campaign.get("metric_list")
            or campaign.get("daily_metrics")
        )
        if isinstance(metrics_list, dict):
            metrics_list = [metrics_list]

        if isinstance(metrics_list, list) and metrics_list:
            for metric in metrics_list:
                if not isinstance(metric, dict):
                    continue
                row = dict(metric)
                row.setdefault("campaign_id", campaign_id)
                if campaign_name:
                    row.setdefault("campaign_name", campaign_name)
                rows.append(row)
            continue

        # Fallback: treat campaign-level metric fields as a single row.
        row = {
            key: campaign.get(key)
            for key in metric_scalar_keys
            if campaign.get(key) is not None
        }
        metric_obj = campaign.get("metrics")
        if isinstance(metric_obj, dict):
            for key, value in metric_obj.items():
                if value is not None:
                    row.setdefault(key, value)
        if row:
            row["campaign_id"] = campaign_id
            if campaign_name:
                row.setdefault("campaign_name", campaign_name)
            rows.append(row)

    if not rows:
        return None
    return rows


def _normalize_ads_daily_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Normalize ads_daily payloads to mapping-friendly campaign rows.

    Live ads endpoints can return a shop-level list with no campaign keys
    (e.g. response=[{date, expense, impression, ...}]). Convert it into
    response.records and inject a sentinel campaign so ads_campaign_daily can upsert.
    """
    if not isinstance(payload, dict):
        return {}

    resp = payload.get("response")
    if isinstance(resp, list):
        payload = dict(payload)
        payload["response"] = {"records": resp}
        resp = payload.get("response")
    if not isinstance(resp, dict):
        return payload

    records = resp.get("records")
    if not isinstance(records, list):
        for alt_key in ("record_list", "data", "result", "items", "list"):
            alt = resp.get(alt_key)
            if isinstance(alt, list):
                records = alt
                resp = dict(resp)
                resp["records"] = records
                payload = dict(payload)
                payload["response"] = resp
                break

    records = resp.get("records")
    if not isinstance(records, list):
        flattened_rows = _flatten_campaign_metrics_records(resp)
        if isinstance(flattened_rows, list):
            resp = dict(resp)
            resp["records"] = flattened_rows
            payload = dict(payload)
            payload["response"] = resp

    records = resp.get("records")
    if not isinstance(records, list):
        return payload

    for item in records:
        if not isinstance(item, dict):
            continue

        campaign_id = _normalize_campaign_id(
            item.get("campaign_id") or item.get("campaignId") or item.get("id")
        )
        item["campaign_id"] = campaign_id
        if campaign_id == "SHOP_TOTAL":
            item["campaign_name"] = "SHOP_TOTAL"
        elif not item.get("campaign_name"):
            item["campaign_name"] = (
                item.get("campaignName")
                or item.get("name")
                or item.get("ad_name")
                or "SHOP_TOTAL"
            )

        if "spend" not in item or item.get("spend") is None:
            item["spend"] = (
                item.get("expense")
                or item.get("cost")
                or item.get("spend_amt")
            )
        if "impressions" not in item or item.get("impressions") is None:
            item["impressions"] = (
                item.get("impression")
                or item.get("views")
            )
        if "clicks" not in item or item.get("clicks") is None:
            item["clicks"] = item.get("click")
        if "orders" not in item or item.get("orders") is None:
            item["orders"] = _select_orders_metric(item)
        if "gmv" not in item or item.get("gmv") is None:
            item["gmv"] = _select_gmv_metric(item)

    return payload


def _ads_daily_needs_campaign_breakdown(payload: dict[str, Any] | None) -> bool:
    """
    Decide whether ads_daily payload needs campaign-level enrichment.

    True when ads_daily effectively contains only SHOP_TOTAL row(s).
    """
    normalized = _normalize_ads_daily_payload(payload)
    records = extract_path(normalized, "response.records")
    if not isinstance(records, list) or not records:
        return False
    has_non_total = False
    for item in records:
        if not isinstance(item, dict):
            continue
        campaign_id = _normalize_campaign_id(
            item.get("campaign_id") or item.get("campaignId") or item.get("id")
        )
        if campaign_id != "SHOP_TOTAL":
            has_non_total = True
            break
    return not has_non_total


def _merge_ads_daily_breakdown_with_shop_total(
    *,
    base_payload: dict[str, Any] | None,
    breakdown_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Keep shop total rows from the original ads_daily payload when campaign breakdown is applied.

    Breakdown endpoints can return campaign/product rows only. To preserve reliable
    shop-level totals for downstream reporting, merge a SHOP_TOTAL row from the
    original ads_daily payload when missing.
    """
    if not isinstance(breakdown_payload, dict):
        return None

    normalized_breakdown = _normalize_ads_daily_payload(breakdown_payload)
    breakdown_records = extract_path(normalized_breakdown, "response.records")
    if not isinstance(breakdown_records, list):
        return normalized_breakdown

    normalized_base = _normalize_ads_daily_payload(base_payload)
    base_records = extract_path(normalized_base, "response.records")
    if not isinstance(base_records, list):
        return normalized_breakdown

    has_shop_total = any(
        isinstance(row, dict)
        and _normalize_campaign_id(
            row.get("campaign_id") or row.get("campaignId") or row.get("id")
        )
        == "SHOP_TOTAL"
        for row in breakdown_records
    )
    if has_shop_total:
        return normalized_breakdown

    shop_total_rows = [
        dict(row)
        for row in base_records
        if isinstance(row, dict)
        and _normalize_campaign_id(
            row.get("campaign_id") or row.get("campaignId") or row.get("id")
        )
        == "SHOP_TOTAL"
    ]
    if not shop_total_rows:
        return normalized_breakdown

    merged_records = list(breakdown_records) + [shop_total_rows[0]]
    out = dict(normalized_breakdown)
    resp = out.get("response")
    if isinstance(resp, dict):
        resp = dict(resp)
    else:
        resp = {}
    resp["records"] = merged_records
    out["response"] = resp
    return out


def _load_campaign_daily_breakdown_fixtures(
    *, fixtures_dir: Path, shop_key: str, target_date: date_cls
) -> dict[str, dict[str, Any] | None]:
    if not fixtures_dir.exists():
        return {
            "campaign_id_list": None,
            "campaign_daily_with_ids": None,
            "campaign_daily_direct": None,
            "all_cpc_daily": None,
        }
    date_iso = target_date.isoformat()

    id_list_candidates: list[Path] = [
        fixtures_dir / f"campaign_id_list_{shop_key}.json",
        fixtures_dir / "campaign_id_list.json",
        fixtures_dir / f"campaign_id_list_forbidden_{shop_key}.json",
        fixtures_dir / "campaign_id_list_forbidden.json",
        fixtures_dir / f"ads_campaign_id_list_{shop_key}.json",
        fixtures_dir / "ads_campaign_id_list.json",
    ]
    perf_with_ids_candidates: list[Path] = [
        fixtures_dir / f"product_campaign_daily_performance_{shop_key}_{date_iso}.json",
        fixtures_dir / f"product_campaign_daily_performance_{shop_key}.json",
        fixtures_dir / "product_campaign_daily_performance.json",
        fixtures_dir / f"ads_campaign_daily_{shop_key}_{date_iso}.json",
        fixtures_dir / f"ads_campaign_daily_{shop_key}.json",
        fixtures_dir / "ads_campaign_daily.json",
    ]
    perf_direct_candidates: list[Path] = [
        fixtures_dir / f"product_campaign_daily_performance_direct_{shop_key}_{date_iso}.json",
        fixtures_dir / f"product_campaign_daily_performance_direct_{shop_key}.json",
        fixtures_dir / "product_campaign_daily_performance_direct.json",
        *perf_with_ids_candidates,
    ]
    all_cpc_candidates: list[Path] = [
        fixtures_dir / f"all_cpc_ads_daily_performance_{shop_key}_{date_iso}.json",
        fixtures_dir / f"all_cpc_ads_daily_performance_{shop_key}.json",
        fixtures_dir / "all_cpc_ads_daily_performance.json",
        fixtures_dir / f"ads_daily_all_cpc_{shop_key}_{date_iso}.json",
        fixtures_dir / f"ads_daily_all_cpc_{shop_key}.json",
        fixtures_dir / "ads_daily_all_cpc.json",
    ]

    def pick_first(candidates: list[Path]) -> dict[str, Any] | None:
        for path in candidates:
            if path.exists():
                return _read_json(path)
        return None

    return {
        "campaign_id_list": pick_first(id_list_candidates),
        "campaign_daily_with_ids": pick_first(perf_with_ids_candidates),
        "campaign_daily_direct": pick_first(perf_direct_candidates),
        "all_cpc_daily": pick_first(all_cpc_candidates),
    }


def _extract_http_status_from_exception(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    try:
        return int(status) if status is not None else None
    except Exception:  # noqa: BLE001
        return None


def _campaign_daily_payload_stats(
    payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, int]]:
    normalized = _normalize_ads_daily_payload(payload if isinstance(payload, dict) else None)
    records = extract_path(normalized, "response.records")
    if not isinstance(records, list):
        records = []
    items_total = 0
    has_campaign_id_field = 0
    non_total_campaign_rows = 0
    for item in records:
        if not isinstance(item, dict):
            continue
        items_total += 1
        campaign_id = _normalize_campaign_id(
            item.get("campaign_id") or item.get("campaignId") or item.get("id")
        )
        if campaign_id:
            has_campaign_id_field = 1
            if campaign_id != "SHOP_TOTAL":
                non_total_campaign_rows += 1
    stats = {
        "items_total": items_total,
        "has_campaign_id_field": has_campaign_id_field,
        "non_total_campaign_rows": non_total_campaign_rows,
    }
    return normalized, stats


def _campaign_daily_endpoint_success(stats: dict[str, int]) -> bool:
    return (
        int(stats.get("items_total", 0)) > 0
        and int(stats.get("has_campaign_id_field", 0)) == 1
        and int(stats.get("non_total_campaign_rows", 0)) >= 1
    )


def _campaign_daily_endpoint_reason(
    *,
    default_reason: str,
    stats: dict[str, int],
) -> str:
    if int(stats.get("items_total", 0)) <= 0:
        return "empty_campaign_daily_response"
    if int(stats.get("has_campaign_id_field", 0)) == 0:
        return "campaign_id_missing"
    if int(stats.get("non_total_campaign_rows", 0)) == 0:
        return "shop_total_only"
    return default_reason


def _append_campaign_daily_endpoint_result(
    *,
    meta: dict[str, Any],
    endpoint: str,
    ok: bool,
    reason: str,
    http_status: int | None,
    api_error: object | None,
    api_message: object | None,
    payload: dict[str, Any] | None,
    ids_total: int = 0,
    chunks: int = 0,
    request_id: str | None = None,
) -> None:
    if request_id is None and isinstance(payload, dict):
        _err, _msg, payload_request_id = _extract_api_fields(payload)
        request_id = payload_request_id
    normalized, stats = _campaign_daily_payload_stats(payload)
    result = {
        "order": len(meta["endpoint_results"]) + 1,
        "endpoint": endpoint,
        "ok": 1 if ok else 0,
        "selected": 1 if ok else 0,
        "reason": reason,
        "http_status": http_status,
        "api_error": api_error,
        "api_message": redact_text(str(api_message)) if api_message not in (None, "") else None,
        "items_total": int(stats.get("items_total", 0)),
        "has_campaign_id_field": int(stats.get("has_campaign_id_field", 0)),
        "non_total_campaign_rows": int(stats.get("non_total_campaign_rows", 0)),
        "ids_total": int(ids_total),
        "chunks": int(chunks),
        "request_id": request_id if request_id else None,
    }
    meta["endpoint_results"].append(result)
    if isinstance(payload, dict):
        meta["endpoint_payloads"].append(
            {
                "endpoint": endpoint,
                "order": result["order"],
                "payload": normalized,
            }
        )
    if http_status == 403:
        meta["blocked_403"] = True

    if ok:
        meta["ok"] = True
        meta["reason"] = reason
        meta["records_total"] = int(stats.get("items_total", 0))
        meta["selected_endpoint"] = endpoint
        meta["api_error"] = None
        meta["api_message"] = None
        meta["http_status"] = None
        return

    meta["ok"] = False
    meta["reason"] = reason
    meta["api_error"] = api_error
    meta["api_message"] = redact_text(str(api_message)) if api_message not in (None, "") else None
    meta["http_status"] = http_status


def _campaign_breakdown_cooldown_hours() -> int:
    raw = os.environ.get(
        "DOTORI_ADS_CAMPAIGN_BREAKDOWN_403_COOLDOWN_HOURS", "24"
    ).strip()
    if not raw:
        return 24
    try:
        value = int(float(raw))
    except Exception:  # noqa: BLE001
        return 24
    if value < 0:
        return 0
    return value


def _campaign_breakdown_max_campaigns() -> int:
    raw = os.environ.get("ADS_CAMPAIGN_DAILY_MAX_IDS", "").strip()
    if not raw:
        return 600
    try:
        value = int(float(raw))
    except Exception:  # noqa: BLE001
        return 600
    if value < 1:
        return 1
    return value


def _campaign_breakdown_chunk_size() -> int:
    raw = os.environ.get("ADS_CAMPAIGN_DAILY_CHUNK_SIZE", "").strip()
    if not raw:
        return 50
    try:
        value = int(float(raw))
    except Exception:  # noqa: BLE001
        return 50
    if value < 1:
        return 1
    if value > 50:
        return 50
    return value


def _to_utc(dt_value: datetime | None) -> datetime | None:
    if not isinstance(dt_value, datetime):
        return None
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc)


def _parse_utc_iso(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_latest_campaign_breakdown_status_meta(
    session,
    shop_key: str,
) -> tuple[dict[str, Any] | None, datetime | None]:
    rows = (
        session.query(EventLog.meta_json, EventLog.created_at)
        .filter(EventLog.message == "ads_campaign_breakdown_status")
        .order_by(EventLog.id.desc())
        .limit(200)
        .all()
    )
    for row in rows:
        try:
            meta_json = row[0]
            created_at = row[1]
        except Exception:  # noqa: BLE001
            meta_json = getattr(row, "meta_json", None)
            created_at = getattr(row, "created_at", None)
        if not meta_json:
            continue
        try:
            payload = json.loads(meta_json)
        except Exception:  # noqa: BLE001
            continue
        if str(payload.get("shop_key") or "") != shop_key:
            continue
        return payload, _to_utc(created_at)
    return None, None


def _campaign_breakdown_cooldown_state(
    *,
    session,
    shop_key: str,
    now_utc: datetime,
) -> dict[str, Any]:
    meta, created_at_utc = _read_latest_campaign_breakdown_status_meta(session, shop_key)
    if not isinstance(meta, dict):
        return {"active": False}

    blocked_403 = int(meta.get("blocked_403") or 0) == 1
    if not blocked_403:
        return {"active": False}

    cooldown_until = _parse_utc_iso(meta.get("cooldown_until_utc"))
    if cooldown_until is None:
        hours = _campaign_breakdown_cooldown_hours()
        if hours <= 0:
            return {"active": False}
        base = created_at_utc or now_utc
        cooldown_until = base + timedelta(hours=hours)
    active = cooldown_until > now_utc
    return {
        "active": active,
        "blocked_403": blocked_403,
        "cooldown_until_utc": cooldown_until,
        "reason": str(meta.get("reason") or "-"),
        "attempted_endpoints": list(meta.get("attempted_endpoints") or []),
    }


def _compute_cooldown_until_utc(*, now_utc: datetime, blocked_403: bool) -> str | None:
    if not blocked_403:
        return None
    hours = _campaign_breakdown_cooldown_hours()
    if hours <= 0:
        return now_utc.isoformat()
    return (now_utc + timedelta(hours=hours)).isoformat()


def _fetch_campaign_daily_breakdown_payload(
    *,
    client: ShopeeClient | None,
    shop_key: str,
    shop_id: int,
    access_token: str | None,
    target_date: date_cls,
    fixtures_dir: Path | None,
    max_campaigns: int = 50,
    chunk_size: int = 50,
    try_alt_endpoints: bool = True,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """
    Fetch campaign-level daily performance with endpoint fallback strategy.

    Returns (payload, meta). payload is None when enrichment is unavailable.
    """
    meta: dict[str, Any] = {
        "ok": False,
        "reason": "unknown",
        "ids_total": 0,
        "chunks": 0,
        "records_total": 0,
        "api_error": None,
        "api_message": None,
        "http_status": None,
        "selected_endpoint": None,
        "blocked_403": False,
        "endpoint_results": [],
        "endpoint_payloads": [],
    }

    def can_try_more() -> bool:
        return bool(try_alt_endpoints)

    # Strategy 1: campaign id list + campaign daily performance
    if fixtures_dir is not None:
        fixture_payloads = _load_campaign_daily_breakdown_fixtures(
            fixtures_dir=fixtures_dir,
            shop_key=shop_key,
            target_date=target_date,
        )
        id_list_payload = fixture_payloads.get("campaign_id_list")
        with_ids_payload = fixture_payloads.get("campaign_daily_with_ids")
        direct_payload = fixture_payloads.get("campaign_daily_direct")
        all_cpc_payload = fixture_payloads.get("all_cpc_daily")

        campaign_ids, _name_map = _extract_campaign_ids_and_names(id_list_payload)
        meta["ids_total"] = len(campaign_ids)
        if isinstance(id_list_payload, dict) and _is_shopee_ok(id_list_payload) and campaign_ids:
            normalized_with_ids, with_ids_stats = _campaign_daily_payload_stats(
                with_ids_payload if isinstance(with_ids_payload, dict) else None
            )
            if _campaign_daily_endpoint_success(with_ids_stats):
                meta["chunks"] = 1
                _append_campaign_daily_endpoint_result(
                    meta=meta,
                    endpoint="get_product_campaign_daily_performance_with_id_list",
                    ok=True,
                    reason="campaign_rows_detected",
                    http_status=200,
                    api_error=None,
                    api_message=None,
                    payload=normalized_with_ids,
                    ids_total=len(campaign_ids),
                    chunks=1,
                )
                return normalized_with_ids, meta
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_with_id_list",
                ok=False,
                reason=_campaign_daily_endpoint_reason(
                    default_reason="campaign_rows_not_detected",
                    stats=with_ids_stats,
                ),
                http_status=200,
                api_error=None,
                api_message=None,
                payload=normalized_with_ids,
                ids_total=len(campaign_ids),
                chunks=1,
            )
        else:
            api_error, api_message, _request_id = _extract_api_fields(
                id_list_payload if isinstance(id_list_payload, dict) else None
            )
            reason = "fixture_missing_campaign_id_list"
            if isinstance(id_list_payload, dict):
                if not _is_shopee_ok(id_list_payload):
                    reason = "campaign_id_list_api_error"
                elif not campaign_ids:
                    reason = "campaign_id_list_empty"
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_with_id_list",
                ok=False,
                reason=reason,
                http_status=403 if str(api_error or "").strip() == "forbidden" else None,
                api_error=api_error,
                api_message=api_message,
                payload=id_list_payload if isinstance(id_list_payload, dict) else None,
                ids_total=len(campaign_ids),
                chunks=0,
            )
            if not can_try_more():
                return None, meta

        # Strategy 2: direct product campaign daily performance (no id list)
        normalized_direct, direct_stats = _campaign_daily_payload_stats(
            direct_payload if isinstance(direct_payload, dict) else None
        )
        if _campaign_daily_endpoint_success(direct_stats):
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_direct",
                ok=True,
                reason="campaign_rows_detected",
                http_status=200,
                api_error=None,
                api_message=None,
                payload=normalized_direct,
            )
            return normalized_direct, meta
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_product_campaign_daily_performance_direct",
            ok=False,
            reason=_campaign_daily_endpoint_reason(
                default_reason="campaign_rows_not_detected",
                stats=direct_stats,
            ),
            http_status=200 if isinstance(direct_payload, dict) else None,
            api_error=None if isinstance(direct_payload, dict) else "fixture_missing",
            api_message=None
            if isinstance(direct_payload, dict)
            else "campaign_daily_direct fixture not found",
            payload=normalized_direct if isinstance(direct_payload, dict) else None,
        )
        if not can_try_more():
            return None, meta

        # Strategy 3: all-cpc daily performance
        normalized_all_cpc, all_cpc_stats = _campaign_daily_payload_stats(
            all_cpc_payload if isinstance(all_cpc_payload, dict) else None
        )
        if _campaign_daily_endpoint_success(all_cpc_stats):
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_all_cpc_ads_daily_performance",
                ok=True,
                reason="campaign_rows_detected",
                http_status=200,
                api_error=None,
                api_message=None,
                payload=normalized_all_cpc,
            )
            return normalized_all_cpc, meta
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_all_cpc_ads_daily_performance",
            ok=False,
            reason=_campaign_daily_endpoint_reason(
                default_reason="campaign_rows_not_detected",
                stats=all_cpc_stats,
            ),
            http_status=200 if isinstance(all_cpc_payload, dict) else None,
            api_error=None if isinstance(all_cpc_payload, dict) else "fixture_missing",
            api_message=None
            if isinstance(all_cpc_payload, dict)
            else "all_cpc_ads_daily_performance fixture not found",
            payload=normalized_all_cpc if isinstance(all_cpc_payload, dict) else None,
        )
        return None, meta

    if client is None or not access_token:
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_product_campaign_daily_performance_with_id_list",
            ok=False,
            reason="missing_client_or_token",
            http_status=None,
            api_error=None,
            api_message=None,
            payload=None,
        )
        return None, meta

    def _campaign_param_candidates(ids: list[str]) -> list[tuple[str, str]]:
        values: list[tuple[str, str]] = [
            ("comma", ",".join(ids)),
            ("json_str", json.dumps(ids, ensure_ascii=True)),
        ]
        if ids and all(str(x).isdigit() for x in ids):
            values.append(
                ("json_int", json.dumps([int(x) for x in ids], ensure_ascii=True))
            )
        return values

    ts = int(datetime.now(timezone.utc).timestamp())
    date_iso = target_date.isoformat()
    campaign_ids: list[str] = []
    name_map: dict[str, str] = {}

    id_list_payload: dict[str, Any] | None = None
    id_list_http_status: int | None = None
    id_list_exception_message: str | None = None
    try:
        id_list_payload = client.request(
            "GET",
            "/api/v2/ads/get_product_level_campaign_id_list",
            shop_id=shop_id,
            access_token=access_token,
            params=None,
            json=None,
            timestamp=ts,
        )
    except Exception as exc:  # noqa: BLE001
        id_list_http_status = _extract_http_status_from_exception(exc)
        id_list_exception_message = redact_text(str(exc))

    if isinstance(id_list_payload, dict):
        api_error, api_message, _request_id = _extract_api_fields(id_list_payload)
        campaign_ids, name_map = _extract_campaign_ids_and_names(id_list_payload)
        raw_cap = os.environ.get("ADS_CAMPAIGN_DAILY_MAX_IDS", "").strip()
        cap = max_campaigns
        if raw_cap:
            try:
                parsed = int(raw_cap)
            except ValueError:
                parsed = cap
            if parsed > 0:
                cap = parsed
        campaign_ids = campaign_ids[:cap]
        meta["ids_total"] = len(campaign_ids)

        if not _is_shopee_ok(id_list_payload):
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_with_id_list",
                ok=False,
                reason="campaign_id_list_api_error",
                http_status=id_list_http_status,
                api_error=api_error,
                api_message=api_message,
                payload=id_list_payload,
                ids_total=len(campaign_ids),
                chunks=0,
            )
        elif not campaign_ids:
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_with_id_list",
                ok=False,
                reason="campaign_id_list_empty",
                http_status=200,
                api_error=None,
                api_message=None,
                payload=id_list_payload,
                ids_total=0,
                chunks=0,
            )
        else:
            raw_chunk = os.environ.get("ADS_CAMPAIGN_DAILY_CHUNK_SIZE", "").strip()
            chunk = chunk_size
            if raw_chunk:
                try:
                    parsed_chunk = int(raw_chunk)
                except ValueError:
                    parsed_chunk = chunk
                if parsed_chunk > 0:
                    chunk = parsed_chunk
            if chunk < 1:
                chunk = 1
            if chunk > 50:
                chunk = 50
            chunks: list[list[str]] = [
                campaign_ids[i : i + chunk] for i in range(0, len(campaign_ids), chunk)
            ]
            meta["chunks"] = len(chunks)

            merged_records: list[dict[str, Any]] = []
            filtered_unknown_campaign_rows = 0
            chunk_api_error: object | None = None
            chunk_api_message: object | None = None
            chunk_http_status: int | None = None
            chosen_fmt: str | None = None
            chunk_failed = False
            for ids in chunks:
                valid_ids = {str(x).strip() for x in ids if str(x).strip()}
                chunk_payload: dict[str, Any] | None = None
                for fmt_name, campaign_value in _campaign_param_candidates(ids):
                    if chosen_fmt and fmt_name != chosen_fmt:
                        continue

                    def _request_fn(date_params: dict[str, str]) -> dict:
                        params = dict(date_params)
                        params["campaign_id_list"] = campaign_value
                        return client.request(
                            "GET",
                            "/api/v2/ads/get_product_campaign_daily_performance",
                            shop_id=shop_id,
                            access_token=access_token,
                            params=params or None,
                            json=None,
                            timestamp=ts,
                        )

                    try:
                        payload, _used_params, mode, fmt, attempts = _call_ads_daily_with_fallback(
                            request_fn=_request_fn,
                            date_iso=date_iso,
                            initial_mode="range",
                            initial_format="dmy",
                        )
                    except Exception as exc:  # noqa: BLE001
                        chunk_payload = None
                        chunk_api_error = "request_failed"
                        chunk_api_message = redact_text(str(exc))
                        chunk_http_status = _extract_http_status_from_exception(exc)
                        break

                    api_error, api_message, _request_id = _extract_api_fields(payload)
                    if _is_shopee_ok(payload):
                        chunk_payload = payload
                        chosen_fmt = fmt_name
                        if attempts > 1:
                            print(
                                "campaign_daily_param_fallback "
                                f"shop={shop_key} mode={mode} fmt={fmt} attempts={attempts}"
                            )
                        break
                    chunk_api_error = api_error
                    chunk_api_message = api_message
                    if str(api_error or "").strip() == "error_param":
                        msg = str(api_message or "").lower()
                        if "campaign_id_list" in msg or "campaignidlist" in msg:
                            continue
                    break

                if not isinstance(chunk_payload, dict):
                    chunk_failed = True
                    break

                normalized_chunk = _normalize_ads_daily_payload(chunk_payload)
                rows = extract_path(normalized_chunk, "response.records")
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    cid = str(row.get("campaign_id") or "").strip()
                    if not cid or cid not in valid_ids:
                        filtered_unknown_campaign_rows += 1
                        continue
                    if cid and not row.get("campaign_name"):
                        fallback_name = name_map.get(cid)
                        if fallback_name:
                            row["campaign_name"] = fallback_name
                    merged_records.append(row)

            if filtered_unknown_campaign_rows > 0:
                meta["filtered_unknown_campaign_rows"] = filtered_unknown_campaign_rows

            merged_payload: dict[str, Any] | None = None
            if merged_records:
                merged_payload = {
                    "error": "",
                    "message": "",
                    "response": {"records": merged_records},
                }
            normalized_merged, merged_stats = _campaign_daily_payload_stats(merged_payload)
            if (not chunk_failed) and _campaign_daily_endpoint_success(merged_stats):
                _append_campaign_daily_endpoint_result(
                    meta=meta,
                    endpoint="get_product_campaign_daily_performance_with_id_list",
                    ok=True,
                    reason="campaign_rows_detected",
                    http_status=200,
                    api_error=None,
                    api_message=None,
                    payload=normalized_merged,
                    ids_total=len(campaign_ids),
                    chunks=len(chunks),
                )
                return normalized_merged, meta

            reason = _campaign_daily_endpoint_reason(
                default_reason="campaign_rows_not_detected",
                stats=merged_stats,
            )
            if chunk_failed and chunk_api_error not in (None, ""):
                reason = f"campaign_daily_chunk_error_{chunk_api_error}"
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_with_id_list",
                ok=False,
                reason=reason,
                http_status=chunk_http_status or 200,
                api_error=chunk_api_error,
                api_message=chunk_api_message,
                payload=normalized_merged if merged_records else None,
                ids_total=len(campaign_ids),
                chunks=len(chunks),
            )
    else:
        reason = "campaign_id_list_request_failed"
        if id_list_http_status == 403:
            reason = "campaign_id_list_forbidden"
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_product_campaign_daily_performance_with_id_list",
            ok=False,
            reason=reason,
            http_status=id_list_http_status,
            api_error="request_failed",
            api_message=id_list_exception_message,
            payload=None,
            ids_total=0,
            chunks=0,
        )

    if not can_try_more():
        return None, meta

    # Strategy 2: direct `get_product_campaign_daily_performance` without id list.
    direct_payload: dict[str, Any] | None = None
    direct_api_error: object | None = None
    direct_api_message: object | None = None
    direct_http_status: int | None = None
    try:
        def _request_direct(date_params: dict[str, str]) -> dict:
            return client.request(
                "GET",
                "/api/v2/ads/get_product_campaign_daily_performance",
                shop_id=shop_id,
                access_token=access_token,
                params=date_params or None,
                json=None,
                timestamp=ts,
            )

        direct_payload, _used_params, _mode, _fmt, _attempts = _call_ads_daily_with_fallback(
            request_fn=_request_direct,
            date_iso=date_iso,
            initial_mode="range",
            initial_format="dmy",
        )
    except Exception as exc:  # noqa: BLE001
        direct_api_error = "request_failed"
        direct_api_message = redact_text(str(exc))
        direct_http_status = _extract_http_status_from_exception(exc)

    if isinstance(direct_payload, dict):
        direct_api_error, direct_api_message, _request_id = _extract_api_fields(direct_payload)
        normalized_direct, direct_stats = _campaign_daily_payload_stats(direct_payload)
        if _is_shopee_ok(direct_payload) and _campaign_daily_endpoint_success(direct_stats):
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_product_campaign_daily_performance_direct",
                ok=True,
                reason="campaign_rows_detected",
                http_status=200,
                api_error=None,
                api_message=None,
                payload=normalized_direct,
            )
            return normalized_direct, meta
        reason = (
            f"direct_api_error_{direct_api_error}"
            if not _is_shopee_ok(direct_payload)
            else _campaign_daily_endpoint_reason(
                default_reason="campaign_rows_not_detected",
                stats=direct_stats,
            )
        )
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_product_campaign_daily_performance_direct",
            ok=False,
            reason=reason,
            http_status=200,
            api_error=direct_api_error if not _is_shopee_ok(direct_payload) else None,
            api_message=direct_api_message if not _is_shopee_ok(direct_payload) else None,
            payload=normalized_direct,
        )
    else:
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_product_campaign_daily_performance_direct",
            ok=False,
            reason="direct_request_failed",
            http_status=direct_http_status,
            api_error=direct_api_error,
            api_message=direct_api_message,
            payload=None,
        )

    if not can_try_more():
        return None, meta

    # Strategy 3: fallback to all-cpc daily performance.
    all_cpc_payload: dict[str, Any] | None = None
    all_cpc_api_error: object | None = None
    all_cpc_api_message: object | None = None
    all_cpc_http_status: int | None = None
    try:
        def _request_all_cpc(date_params: dict[str, str]) -> dict:
            return client.request(
                "GET",
                "/api/v2/ads/get_all_cpc_ads_daily_performance",
                shop_id=shop_id,
                access_token=access_token,
                params=date_params or None,
                json=None,
                timestamp=ts,
            )

        all_cpc_payload, _used_params, _mode, _fmt, _attempts = _call_ads_daily_with_fallback(
            request_fn=_request_all_cpc,
            date_iso=date_iso,
            initial_mode="range",
            initial_format="dmy",
        )
    except Exception as exc:  # noqa: BLE001
        all_cpc_api_error = "request_failed"
        all_cpc_api_message = redact_text(str(exc))
        all_cpc_http_status = _extract_http_status_from_exception(exc)

    if isinstance(all_cpc_payload, dict):
        all_cpc_api_error, all_cpc_api_message, _request_id = _extract_api_fields(all_cpc_payload)
        normalized_all_cpc, all_cpc_stats = _campaign_daily_payload_stats(all_cpc_payload)
        if _is_shopee_ok(all_cpc_payload) and _campaign_daily_endpoint_success(all_cpc_stats):
            _append_campaign_daily_endpoint_result(
                meta=meta,
                endpoint="get_all_cpc_ads_daily_performance",
                ok=True,
                reason="campaign_rows_detected",
                http_status=200,
                api_error=None,
                api_message=None,
                payload=normalized_all_cpc,
            )
            return normalized_all_cpc, meta
        reason = (
            f"all_cpc_api_error_{all_cpc_api_error}"
            if not _is_shopee_ok(all_cpc_payload)
            else _campaign_daily_endpoint_reason(
                default_reason="campaign_rows_not_detected",
                stats=all_cpc_stats,
            )
        )
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_all_cpc_ads_daily_performance",
            ok=False,
            reason=reason,
            http_status=200,
            api_error=all_cpc_api_error if not _is_shopee_ok(all_cpc_payload) else None,
            api_message=all_cpc_api_message if not _is_shopee_ok(all_cpc_payload) else None,
            payload=normalized_all_cpc,
        )
    else:
        _append_campaign_daily_endpoint_result(
            meta=meta,
            endpoint="get_all_cpc_ads_daily_performance",
            ok=False,
            reason="all_cpc_request_failed",
            http_status=all_cpc_http_status,
            api_error=all_cpc_api_error,
            api_message=all_cpc_api_message,
            payload=None,
        )
    return None, meta


def _extract_campaign_ids_and_names(
    payload: dict[str, Any] | None,
) -> tuple[list[str], dict[str, str]]:
    """
    Extract campaign ids (and best-effort names) from the live Ads "campaign id list" response.

    Shopee Ads responses are not consistent across endpoints/versions; in practice we have seen
    this endpoint return either a `records` list or a dedicated `campaign_id_list`/`campaign_list`.
    Keep this function tolerant so the downstream "setting_info requires campaign_id_list" call
    can be satisfied without hardcoding a single response schema.
    """

    if not isinstance(payload, dict):
        return [], {}

    resp = payload.get("response")
    # Some endpoints return arrays directly; accept both dict and list.
    if not isinstance(resp, (dict, list)):
        return [], {}

    ids: list[str] = []
    names: dict[str, str] = {}
    seen: set[str] = set()

    def add_id_and_name(campaign_id: object, campaign_name: object | None) -> None:
        if campaign_id is None:
            return
        cid = str(campaign_id).strip()
        if not cid:
            return
        if cid in seen:
            return
        seen.add(cid)
        ids.append(cid)
        if campaign_name is not None:
            name = str(campaign_name).strip()
            if name:
                names[cid] = name

    def ingest_items(items: object) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if isinstance(item, dict):
                campaign_id = (
                    item.get("campaign_id")
                    or item.get("campaignId")
                    or item.get("campaignID")
                    or item.get("id")
                )
                campaign_name = (
                    item.get("campaign_name")
                    or item.get("campaignName")
                    or item.get("campaign")
                    or item.get("name")
                )
                add_id_and_name(campaign_id, campaign_name)
            else:
                add_id_and_name(item, None)

    def ingest_value(value: object) -> None:
        # Supports: list, JSON array string, comma-separated string.
        if isinstance(value, list):
            ingest_items(value)
            return
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return
            if s.startswith("[") and s.endswith("]"):
                try:
                    decoded = json.loads(s)
                except Exception:  # noqa: BLE001
                    decoded = None
                if isinstance(decoded, list):
                    ingest_items(decoded)
                    return
            if "," in s:
                parts = [p.strip() for p in s.split(",")]
                for part in parts:
                    if part:
                        add_id_and_name(part, None)
                return
            add_id_and_name(s, None)

    if isinstance(resp, list):
        ingest_items(resp)
        return ids, names

    # Common schemas.
    for key in (
        "records",
        "campaign_id_list",
        "campaign_ids",
        "campaign_list",
        "campaigns",
    ):
        value = resp.get(key)
        ingest_value(value)
        if ids:
            return ids, names

    # Heuristic fallback: scan response dict values for a list that contains campaign-like objects.
    for value in resp.values():
        if isinstance(value, list) and value:
            ingest_items(value)
            if ids:
                break
        ingest_value(value)
        if ids:
            break

    # Guardrail: live payloads can occasionally include sample/non-numeric ids (e.g. c1/c2)
    # mixed with real numeric Shopee campaign ids. When numeric ids are present, keep only
    # numeric ids to avoid polluting live DB/report rows with fixture-like campaigns.
    numeric_ids = [cid for cid in ids if cid.isdigit()]
    has_long_numeric = any(len(cid) >= 6 for cid in numeric_ids)
    if has_long_numeric and len(numeric_ids) < len(ids):
        keep = set(numeric_ids)
        ids = [cid for cid in ids if cid in keep]
        names = {cid: name for cid, name in names.items() if cid in keep}

    return ids, names


def _normalize_campaign_records(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort normalization for campaign setting/list payloads:
    - ensure `payload.response.records` exists when the API uses alternative list keys.
    """
    resp = payload.get("response")
    if not isinstance(resp, dict):
        return payload
    records = resp.get("records")
    if isinstance(records, list):
        return payload
    for alt_key in (
        "campaign_setting_info",
        "campaign_setting_list",
        "campaign_settings",
        "campaign_list",
        "campaigns",
    ):
        alt = resp.get(alt_key)
        if isinstance(alt, list):
            resp = dict(resp)
            resp["records"] = alt
            out = dict(payload)
            out["response"] = resp
            return out
    return payload


def _call_ads_campaign_setting_info_with_id_list(
    *,
    client: ShopeeClient,
    method: str,
    setting_info_path: str,
    shop_id: int,
    access_token: str,
    base_params: dict[str, Any],
    body: dict[str, Any] | None,
    timestamp: int,
    cached_id_list: dict[str, Any] | None,
    max_campaigns: int = 2000,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """
    Shopee Ads setting info endpoint requires `campaign_id_list` param.
    We auto-fetch `/api/v2/ads/get_product_level_campaign_id_list` and then call
    `/api/v2/ads/get_product_level_campaign_setting_info` with a best-effort param format.
    """
    id_list_payload = cached_id_list
    if id_list_payload is None:
        try:
            id_list_payload = client.request(
                "GET",
                "/api/v2/ads/get_product_level_campaign_id_list",
                shop_id=shop_id,
                access_token=access_token,
                params=None,
                json=None,
                timestamp=timestamp,
            )
        except Exception:  # noqa: BLE001
            id_list_payload = None

    campaign_ids, name_map = _extract_campaign_ids_and_names(id_list_payload)

    # Hard cap: avoid unbounded live API calls. Operator can override with env.
    cap = max_campaigns
    raw_cap = os.environ.get("ADS_CAMPAIGN_LIST_MAX_IDS", "").strip()
    if raw_cap:
        try:
            parsed = int(raw_cap)
        except ValueError:
            parsed = cap
        if parsed > 0:
            cap = parsed
    campaign_ids_total = len(campaign_ids)
    if campaign_ids_total > cap:
        if all(str(cid).isdigit() for cid in campaign_ids):
            # Keep the highest numeric ids first (usually the most recently created campaigns).
            # This avoids dropping active/new campaigns when the account has large history.
            campaign_ids = sorted(campaign_ids, key=lambda x: int(x), reverse=True)[:cap]
        else:
            # Best effort fallback for non-numeric ids: prefer the tail of the list.
            campaign_ids = campaign_ids[-cap:]

    # If we can't get any campaign ids, fall back to the caller's params.
    if not campaign_ids:
        payload = client.request(
            method,
            setting_info_path,
            shop_id=shop_id,
            access_token=access_token,
            params=base_params or None,
            json=body,
            timestamp=timestamp,
        )
        return payload, base_params, id_list_payload

    chunk_size = 50
    chunks: list[list[str]] = [
        campaign_ids[i : i + chunk_size] for i in range(0, len(campaign_ids), chunk_size)
    ]
    print(
        "campaign_setting_info_chunks "
        f"shop_id={shop_id} total_ids={len(campaign_ids)} "
        f"chunks={len(chunks)} chunk_size={chunk_size} cap={cap} "
        f"ids_before_cap={campaign_ids_total}"
    )

    # Live API also requires `info_type_list` for this endpoint. We don't know the exact enum
    # mapping ahead of time, so try a small, low-risk set of common formats.
    info_type_values: list[str] = [
        "1",
        json.dumps([1], ensure_ascii=True),
        json.dumps([1, 2, 3, 4, 5], ensure_ascii=True),
        "0",
        json.dumps([0], ensure_ascii=True),
    ]

    def _campaign_param_candidates(ids: list[str]) -> list[tuple[str, str]]:
        values: list[tuple[str, str]] = [
            ("comma", ",".join(ids)),
            ("json_str", json.dumps(ids, ensure_ascii=True)),
        ]
        if ids and all(str(x).isdigit() for x in ids):
            values.append(("json_int", json.dumps([int(x) for x in ids], ensure_ascii=True)))
        return values

    chosen_campaign_fmt: str | None = None
    chosen_info_value: str | None = None

    def _request_chunk(ids: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal chosen_campaign_fmt, chosen_info_value

        # Determine a working param format on first chunk; reuse for the rest to keep
        # the number of live HTTP calls bounded.
        if chosen_campaign_fmt and chosen_info_value:
            if chosen_campaign_fmt == "comma":
                campaign_value = ",".join(ids)
            elif chosen_campaign_fmt == "json_int":
                campaign_value = json.dumps([int(x) for x in ids], ensure_ascii=True)
            else:
                campaign_value = json.dumps(ids, ensure_ascii=True)
            params = dict(base_params)
            params["campaign_id_list"] = campaign_value
            params["info_type_list"] = chosen_info_value
            payload = client.request(
                method,
                setting_info_path,
                shop_id=shop_id,
                access_token=access_token,
                params=params or None,
                json=body,
                timestamp=timestamp,
            )
            if _is_shopee_ok(payload):
                return payload, params
            # Fall back to discovery mode for this chunk if reuse failed.

        last_payload_local: dict[str, Any] = {}
        last_params_local: dict[str, Any] = dict(base_params)
        for fmt_name, campaign_value in _campaign_param_candidates(ids):
            for info_value in info_type_values:
                params = dict(base_params)
                params["campaign_id_list"] = campaign_value
                params["info_type_list"] = info_value
                payload = client.request(
                    method,
                    setting_info_path,
                    shop_id=shop_id,
                    access_token=access_token,
                    params=params or None,
                    json=body,
                    timestamp=timestamp,
                )
                last_payload_local = payload
                last_params_local = params
                if _is_shopee_ok(payload):
                    chosen_campaign_fmt = fmt_name
                    chosen_info_value = info_value
                    return payload, params
                api_error, api_message, _request_id = _extract_api_fields(payload)
                if str(api_error or "").strip() != "error_param":
                    return payload, params
                msg = str(api_message or "").lower()
                if "info_type_list" in msg or "infotypelist" in msg:
                    continue
                if "campaign_id_list" in msg or "campaignidlist" in msg:
                    break
                return payload, params
        return last_payload_local, last_params_local

    merged_payload: dict[str, Any] | None = None
    merged_params: dict[str, Any] = dict(base_params)
    merged_records: list[Any] = []

    for chunk in chunks:
        chunk_payload, chunk_params = _request_chunk(chunk)
        merged_params = chunk_params

        normalized = _normalize_campaign_records(chunk_payload)
        chunk_records = extract_path(normalized, "response.records")
        if merged_payload is None:
            merged_payload = normalized
        if isinstance(chunk_records, list):
            merged_records.extend(chunk_records)

        if not _is_shopee_ok(chunk_payload):
            # Stop early on a hard failure to avoid spamming the API.
            break

    if merged_payload is None:
        merged_payload = {}

    merged_payload = _normalize_campaign_records(merged_payload)
    resp = merged_payload.get("response")
    if isinstance(resp, dict):
        resp = dict(resp)
        resp["records"] = merged_records
        merged_payload = dict(merged_payload)
        merged_payload["response"] = resp

    normalized = merged_payload
    records = extract_path(normalized, "response.records")
    if isinstance(records, list):
        for item in records:
            if not isinstance(item, dict):
                continue

            # Live `...campaign_setting_info` returns a nested structure, e.g.
            # `common_info.ad_name`, `common_info.campaign_status`, `common_info.campaign_budget`.
            # Our mapping expects flat keys (`campaign_name`, `status`, `daily_budget`), so we
            # mirror them here to keep fixtures + mappings stable.
            common = item.get("common_info")
            if isinstance(common, dict):
                if not item.get("campaign_name"):
                    item["campaign_name"] = (
                        common.get("ad_name")
                        or common.get("campaign_name")
                        or common.get("campaignName")
                    )
                if not item.get("status"):
                    item["status"] = common.get("campaign_status") or common.get(
                        "status"
                    )
                if "daily_budget" not in item or item.get("daily_budget") is None:
                    budget = common.get("campaign_budget")
                    if budget is None:
                        budget = common.get("daily_budget")
                    if budget is not None:
                        item["daily_budget"] = budget

            # If we have an id->name map (from id list), fill in missing campaign_name.
            if name_map:
                cid = item.get("campaign_id")
                if cid and not item.get("campaign_name"):
                    name = name_map.get(str(cid))
                    if name:
                        item["campaign_name"] = name
    return normalized, merged_params, id_list_payload


def ingest_ads_live(
    *,
    shop_cfg,
    settings,
    target_date: date_cls,
    plan_path: Path,
    mapping_path: Path,
    save_artifacts: bool = False,
    save_failure_artifacts: bool = False,
    dry_run: bool = False,
    strict_mapping: bool = False,
    fixtures_dir: Path | None = None,
    save_root: Path | None = None,
    token_mode: str = "default",
    client_factory: Callable[[object], ShopeeClient] | None = None,
) -> LiveIngestResult:
    plan_def = load_plan(plan_path)
    mapping = load_mapping(mapping_path)

    planned_calls = [call.name for call in plan_def.calls]
    mapped_count, unmapped = coverage_for_plan(mapping, planned_calls)
    print(f"planned_calls: {', '.join(planned_calls)}")
    print(
        "mapping_coverage: "
        f"mapped={mapped_count} unmapped={len(unmapped)} missing=[{', '.join(unmapped)}]"
    )
    if unmapped:
        print(f"mapping_warning: missing=[{', '.join(unmapped)}]")
        if strict_mapping:
            print(f"strict_mapping_missing: {', '.join(unmapped)}")
            raise ValueError("Strict mapping failed due to unmapped calls.")

    vars_map = build_builtin_vars(shop_cfg.shop_key, shop_cfg.shopee_shop_id or 0)
    vars_map.update(_build_date_vars(target_date))

    if dry_run:
        _print_dry_run(plan_def.calls, mapping)
        return LiveIngestResult(0, 0, 0, 0, 0)

    client_factory = client_factory or _build_shopee_client
    client = client_factory(settings) if fixtures_dir is None else None

    init_db()
    session = SessionLocal()
    try:
        token = None
        if fixtures_dir is None:
            token = get_token(session, shop_cfg.shop_key)
            if token is None:
                raise RuntimeError("no token found; run shopee exchange-code first")

        call_results: list[CallResult] = []
        call_failures: list[CallFailure] = []
        calls_ok = 0
        calls_fail = 0
        failure_artifacts_saved = 0
        failure_artifacts_dir = (
            str(_resolve_failure_artifacts_root()) if save_failure_artifacts else None
        )
        # Cache for expensive or repeated sub-requests within a single ingest run.
        cached_product_level_campaign_id_list: dict[str, Any] | None = None
        campaign_breakdown_status: dict[str, Any] | None = None

        for call in plan_def.calls:
            if token and needs_refresh(token.access_token_expires_at):
                if token_mode == "passive":
                    raise RuntimeError(
                        "token_expired_refresh_disabled "
                        f"shop={shop_cfg.shop_key} shop_id={shop_cfg.shopee_shop_id}"
                    )
                refreshed = refresh_access_token(
                    client,
                    settings.shopee_partner_id,
                    settings.shopee_partner_key,
                    shop_cfg.shopee_shop_id,
                    token.refresh_token,
                    int(datetime.now().timestamp()),
                )
                upsert_token(
                    session,
                    shop_cfg.shop_key,
                    refreshed.shop_id,
                    refreshed.access_token,
                    refreshed.refresh_token,
                    refreshed.access_expires_at,
                )
                session.commit()
                token = get_token(session, shop_cfg.shop_key)

            planned_api_path = interpolate_data(call.path, vars_map)
            api_path = planned_api_path
            params = interpolate_data(call.params, vars_map)
            ads_daily_date_str: str | None = None
            if call.name == "ads_daily":
                # Prefer range params by default to satisfy the common "end_date required" case.
                ads_daily_date_str = str(
                    vars_map.get("date") or target_date.isoformat()
                ).strip()
                params = _build_ads_daily_params(ads_daily_date_str, "range")
            ads_snapshot_perf_date_str: str | None = None
            body = interpolate_data(call.body, vars_map) if call.body else None
            if fixtures_dir is None and call.name == "ads_snapshot":
                # Alerts need performance metrics (spend/impr/click/orders/gmv). Some plans define
                # ads_snapshot as an ID-list endpoint; in that case we override to a perf endpoint.
                use_perf_flow, override_path = _ads_snapshot_wants_performance(planned_api_path)
                if use_perf_flow:
                    if override_path:
                        api_path = _resolve_ads_snapshot_perf_path()
                    ads_snapshot_perf_date_str = str(
                        vars_map.get("date") or target_date.isoformat()
                    ).strip()
            timestamp = int(datetime.now(timezone.utc).timestamp())
            safe_fingerprint = _build_safe_fingerprint(
                access_token=token.access_token if token else None,
                partner_id=settings.shopee_partner_id,
                partner_key=settings.shopee_partner_key,
                path=api_path,
                timestamp=timestamp,
                shop_id=shop_cfg.shopee_shop_id,
            )
            access_token_debug = _build_access_token_encoding_flags(
                token.access_token if token else None
            )

            response = None
            ok = True
            error_text: str | None = None
            http_status = None
            shopee_error = None
            response_json: dict | None = None
            response_text_head: str | None = None
            api_error = None
            api_message = None
            request_id = None
            query_keys = _build_query_keys(
                params,
                shop_id=shop_cfg.shopee_shop_id,
                access_token=token.access_token if token else None,
            )
            try:
                if fixtures_dir is not None:
                    response = _load_fixture_payload(fixtures_dir, call.name)
                    if response is None:
                        ok = False
                        error_text = f"fixture not found for call {call.name}"
                        http_status = 404
                    else:
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(
                            response_json
                        )
                else:
                    _validate_outgoing_access_token(
                        access_token=token.access_token if token else None,
                        shop_key=shop_cfg.shop_key,
                        path=api_path,
                    )
                    if "/api/v2/shop/get_shop_info" in api_path:
                        token_len = len(token.access_token) if token else 0
                        token_sha8 = _sha256_8(token.access_token if token else None)
                        print(
                            f"token_source=db shop={shop_cfg.shop_key} "
                            f"token_len={token_len} token_sha8={token_sha8}"
                        )
                    # If we already fetched campaign id list as a sub-request earlier in this ingest run,
                    # reuse it to avoid additional live HTTP calls.
                    if call.name == "ads_snapshot" and ads_snapshot_perf_date_str:
                        def _request_ads_snapshot_perf(p: dict[str, str]) -> dict:
                            return client.request(
                                call.method,
                                api_path,
                                shop_id=shop_cfg.shopee_shop_id,
                                access_token=token.access_token,
                                params=p or None,
                                json=body,
                                timestamp=timestamp,
                            )

                        response, params, mode, fmt, attempts = _call_ads_daily_with_fallback(
                            request_fn=_request_ads_snapshot_perf,
                            date_iso=ads_snapshot_perf_date_str,
                            initial_mode="range",
                            initial_format="dmy",
                        )
                        if attempts > 1:
                            print(
                                "ads_snapshot_perf_param_fallback "
                                f"shop={shop_cfg.shop_key} mode={mode} fmt={fmt} attempts={attempts}"
                            )
                        ts_iso = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                        response = _normalize_ads_snapshot_perf_payload(
                            response if isinstance(response, dict) else None, ts_iso=ts_iso
                        )
                        query_keys = _build_query_keys(
                            params,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token if token else None,
                        )
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(
                            response_json
                        )
                    # If we already fetched campaign id list as a sub-request earlier in this ingest run,
                    # reuse it to avoid additional live HTTP calls.
                    elif (
                        call.name == "ads_snapshot"
                        and cached_product_level_campaign_id_list is not None
                        and api_path.strip() == "/api/v2/ads/get_product_level_campaign_id_list"
                    ):
                        response = cached_product_level_campaign_id_list
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(response_json)
                    elif call.name == "ads_daily" and ads_daily_date_str:
                        def _request_ads_daily(p: dict[str, str]) -> dict:
                            return client.request(
                                call.method,
                                api_path,
                                shop_id=shop_cfg.shopee_shop_id,
                                access_token=token.access_token,
                                params=p or None,
                                json=body,
                                timestamp=timestamp,
                            )

                        response, params, mode, fmt, attempts = _call_ads_daily_with_fallback(
                            request_fn=_request_ads_daily,
                            date_iso=ads_daily_date_str,
                            initial_mode="range",
                            initial_format="dmy",
                        )
                        if attempts > 1:
                            # Secret-free trace; helps understand which param shape succeeded.
                            print(
                                "ads_daily_param_fallback "
                                f"shop={shop_cfg.shop_key} mode={mode} fmt={fmt} attempts={attempts}"
                            )
                        query_keys = _build_query_keys(
                            params,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token if token else None,
                        )
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(
                            response_json
                        )
                    elif (
                        call.name == "ads_campaign_list"
                        and api_path.strip() == "/api/v2/ads/get_product_level_campaign_setting_info"
                    ):
                        (
                            response,
                            params,
                            cached_product_level_campaign_id_list,
                        ) = _call_ads_campaign_setting_info_with_id_list(
                            client=client,
                            method=call.method,
                            setting_info_path=api_path,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token,
                            base_params=params or {},
                            body=body,
                            timestamp=timestamp,
                            cached_id_list=cached_product_level_campaign_id_list,
                        )
                        query_keys = _build_query_keys(
                            params,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token if token else None,
                        )
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(
                            response_json
                        )
                    if call.name == "ads_daily" and ads_daily_date_str:
                        pass
                    elif call.name == "ads_snapshot" and ads_snapshot_perf_date_str:
                        pass
                    elif response is None:
                        response = client.request(
                            call.method,
                            api_path,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token,
                            params=params or None,
                            json=body,
                            timestamp=timestamp,
                        )
                        http_status = 200
                        response_json = response if isinstance(response, dict) else None
                        api_error, api_message, request_id = _extract_api_fields(
                            response_json
                        )
            except Exception as exc:  # noqa: BLE001
                ok = False
                if hasattr(exc, "response") and exc.response is not None:
                    http_status = getattr(exc.response, "status_code", None)
                    response_json, response_text_head = _parse_response_payload(
                        exc.response
                    )
                    api_error, api_message, request_id = _extract_api_fields(
                        response_json
                    )
                    if http_status:
                        error_text = f"HTTP {http_status}"
                if not error_text:
                    error_text = redact_text(str(exc))
            if api_message is None and response_text_head:
                api_message = response_text_head

            if ok and isinstance(response, dict):
                shopee_error = response.get("error")
                if shopee_error == "":
                    shopee_error = None
                if shopee_error not in (None, 0, "0"):
                    ok = False
                    message = response.get("message") or response.get("msg") or "-"
                    error_text = f"Shopee API error {shopee_error}: {message}"
                    api_error, api_message, request_id = _extract_api_fields(
                        response_json or response
                    )
                elif call.name == "ads_daily" and _ads_daily_needs_campaign_breakdown(response):
                    now_utc = datetime.now(timezone.utc)
                    cooldown_state = (
                        _campaign_breakdown_cooldown_state(
                            session=session,
                            shop_key=shop_cfg.shop_key,
                            now_utc=now_utc,
                        )
                        if fixtures_dir is None
                        else {"active": False}
                    )
                    if bool(cooldown_state.get("active")):
                        cooldown_until_utc = cooldown_state.get("cooldown_until_utc")
                        cooldown_text = (
                            cooldown_until_utc.isoformat()
                            if isinstance(cooldown_until_utc, datetime)
                            else "-"
                        )
                        print(
                            "campaign_breakdown_skip_due_to_cooldown=1 "
                            f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
                            f"cooldown_until_utc={cooldown_text} "
                            f"reason={cooldown_state.get('reason', '-')}"
                        )
                        campaign_breakdown_status = {
                            "shop_key": shop_cfg.shop_key,
                            "shop_label": shop_cfg.label,
                            "shop_id": shop_cfg.shopee_shop_id,
                            "date": target_date.isoformat(),
                            "blocked_403": 1,
                            "status": "cooldown_skip",
                            "reason": "cooldown_active",
                            "selected_endpoint": None,
                            "attempted_endpoints": list(
                                cooldown_state.get("attempted_endpoints") or []
                            ),
                            "endpoint_results": [],
                            "cooldown_until_utc": cooldown_text,
                        }
                    else:
                        breakdown_payload, breakdown_meta = _fetch_campaign_daily_breakdown_payload(
                            client=client,
                            shop_key=shop_cfg.shop_key,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token if token else None,
                            target_date=target_date,
                            fixtures_dir=fixtures_dir,
                            max_campaigns=_campaign_breakdown_max_campaigns(),
                            chunk_size=_campaign_breakdown_chunk_size(),
                            try_alt_endpoints=True,
                        )
                        endpoint_results = breakdown_meta.get("endpoint_results", []) or []
                        for endpoint_result in endpoint_results:
                            if int(endpoint_result.get("ok", 0)) == 1:
                                continue
                            endpoint_name = str(endpoint_result.get("endpoint") or "-")
                            endpoint_reason = str(endpoint_result.get("reason") or "-")
                            endpoint_http = endpoint_result.get("http_status")
                            endpoint_api_error = endpoint_result.get("api_error")
                            endpoint_api_message = endpoint_result.get("api_message")
                            print(
                                "campaign_breakdown_softfail=1 "
                                f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
                                f"endpoint={endpoint_name} reason={endpoint_reason} "
                                f"http_status={endpoint_http if endpoint_http is not None else '-'} "
                                f"api_error={endpoint_api_error if endpoint_api_error not in (None, '') else '-'} "
                                f"api_message={redact_text(str(endpoint_api_message or '-'))}"
                            )
                        attempted_endpoints = [
                            str(row.get("endpoint") or "-") for row in endpoint_results
                        ]
                        blocked_403 = bool(breakdown_meta.get("blocked_403"))
                        cooldown_until = _compute_cooldown_until_utc(
                            now_utc=now_utc,
                            blocked_403=blocked_403,
                        )
                        if isinstance(breakdown_payload, dict):
                            response = _merge_ads_daily_breakdown_with_shop_total(
                                base_payload=response,
                                breakdown_payload=breakdown_payload,
                            )
                            response_json = response if isinstance(response, dict) else breakdown_payload
                            selected_endpoint = str(
                                breakdown_meta.get("selected_endpoint") or "-"
                            )
                            print(
                                "ads_daily_campaign_breakdown_applied "
                                f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
                                f"endpoint={selected_endpoint} "
                                f"records={breakdown_meta.get('records_total', 0)} "
                                f"ids={breakdown_meta.get('ids_total', 0)} "
                                f"chunks={breakdown_meta.get('chunks', 0)}"
                            )
                            campaign_breakdown_status = {
                                "shop_key": shop_cfg.shop_key,
                                "shop_label": shop_cfg.label,
                                "shop_id": shop_cfg.shopee_shop_id,
                                "date": target_date.isoformat(),
                                "blocked_403": 1 if blocked_403 else 0,
                                "status": "applied",
                                "reason": str(breakdown_meta.get("reason") or "-"),
                                "selected_endpoint": selected_endpoint,
                                "attempted_endpoints": attempted_endpoints,
                                "endpoint_results": [
                                    {
                                        "endpoint": str(row.get("endpoint") or "-"),
                                        "ok": int(row.get("ok", 0)),
                                        "reason": str(row.get("reason") or "-"),
                                        "http_status": row.get("http_status"),
                                        "request_id": row.get("request_id"),
                                    }
                                    for row in endpoint_results
                                ],
                                "cooldown_until_utc": cooldown_until,
                            }
                        else:
                            api_error_text = (
                                "-"
                                if breakdown_meta.get("api_error") in (None, "")
                                else str(breakdown_meta.get("api_error"))
                            )
                            api_message_text = (
                                "-"
                                if breakdown_meta.get("api_message") in (None, "")
                                else str(breakdown_meta.get("api_message"))
                            )
                            print(
                                "ads_daily_campaign_breakdown_skipped "
                                f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
                                f"reason={breakdown_meta.get('reason', '-')} "
                                f"api_error={api_error_text} "
                                f"api_message={redact_text(api_message_text)}"
                            )
                            campaign_breakdown_status = {
                                "shop_key": shop_cfg.shop_key,
                                "shop_label": shop_cfg.label,
                                "shop_id": shop_cfg.shopee_shop_id,
                                "date": target_date.isoformat(),
                                "blocked_403": 1 if blocked_403 else 0,
                                "status": "skipped",
                                "reason": str(breakdown_meta.get("reason") or "-"),
                                "selected_endpoint": str(
                                    breakdown_meta.get("selected_endpoint") or "-"
                                ),
                                "attempted_endpoints": attempted_endpoints,
                                "endpoint_results": [
                                    {
                                        "endpoint": str(row.get("endpoint") or "-"),
                                        "ok": int(row.get("ok", 0)),
                                        "reason": str(row.get("reason") or "-"),
                                        "http_status": row.get("http_status"),
                                        "request_id": row.get("request_id"),
                                    }
                                    for row in endpoint_results
                                ],
                                "cooldown_until_utc": cooldown_until,
                            }

            if ok:
                calls_ok += 1
            else:
                calls_fail += 1

            if save_artifacts:
                _save_artifact(
                    shop_cfg.shop_key,
                    call,
                    api_path,
                    params,
                    body,
                    response,
                    ok,
                    error_text,
                    http_status,
                    shopee_error,
                    save_root=save_root,
                )

            if save_failure_artifacts and (http_status != 200 or not ok):
                artifact_path = _save_failure_artifact(
                    shop_key=shop_cfg.shop_key,
                    target_date=target_date,
                    call_name=call.name,
                    api_path=api_path,
                    method=call.method,
                    query_keys=query_keys,
                    http_status=http_status,
                    api_error=api_error,
                    api_message=api_message,
                    request_id=request_id,
                    response_json=response_json,
                    response_text_head=response_text_head,
                    safe_fingerprint=safe_fingerprint,
                    access_token_debug=access_token_debug,
                )
                if artifact_path:
                    failure_artifacts_saved += 1

            call_results.append(
                CallResult(
                    call_name=call.name,
                    ok=ok,
                    payload=response if isinstance(response, dict) else None,
                    error=error_text,
                    http_status=http_status,
                    api_error=api_error,
                    api_message=api_message,
                    request_id=request_id,
                    response_text_head=response_text_head,
                )
            )
            if not ok:
                call_failures.append(
                    CallFailure(
                        call_name=call.name,
                        http_status=http_status,
                        api_error=api_error,
                        api_message=api_message,
                        request_id=request_id,
                    )
                )

        daily_rows, snapshot_rows, campaign_rows = _extract_rows(
            call_results,
            mapping,
            target_date,
        )
        balance_rows = _extract_account_balance_rows(call_results)

        campaigns = _merge_campaigns(daily_rows, snapshot_rows, campaign_rows)
        campaigns_count = _upsert_campaigns(session, shop_cfg.shop_key, campaigns)
        daily_count = _upsert_daily(session, shop_cfg.shop_key, daily_rows)
        snapshot_count = _upsert_snapshot(session, shop_cfg.shop_key, snapshot_rows)
        _upsert_account_balance_snapshot(session, shop_cfg.shop_key, balance_rows)
        if campaign_breakdown_status:
            session.add(
                EventLog(
                    level="INFO",
                    message="ads_campaign_breakdown_status",
                    meta_json=_dump_json(campaign_breakdown_status),
                )
            )
        session.commit()

        return LiveIngestResult(
            calls_ok=calls_ok,
            calls_fail=calls_fail,
            campaigns=campaigns_count,
            daily=daily_count,
            snapshots=snapshot_count,
            call_failures=call_failures,
            failure_artifacts_saved=failure_artifacts_saved,
            failure_artifacts_dir=failure_artifacts_dir,
        )
    finally:
        session.close()


def _extract_rows(
    call_results: list[CallResult],
    mapping: MappingConfig,
    target_date: date_cls,
) -> tuple[list[DailyMetric], list[SnapshotMetric], list[Campaign]]:
    daily_rows: list[DailyMetric] = []
    snapshot_rows: list[SnapshotMetric] = []
    campaign_rows: list[Campaign] = []
    for call in call_results:
        call_mapping = mapping.calls.get(call.call_name)
        if call_mapping is None or not call.ok or not call.payload:
            continue
        if call_mapping.type == "daily":
            normalized_daily = _normalize_ads_daily_payload(call.payload)
            daily_rows.extend(_parse_daily(normalized_daily, call_mapping, target_date))
        elif call_mapping.type == "snapshot":
            snapshot_rows.extend(_parse_snapshot(call.payload, call_mapping))
        elif call_mapping.type == "campaign":
            campaign_rows.extend(_parse_campaign(call.payload, call_mapping))
    return daily_rows, snapshot_rows, campaign_rows


def _extract_account_balance_rows(call_results: list[CallResult]) -> list[tuple[datetime, Decimal]]:
    rows: list[tuple[datetime, Decimal]] = []
    now = datetime.now(timezone.utc)
    for call in call_results:
        if call.call_name != "ads_total_balance":
            continue
        if not call.ok or not isinstance(call.payload, dict):
            continue
        total_balance = _extract_total_balance(call.payload)
        if total_balance is None:
            continue
        ts_value = parse_datetime_value(
            extract_path(call.payload, "response.updated_at")
            or extract_path(call.payload, "response.ts")
            or extract_path(call.payload, "response.timestamp")
            or extract_path(call.payload, "updated_at")
            or extract_path(call.payload, "ts")
            or extract_path(call.payload, "timestamp"),
            now,
        )
        rows.append((ts_value, total_balance))
    return rows


def _extract_total_balance(payload: dict[str, Any]) -> Decimal | None:
    candidates = [
        extract_path(payload, "response.total_balance"),
        extract_path(payload, "response.balance"),
        extract_path(payload, "response.available_balance"),
        extract_path(payload, "response.remaining_balance"),
        extract_path(payload, "response.ads_balance"),
        extract_path(payload, "total_balance"),
        extract_path(payload, "balance"),
        extract_path(payload, "available_balance"),
        extract_path(payload, "remaining_balance"),
        extract_path(payload, "ads_balance"),
    ]
    for candidate in candidates:
        value = _to_decimal(candidate)
        if value is not None:
            return value

    records = extract_path(payload, "response.records")
    if not isinstance(records, list):
        records = extract_path(payload, "response")
    if isinstance(records, list):
        total = Decimal("0")
        found = False
        for record in records:
            if not isinstance(record, dict):
                continue
            value = (
                _to_decimal(record.get("total_balance"))
                or _to_decimal(record.get("balance"))
                or _to_decimal(record.get("available_balance"))
                or _to_decimal(record.get("remaining_balance"))
                or _to_decimal(record.get("amount"))
            )
            if value is None:
                continue
            total += value
            found = True
        if found:
            return total
    return None


def _upsert_account_balance_snapshot(
    session,
    shop_key: str,
    rows: list[tuple[datetime, Decimal]],
) -> int:
    count = 0
    for ts_value, total_balance in rows:
        existing = (
            session.query(AdsAccountBalanceSnapshot)
            .filter_by(shop_key=shop_key, ts=ts_value)
            .one_or_none()
        )
        if existing:
            existing.total_balance = total_balance
        else:
            session.add(
                AdsAccountBalanceSnapshot(
                    shop_key=shop_key,
                    ts=ts_value,
                    total_balance=total_balance,
                )
            )
        count += 1
    return count


def _parse_campaign(payload: dict[str, Any], mapping: CallMapping) -> list[Campaign]:
    records = _resolve_records(payload, mapping.records_path)
    rows: list[Campaign] = []
    for record in records:
        campaign_id = extract_path(record, mapping.campaign_id_path)
        campaign_name = extract_path(record, mapping.campaign_name_path)
        if campaign_id is None or campaign_name is None:
            continue
        status = extract_path(record, mapping.status_path) if mapping.status_path else None
        daily_budget = None
        if mapping.daily_budget_path:
            daily_budget = cast_value(
                extract_path(record, mapping.daily_budget_path), "decimal", None
            )
        rows.append(
            Campaign(
                campaign_id=str(campaign_id),
                campaign_name=str(campaign_name),
                status=str(status) if status is not None else None,
                daily_budget=_to_decimal(daily_budget),
            )
        )
    return rows


def _parse_daily(
    payload: dict[str, Any],
    mapping: CallMapping,
    target_date: date_cls,
) -> list[DailyMetric]:
    records = _resolve_records(payload, mapping.records_path)
    rows: list[DailyMetric] = []
    for record in records:
        campaign_id = extract_path(record, mapping.campaign_id_path)
        campaign_name = extract_path(record, mapping.campaign_name_path)
        if campaign_id is None or campaign_name is None:
            continue
        status = extract_path(record, mapping.status_path) if mapping.status_path else None
        daily_budget = None
        if mapping.daily_budget_path:
            daily_budget = cast_value(
                extract_path(record, mapping.daily_budget_path), "decimal", None
            )
        date_value = parse_date_value(
            extract_path(record, mapping.date_path), target_date
        )
        # The daily ingest call is executed for a single target day. Some payloads return
        # stale/alternate date strings (or fixture-fixed dates), which can make reports look
        # empty for the requested day. Keep ads_daily rows aligned to target_date.
        if mapping.name == "ads_daily" and date_value != target_date:
            date_value = target_date
        metrics = _extract_metrics(record, mapping.fields)
        rows.append(
            DailyMetric(
                campaign_id=str(campaign_id),
                campaign_name=str(campaign_name),
                status=str(status) if status is not None else None,
                daily_budget=_to_decimal(daily_budget),
                date=date_value,
                spend=metrics["spend"],
                impressions=int(metrics["impressions"]),
                clicks=int(metrics["clicks"]),
                orders=int(metrics["orders"]),
                gmv=metrics["gmv"],
            )
        )
    return rows


def _parse_snapshot(
    payload: dict[str, Any],
    mapping: CallMapping,
) -> list[SnapshotMetric]:
    records = _resolve_records(payload, mapping.records_path)
    rows: list[SnapshotMetric] = []
    now = datetime.now(timezone.utc)
    for record in records:
        campaign_id = extract_path(record, mapping.campaign_id_path)
        campaign_name = extract_path(record, mapping.campaign_name_path)
        if campaign_id is None or campaign_name is None:
            continue
        status = extract_path(record, mapping.status_path) if mapping.status_path else None
        daily_budget = None
        if mapping.daily_budget_path:
            daily_budget = cast_value(
                extract_path(record, mapping.daily_budget_path), "decimal", None
            )
        ts_value = parse_datetime_value(
            extract_path(record, mapping.timestamp_path), now
        )
        metrics = _extract_metrics(record, mapping.fields)
        rows.append(
            SnapshotMetric(
                campaign_id=str(campaign_id),
                campaign_name=str(campaign_name),
                status=str(status) if status is not None else None,
                daily_budget=_to_decimal(daily_budget),
                ts=ts_value,
                spend_today=metrics["spend"],
                impressions_today=int(metrics["impressions"]),
                clicks_today=int(metrics["clicks"]),
                orders_today=int(metrics["orders"]),
                gmv_today=metrics["gmv"],
            )
        )
    return rows


def _extract_metrics(record: Any, fields: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name, field in fields.items():
        raw = extract_path(record, field.path)
        values[name] = cast_value(raw, field.cast, field.default)
    spend = _to_decimal(values.get("spend"))
    gmv = _to_decimal(values.get("gmv"))
    values["spend"] = spend
    values["gmv"] = gmv
    values["impressions"] = int(values.get("impressions", 0))
    values["clicks"] = int(values.get("clicks", 0))
    values["orders"] = int(values.get("orders", 0))
    return values


def _resolve_records(payload: dict[str, Any], records_path: str | None) -> list[dict]:
    if records_path:
        data = extract_path(payload, records_path)
    else:
        data = payload
    if data is None:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _merge_campaigns(
    daily_rows: list[DailyMetric],
    snapshot_rows: list[SnapshotMetric],
    campaign_rows: list[Campaign] | None = None,
) -> list[Campaign]:
    campaigns: dict[str, Campaign] = {}
    for campaign in _campaigns_from_daily(daily_rows):
        campaigns[campaign.campaign_id] = _merge_campaign(campaigns.get(campaign.campaign_id), campaign)
    for campaign in _campaigns_from_snapshot(snapshot_rows):
        campaigns[campaign.campaign_id] = _merge_campaign(campaigns.get(campaign.campaign_id), campaign)
    for campaign in campaign_rows or []:
        campaigns[campaign.campaign_id] = _merge_campaign(campaigns.get(campaign.campaign_id), campaign)
    return list(campaigns.values())


def _merge_campaign(existing: Campaign | None, incoming: Campaign) -> Campaign:
    if existing is None:
        return incoming
    name = incoming.campaign_name.strip() if incoming.campaign_name else ""
    merged_name = name or existing.campaign_name
    merged_status = incoming.status if incoming.status is not None else existing.status
    merged_budget = (
        incoming.daily_budget if incoming.daily_budget is not None else existing.daily_budget
    )
    return Campaign(
        campaign_id=incoming.campaign_id,
        campaign_name=merged_name,
        status=merged_status,
        daily_budget=merged_budget,
    )


def _build_date_vars(target_date: date_cls) -> dict[str, str]:
    date_str = target_date.isoformat()
    date_from = target_date.isoformat()
    date_to = target_date.isoformat()
    timestamp = int(
        datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=resolve_timezone("Asia/Ho_Chi_Minh"),
        ).timestamp()
    )
    return {
        "date": date_str,
        "date_from": date_from,
        "date_to": date_to,
        "timestamp": str(timestamp),
    }


def _build_shopee_client(settings) -> ShopeeClient:
    return ShopeeClient(
        partner_id=settings.shopee_partner_id,
        partner_key=settings.shopee_partner_key,
        host=settings.shopee_api_host,
    )


def _extract_api_fields(payload: dict | None) -> tuple[object | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None, None
    api_error = payload.get("error")
    if api_error is None:
        api_error = payload.get("error_code")
    if api_error == "":
        api_error = None
    api_message = payload.get("message")
    if api_message is None:
        api_message = payload.get("msg")
    if api_message is None:
        api_message = payload.get("error_msg")
    if api_message == "":
        api_message = None
    request_id = payload.get("request_id")
    if request_id is None:
        request_id = payload.get("requestId")
    if request_id is None:
        request_id = payload.get("requestid")
    return api_error, api_message, request_id


def _parse_response_payload(response) -> tuple[dict | None, str | None]:
    payload = None
    text_head = None
    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        payload = None
    if payload is None:
        try:
            text_head = response.text
        except Exception:  # noqa: BLE001
            text_head = None
    if text_head:
        text_head = redact_text(text_head[:200])
    return payload, text_head


def _build_query_keys(
    params: dict | None,
    *,
    shop_id: int | None,
    access_token: str | None,
) -> list[str]:
    keys = {"partner_id", "timestamp", "sign"}
    if shop_id is not None:
        keys.add("shop_id")
    if access_token is not None:
        keys.add("access_token")
    if params:
        keys.update(str(key) for key in params.keys())
    return sorted(keys)


def _sha256_8(value: str | None) -> str:
    if not value:
        return "-"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _build_access_token_encoding_flags(access_token: str | None) -> dict[str, int]:
    if not access_token:
        return {
            "token_has_plus": 0,
            "token_has_slash": 0,
            "token_has_equal": 0,
            "access_token_encoded_has_%2B": 0,
            "access_token_encoded_has_plus": 0,
        }
    encoded = quote_plus(access_token, safe="")
    encoded_upper = encoded.upper()
    return {
        "token_has_plus": 1 if "+" in access_token else 0,
        "token_has_slash": 1 if "/" in access_token else 0,
        "token_has_equal": 1 if "=" in access_token else 0,
        "access_token_encoded_has_%2B": 1 if "%2B" in encoded_upper else 0,
        "access_token_encoded_has_plus": 1 if "+" in encoded else 0,
    }


def _build_safe_fingerprint(
    *,
    access_token: str | None,
    partner_id: int | None,
    partner_key: str | None,
    path: str,
    timestamp: int,
    shop_id: int | None,
    omit_access_token: bool = False,
) -> dict[str, object]:
    access_token_len = len(access_token) if access_token else 0
    token_sha = _sha256_8(access_token)
    sign_input = None
    sign = None
    if partner_id is not None:
        sign_input = build_sign_base(
            partner_id,
            path,
            timestamp,
            access_token=access_token,
            shop_id=shop_id,
            omit_access_token=omit_access_token,
        )
        if partner_key:
            sign = sign_hmac_sha256_hex(sign_input, partner_key)
    return {
        "access_token_len": access_token_len,
        "access_token_sha256_8": token_sha,
        "sign_sha256_8": _sha256_8(sign),
        "sign_input_sha256_8": _sha256_8(sign_input),
    }


def _validate_outgoing_access_token(
    *,
    access_token: str | None,
    shop_key: str,
    path: str,
) -> None:
    token_len = len(access_token) if access_token else 0
    token_sha = _sha256_8(access_token)
    if not access_token or "***" in access_token:
        raise RuntimeError(
            "outgoing_access_token_invalid_or_redacted "
            f"shop={shop_key} path={path} "
            f"access_token_len={token_len} access_token_sha256_8={token_sha}"
        )


def _print_dry_run(calls, mapping: MappingConfig) -> None:
    for call in calls:
        has_mapping = call.name in mapping.calls
        mapping_type = mapping.calls[call.name].type if has_mapping else "missing"
        print(
            f"call={call.name} method={call.method} path={call.path} mapping={mapping_type}"
        )


def _save_artifact(
    shop_key: str,
    call,
    api_path: str,
    params: dict | None,
    body: dict | None,
    response: dict | None,
    ok: bool,
    error_text: str | None,
    http_status: int | None,
    shopee_error: Any,
    save_root: Path | None,
) -> None:
    requested_at = datetime.now(timezone.utc)
    root = save_root or (Path("collaboration") / "artifacts" / "shopee_api")
    output_path = build_artifact_path(
        root,
        shop_key,
        call.name,
        api_path,
        requested_at,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "shop_key": shop_key,
        "call_name": call.name,
        "method": call.method,
        "path": api_path,
        "params": params,
        "body": body,
        "requested_at": requested_at.isoformat(),
        "http_status": http_status,
        "shopee_error": shopee_error,
    }
    meta = redact_secrets(
        meta,
        extra_keys={
            "partner_key",
            "access_token",
            "refresh_token",
            "sign",
            "authorization",
            "cookie",
            "secret",
            "client_secret",
        },
    )
    payload: dict[str, Any] = {"__meta": meta}
    if ok and response is not None:
        payload.update(
            redact_secrets(
                response,
                extra_keys={
                    "partner_key",
                    "access_token",
                    "refresh_token",
                    "sign",
                    "authorization",
                    "cookie",
                    "secret",
                    "client_secret",
                },
            )
        )
    else:
        payload["error"] = redact_text(error_text or "unknown error")
    output_path.write_text(_dump_json(payload), encoding="utf-8")


def _resolve_failure_artifacts_root() -> Path:
    override = os.environ.get("FAILURE_ARTIFACTS_ROOT")
    if override:
        return Path(override)
    return Path("collaboration") / "artifacts" / "shopee_api"


def _save_failure_artifact(
    *,
    shop_key: str,
    target_date: date_cls,
    call_name: str,
    api_path: str,
    method: str,
    query_keys: list[str],
    http_status: int | None,
    api_error: object | None,
    api_message: object | None,
    request_id: object | None,
    response_json: dict | None,
    response_text_head: str | None,
    safe_fingerprint: dict[str, object] | None = None,
    access_token_debug: dict[str, int] | None = None,
) -> Path | None:
    requested_at = datetime.now(timezone.utc)
    timestamp = int(requested_at.timestamp())
    root = _resolve_failure_artifacts_root()
    safe_call = safe_name(call_name)
    safe_api = safe_path(api_path)
    ts_ms = int(requested_at.timestamp() * 1000)
    output_path = (
        root
        / shop_key
        / target_date.isoformat()
        / f"{ts_ms}_{safe_call}_{safe_api}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    safe_fp = safe_fingerprint
    request_meta = {
        "method": method,
        "path": api_path,
        "query_keys": query_keys,
        "timestamp": timestamp,
        "requested_at": requested_at.isoformat(),
    }
    response_meta = {"http_status": http_status}
    parsed_error = {
        "api_error": api_error,
        "api_message": api_message,
        "request_id": request_id,
    }
    meta = {
        "shop_key": shop_key,
        "call_name": call_name,
    }
    extra_keys = {
        "partner_key",
        "access_token",
        "refresh_token",
        "sign",
        "authorization",
        "cookie",
        "secret",
        "client_secret",
    }
    meta = redact_secrets(meta, extra_keys=extra_keys)
    request_meta = redact_secrets(request_meta, extra_keys=extra_keys)
    if safe_fp:
        request_meta["safe_fingerprint"] = safe_fp
    if access_token_debug:
        request_meta["access_token_encoding"] = access_token_debug
    response_meta = redact_secrets(response_meta, extra_keys=extra_keys)
    parsed_error = redact_secrets(parsed_error, extra_keys=extra_keys)
    payload: dict[str, Any] = {
        "__meta": meta,
        "request_meta": request_meta,
        "response_meta": response_meta,
        "parsed_error": parsed_error,
    }
    if response_json is not None:
        payload["response"] = redact_secrets(
            response_json,
            extra_keys=extra_keys,
        )
    if response_text_head:
        payload["raw_body_snippet"] = redact_text(response_text_head[:2048])
    output_path.write_text(_dump_json(payload), encoding="utf-8")
    return output_path


def _dump_json(payload: dict) -> str:
    import json

    return json.dumps(payload, ensure_ascii=True)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


def _load_fixture_payload(fixtures_dir: Path, call_name: str) -> dict[str, Any] | None:
    if not fixtures_dir.exists():
        return None
    if call_name == "ads_snapshot":
        preferred = fixtures_dir / "ads_snapshot_ok_with_fake_secrets.json"
        if preferred.exists():
            return _read_json(preferred)
    exact = fixtures_dir / f"{call_name}.json"
    if exact.exists():
        return _read_json(exact)
    matches = sorted(fixtures_dir.glob(f"{call_name}*.json"))
    if matches:
        return _read_json(matches[0])
    return None


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))

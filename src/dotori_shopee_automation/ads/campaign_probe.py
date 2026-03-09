from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from ..config import resolve_timezone
from ..db import SessionLocal, init_db
from ..shopee.auth import refresh_access_token
from ..shopee.client import ShopeeClient
from ..shopee.redact import redact_secrets, redact_text
from ..shopee.signing import build_sign_base, sign_hmac_sha256_hex
from ..shopee.token_store import get_token, needs_refresh, upsert_token


_SECRET_KEYS = {
    "partner_key",
    "access_token",
    "refresh_token",
    "sign",
    "authorization",
    "cookie",
    "secret",
    "client_secret",
}


@dataclass
class CampaignProbeShopResult:
    shop_key: str
    shop_label: str
    shop_id: int
    registry_rows: list[dict[str, Any]]
    gms_campaign_ids: set[str]
    id_list_count: int
    setting_chunks_ok: int
    setting_chunks_fail: int
    setting_rows_raw: int
    gms_ok: bool
    preflight_ok: bool = True
    preflight_reason: str = ""
    preflight_endpoint: str = "/api/v2/ads/get_total_balance"
    preflight_http_status: int | None = None
    preflight_api_error: str | None = None
    preflight_api_message: str | None = None
    preflight_request_id: str | None = None
    token_len: int = 0
    token_sha8: str = ""
    meta_probe_ok: bool = False
    meta_probe_reason: str = ""
    gms_probe_reason: str = ""


@dataclass
class GmsProbeShopResult:
    shop_key: str
    shop_label: str
    shop_id: int
    gms_http_status: int | None
    gms_api_error: str | None
    gms_api_message: str | None
    gms_request_id: str | None
    gms_ok_count: int
    gms_campaign_count: int
    rate_limit_hit: bool
    calls_made: int
    campaign_level_supported: str
    gms_name_supported: str
    gms_budget_supported: str
    probe_reason: str
    normalized_rows: list[dict[str, Any]]


@dataclass
class CallTrace:
    path: str
    method: str
    called_at_utc: str
    params: dict[str, Any]
    http_status: int | None
    api_error: str | None
    api_message: str | None
    request_id: str | None
    retry_after_sec: int | None
    rate_limited: bool
    skipped_by_cooldown: bool = False
    skipped_by_budget: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "method": self.method,
            "called_at_utc": self.called_at_utc,
            "params": self.params,
            "http_status": self.http_status,
            "api_error": self.api_error,
            "api_message": self.api_message,
            "request_id": self.request_id,
            "retry_after_sec": self.retry_after_sec,
            "rate_limited": 1 if self.rate_limited else 0,
            "skipped_by_cooldown": 1 if self.skipped_by_cooldown else 0,
            "skipped_by_budget": 1 if self.skipped_by_budget else 0,
        }


@dataclass
class PreflightResult:
    ok: bool
    reason: str
    endpoint: str
    http_status: int | None
    api_error: str | None
    api_message: str | None
    request_id: str | None
    token_len: int
    token_sha8: str


def _write_json(path: Path, payload: Any, *, redact: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data: Any = payload
    if redact:
        if isinstance(payload, dict):
            data = redact_secrets(payload, extra_keys=_SECRET_KEYS)
        else:
            data = {"payload": redact_text(str(payload))}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json_safely(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _payload_with_trace(payload: dict[str, Any] | None, trace: CallTrace) -> dict[str, Any]:
    base = dict(payload or {})
    base["__trace"] = trace.to_dict()
    return base


def _api_ok(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    err = payload.get("error")
    return err in (None, "", 0, "0")


def _api_error(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return "invalid_payload"
    err = payload.get("error")
    msg = payload.get("message") or payload.get("msg") or ""
    if err in (None, "", 0, "0"):
        return ""
    return f"{err}:{msg}"


def _api_error_parts(payload: dict[str, Any] | None) -> tuple[str | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None, None
    err_raw = payload.get("error")
    if err_raw is None:
        err_raw = payload.get("error_code")
    msg_raw = payload.get("message")
    if msg_raw is None:
        msg_raw = payload.get("msg")
    if msg_raw is None:
        msg_raw = payload.get("error_msg")
    req_raw = payload.get("request_id")
    if req_raw is None:
        req_raw = payload.get("requestId")
    err = str(err_raw).strip() if err_raw not in (None, "") else None
    msg = str(msg_raw).strip() if msg_raw not in (None, "") else None
    req = str(req_raw).strip() if req_raw not in (None, "") else None
    return err, msg, req


def _token_sha8(token: str | None) -> str:
    value = str(token or "")
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _is_token_invalid(http_status: int | None, api_error: str | None, api_message: str | None) -> bool:
    error_text = f"{api_error or ''} {api_message or ''}".lower()
    if "invalid_access_token" in error_text:
        return True
    if "invalid_acceess_token" in error_text:
        return True
    if "invalid access token" in error_text:
        return True
    if http_status == 403:
        return True
    return False


def _preflight_reason_from_trace(trace: CallTrace) -> str:
    if trace.skipped_by_cooldown:
        return "cooldown_active"
    if trace.skipped_by_budget:
        return "request_budget_exhausted"
    if _is_rate_limited(trace.http_status, trace.api_error, trace.api_message):
        return "rate_limited"
    if _is_token_invalid(trace.http_status, trace.api_error, trace.api_message):
        return "token_invalid"
    if trace.http_status is not None and trace.http_status >= 400:
        return f"http_{trace.http_status}"
    if trace.api_error:
        return str(trace.api_error)
    return "preflight_failed"


def _is_rate_limited(http_status: int | None, api_error: str | None, api_message: str | None) -> bool:
    if http_status == 429:
        return True
    error_text = f"{api_error or ''} {api_message or ''}".lower()
    if "rate_limit" in error_text:
        return True
    if "ads_rate_limit" in error_text:
        return True
    if "ads_rate_limit_total_api" in error_text:
        return True
    if "too many request" in error_text:
        return True
    return False


def _parse_retry_after_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        seconds = int(float(text))
    except Exception:  # noqa: BLE001
        return None
    if seconds <= 0:
        return None
    return seconds


def _default_rate_limit_state_path() -> Path:
    env_path = os.environ.get("DOTORI_ADS_RATE_LIMIT_STATE_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return Path("artifacts") / "ads_rate_limit" / "rate_limit_state.json"


def resolve_rate_limit_state_path_info(
    *,
    rate_limit_state_path: Path | str | None,
    out_dir: Path | None,
) -> dict[str, Any]:
    if rate_limit_state_path not in (None, ""):
        return {
            "path": Path(str(rate_limit_state_path)),
            "source": "cli",
        }
    env_path = os.environ.get("DOTORI_ADS_RATE_LIMIT_STATE_PATH", "").strip()
    if env_path:
        return {
            "path": Path(env_path),
            "source": "env",
        }
    if out_dir is not None:
        return {
            "path": Path(out_dir) / "rate_limit_state.json",
            "source": "fallback",
        }
    return {
        "path": _default_rate_limit_state_path(),
        "source": "fallback",
    }


def _resolve_rate_limit_state_path(
    *,
    rate_limit_state_path: Path | str | None,
    out_dir: Path | None,
) -> Path:
    info = resolve_rate_limit_state_path_info(
        rate_limit_state_path=rate_limit_state_path,
        out_dir=out_dir,
    )
    return Path(info["path"])


def _load_rate_limit_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"shops": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"shops": {}}
    if not isinstance(data, dict):
        return {"shops": {}}
    shops = data.get("shops")
    if not isinstance(shops, dict):
        data["shops"] = {}
    return data


def _save_rate_limit_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(state, ensure_ascii=True, indent=2)
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:  # noqa: BLE001
            pass


def _parse_iso_to_utc(value: Any) -> datetime | None:
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


def _read_shop_cooldown_until(
    *,
    state_path: Path,
    shop_key: str,
    now_utc: datetime,
) -> datetime | None:
    state = _load_rate_limit_state(state_path)
    shops = state.get("shops") if isinstance(state, dict) else {}
    if not isinstance(shops, dict):
        return None
    row = shops.get(shop_key)
    if not isinstance(row, dict):
        return None
    until = _parse_iso_to_utc(row.get("cooldown_until_utc"))
    if until is None:
        return None
    if until <= now_utc:
        return None
    return until


def _update_rate_limit_state(
    *,
    state_path: Path,
    shop_key: str,
    now_utc: datetime,
    retry_after_sec: int | None,
    http_status: int | None,
    api_error: str | None,
    api_message: str | None,
) -> datetime:
    state = _load_rate_limit_state(state_path)
    shops = state.setdefault("shops", {})
    if not isinstance(shops, dict):
        shops = {}
        state["shops"] = shops
    row = shops.get(shop_key)
    if not isinstance(row, dict):
        row = {}
        shops[shop_key] = row

    previous_until = _parse_iso_to_utc(row.get("cooldown_until_utc"))
    previous_strikes = int(row.get("rate_limit_strikes") or 0)
    if retry_after_sec is not None:
        cooldown_sec = retry_after_sec
        strikes = 1
    else:
        strikes = previous_strikes + 1 if previous_until and previous_until > now_utc else 1
        cooldown_sec = min(3600 * (2 ** (max(strikes, 1) - 1)), 21600)
    cooldown_until = now_utc + timedelta(seconds=cooldown_sec)

    row["cooldown_until_utc"] = cooldown_until.isoformat()
    row["rate_limit_strikes"] = strikes
    row["last_rate_limited_at_utc"] = now_utc.isoformat()
    row["last_http_status"] = int(http_status) if http_status is not None else None
    row["last_error"] = api_error or ""
    row["last_message"] = api_message or ""
    row["updated_at_utc"] = now_utc.isoformat()
    _save_rate_limit_state(state_path, state)
    return cooldown_until


def _clear_rate_limit_state(
    *,
    state_path: Path,
    shop_key: str,
    now_utc: datetime,
) -> None:
    state = _load_rate_limit_state(state_path)
    shops = state.setdefault("shops", {})
    if not isinstance(shops, dict):
        shops = {}
        state["shops"] = shops
    row = shops.get(shop_key)
    if not isinstance(row, dict):
        row = {}
        shops[shop_key] = row
    row["cooldown_until_utc"] = ""
    row["rate_limit_strikes"] = 0
    row["updated_at_utc"] = now_utc.isoformat()
    _save_rate_limit_state(state_path, state)


def read_ads_rate_limit_status(
    *,
    shop_keys: list[str],
    now_utc: datetime | None = None,
    state_path: Path | str | None = None,
) -> dict[str, dict[str, Any]]:
    now_value = now_utc or datetime.now(timezone.utc)
    resolved_state_path = _resolve_rate_limit_state_path(
        rate_limit_state_path=state_path,
        out_dir=None,
    )
    state = _load_rate_limit_state(resolved_state_path)
    shops_state = state.get("shops") if isinstance(state, dict) else {}
    shops_state = shops_state if isinstance(shops_state, dict) else {}

    out: dict[str, dict[str, Any]] = {}
    for shop_key in shop_keys:
        row = shops_state.get(shop_key)
        row = row if isinstance(row, dict) else {}
        cooldown_until = _parse_iso_to_utc(row.get("cooldown_until_utc"))
        cooldown_active = bool(cooldown_until is not None and cooldown_until > now_value)
        last_seen = _parse_iso_to_utc(row.get("last_rate_limited_at_utc")) or _parse_iso_to_utc(
            row.get("updated_at_utc")
        )
        http_status_raw = row.get("last_http_status")
        try:
            last_http_status = int(http_status_raw) if http_status_raw not in (None, "") else None
        except Exception:  # noqa: BLE001
            last_http_status = None
        last_api_error = str(row.get("last_error") or "").strip() or None
        out[shop_key] = {
            "cooldown_active": bool(cooldown_active),
            "cooldown_until_utc": cooldown_until.isoformat().replace("+00:00", "Z")
            if isinstance(cooldown_until, datetime)
            else None,
            "last_seen_utc": last_seen.isoformat().replace("+00:00", "Z")
            if isinstance(last_seen, datetime)
            else None,
            "last_http_status": last_http_status,
            "last_api_error": last_api_error,
            "state_path": str(resolved_state_path),
        }
    return out


def _extract_list(payload: dict[str, Any] | None, keys: list[str]) -> list[Any]:
    if not isinstance(payload, dict):
        return []
    response = payload.get("response")
    if isinstance(response, dict):
        for key in keys:
            value = response.get(key)
            if isinstance(value, list):
                return value
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _extract_campaign_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("campaign_id", "campaignId", "id"):
            raw = value.get(key)
            if raw not in (None, ""):
                return str(raw).strip()
    if value not in (None, ""):
        return str(value).strip()
    return ""


def _extract_campaign_ids_from_list_payload(payload: dict[str, Any] | None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    rows = _extract_list(payload, ["campaign_list", "campaign_id_list", "records", "items", "list"])
    for row in rows:
        campaign_id = _extract_campaign_id(row)
        if not campaign_id or campaign_id in seen:
            continue
        ad_type = ""
        if isinstance(row, dict):
            ad_type = str(row.get("ad_type") or "").strip()
        seen.add(campaign_id)
        out.append({"campaign_id": campaign_id, "ad_type": ad_type})
    return out


def _extract_setting_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = _extract_list(payload, ["campaign_list", "campaign_setting_list", "records", "items", "list"])
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
    return out


def _extract_gms_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = _extract_list(payload, ["records", "campaign_list", "items", "list", "data"])
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
    return out


def _extract_nested_scalar_by_keys(node: Any, keys: list[str]) -> str:
    key_set = {str(key).strip().lower() for key in keys if str(key).strip()}
    if not key_set:
        return ""
    queue: list[Any] = [node]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for key, value in current.items():
                key_lower = str(key).strip().lower()
                if key_lower in key_set:
                    text = _json_scalar(value)
                    if text:
                        return text
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            for value in current:
                if isinstance(value, (dict, list)):
                    queue.append(value)
    return ""


def _normalize_gms_campaign_rows(
    *,
    shop_key: str,
    shop_label: str,
    payload: dict[str, Any] | None,
    raw_source: str,
) -> list[dict[str, Any]]:
    rows = _extract_gms_rows(payload)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for idx, row in enumerate(rows, start=1):
        campaign_id = _extract_campaign_id(row)
        if not campaign_id:
            campaign_id = _extract_nested_scalar_by_keys(
                row,
                ["campaign_id", "campaignid", "id"],
            )
        campaign_name = _extract_nested_scalar_by_keys(
            row,
            [
                "campaign_name",
                "campaignname",
                "ad_name",
                "name",
                "title",
            ],
        )
        campaign_type = (
            _extract_nested_scalar_by_keys(
                row,
                ["campaign_type", "campaign_kind", "ad_type", "type"],
            )
            or "gms"
        )
        daily_budget = _extract_nested_scalar_by_keys(
            row,
            ["daily_budget", "campaign_budget", "budget", "dailybudget"],
        )
        total_budget = _extract_nested_scalar_by_keys(
            row,
            ["total_budget", "campaign_total_budget", "lifetime_budget", "totalbudget"],
        )
        spend_today = _extract_nested_scalar_by_keys(
            row,
            [
                "spend",
                "spend_today",
                "today_spend",
                "cost",
                "expense",
                "consumed_budget",
            ],
        )
        spend_7d = _extract_nested_scalar_by_keys(
            row,
            [
                "spend_7d",
                "spend7d",
                "seven_day_spend",
                "last_7d_spend",
                "spend_7_day",
            ],
        )

        unique_key = (
            campaign_id,
            campaign_name,
            daily_budget,
            total_budget,
            spend_today,
        )
        if unique_key in seen:
            continue
        seen.add(unique_key)

        out.append(
            {
                "shop_key": shop_key,
                "shop_label": shop_label,
                "campaign_type": campaign_type,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "daily_budget": daily_budget,
                "total_budget": total_budget,
                "spend_today": spend_today,
                "spend_7d": spend_7d,
                "raw_source": raw_source,
                "row_index": idx,
            }
        )
    return out


def _parse_budget(record: dict[str, Any]) -> tuple[str, str]:
    common = record.get("common_info")
    if not isinstance(common, dict):
        common = {}
    daily = common.get("campaign_budget")
    total = common.get("total_budget")
    if total is None and "campaign_total_budget" in common:
        total = common.get("campaign_total_budget")
    if daily is None and "daily_budget" in common:
        daily = common.get("daily_budget")
    return (_json_scalar(daily), _json_scalar(total))


def _extract_duration(record: dict[str, Any]) -> tuple[str, str]:
    common = record.get("common_info")
    if not isinstance(common, dict):
        common = {}
    duration = common.get("campaign_duration")
    if not isinstance(duration, dict):
        return ("", "")
    start_time = _json_scalar(duration.get("start_time"))
    end_time = _json_scalar(duration.get("end_time"))
    return (start_time, end_time)


def _extract_item_ids(record: dict[str, Any]) -> list[str]:
    common = record.get("common_info")
    if not isinstance(common, dict):
        common = {}
    raw = common.get("item_id_list")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        text = _json_scalar(item)
        if text:
            out.append(text)
    return out


def _extract_product_names(record: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                key_lower = str(key).lower()
                if key_lower in {"product_name", "item_name", "name"} and isinstance(value, str):
                    text = value.strip()
                    if text and text not in seen:
                        seen.add(text)
                        out.append(text)
                walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(record)
    return out


def _normalize_setting_row(
    *,
    shop_label: str,
    campaign_id: str,
    record: dict[str, Any],
    source_endpoint: str,
) -> dict[str, Any]:
    common = record.get("common_info")
    if not isinstance(common, dict):
        common = {}
    ad_name = str(
        common.get("ad_name")
        or common.get("campaign_name")
        or record.get("campaign_name")
        or ""
    ).strip()
    status = str(common.get("campaign_status") or record.get("status") or "").strip()
    daily_budget, total_budget = _parse_budget(record)
    start_time, end_time = _extract_duration(record)
    item_ids = _extract_item_ids(record)
    product_names = _extract_product_names(record)
    return {
        "shop_label": shop_label,
        "campaign_id": campaign_id,
        "ad_name": ad_name,
        "status": status,
        "daily_budget": daily_budget,
        "total_budget": total_budget,
        "start_time": start_time,
        "end_time": end_time,
        "item_count": len(item_ids),
        "item_id_list_json": json.dumps(item_ids, ensure_ascii=False),
        "product_name_list_json": json.dumps(product_names, ensure_ascii=False),
        "source_endpoint": source_endpoint,
    }


def _json_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value).strip()


def _build_client(settings) -> ShopeeClient:
    return ShopeeClient(
        partner_id=settings.shopee_partner_id,
        partner_key=settings.shopee_partner_key,
        host=settings.shopee_api_host,
    )


def _build_fixture_client() -> ShopeeClient:
    def handler(request: httpx.Request) -> httpx.Response:
        shop_id = str(request.url.params.get("shop_id") or "-")
        request_id_seed = f"{request.url.path}:{shop_id}"
        request_id = hashlib.sha256(request_id_seed.encode("utf-8")).hexdigest()[:24]
        if request.url.path == "/api/v2/ads/get_total_balance":
            return httpx.Response(
                403,
                json={
                    "error": "ads_rate_limit_total_api",
                    "message": "ads_rate_limit_total_api",
                    "request_id": request_id,
                },
            )
        return httpx.Response(
            200,
            json={"error": "", "message": "", "request_id": request_id, "response": {}},
        )

    return ShopeeClient(
        partner_id=1,
        partner_key="fixture_partner_key",
        host="https://fixture.local",
        transport=httpx.MockTransport(handler),
    )


def _ensure_live_token(
    *,
    settings,
    shop_cfg,
) -> tuple[str, int]:
    init_db()
    session = SessionLocal()
    try:
        token = get_token(session, shop_cfg.shop_key)
        if token is None:
            raise RuntimeError("missing_token")
        if needs_refresh(token.access_token_expires_at):
            refreshed = refresh_access_token(
                _build_client(settings),
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
            if token is None:
                raise RuntimeError("missing_token_after_refresh")
        return (token.access_token, int(token.shop_id))
    finally:
        session.close()


def _run_preflight_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    shop_id: int,
    access_token: str,
    token_len: int,
    token_sha8: str,
    request_budget: dict[str, int] | None,
    ignore_cooldown: bool,
    rate_limit_state_path: Path,
) -> tuple[PreflightResult, dict[str, Any]]:
    endpoint = "/api/v2/ads/get_total_balance"
    payload, error_text, trace = _call_live(
        client=client,
        shop_key=shop_key,
        path=endpoint,
        shop_id=shop_id,
        access_token=access_token,
        params={},
        request_budget=request_budget,
        ignore_cooldown=ignore_cooldown,
        rate_limit_state_path=rate_limit_state_path,
    )
    api_error = trace.api_error
    api_message = trace.api_message
    request_id = trace.request_id
    http_status = trace.http_status

    ok = bool(payload is not None and _api_ok(payload) and (http_status is None or http_status < 400))
    reason = ""
    if not ok:
        reason = _preflight_reason_from_trace(trace)

    if payload is None:
        payload_for_file = {"error": "exception", "message": error_text}
    else:
        payload_for_file = dict(payload)

    payload_for_file.update(
        {
            "shop_key": shop_key,
            "endpoint": endpoint,
            "http_status": http_status,
            "api_error": api_error,
            "api_message": api_message,
            "request_id": request_id,
            "token_len": token_len,
            "token_sha8": token_sha8,
            "ok": 1 if ok else 0,
            "reason": reason,
        }
    )
    payload_for_file = _payload_with_trace(payload_for_file, trace)

    preflight = PreflightResult(
        ok=ok,
        reason=reason,
        endpoint=endpoint,
        http_status=http_status,
        api_error=api_error,
        api_message=api_message,
        request_id=request_id,
        token_len=token_len,
        token_sha8=token_sha8,
    )
    return preflight, payload_for_file


def _call_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    path: str,
    shop_id: int,
    access_token: str,
    params: dict[str, Any] | None,
    request_budget: dict[str, int] | None = None,
    ignore_cooldown: bool = False,
    rate_limit_state_path: Path | None = None,
) -> tuple[dict[str, Any] | None, str, CallTrace]:
    now_utc = datetime.now(timezone.utc)
    called_at_utc = now_utc.isoformat()
    params_safe = dict(params or {})
    state_path = _resolve_rate_limit_state_path(
        rate_limit_state_path=rate_limit_state_path,
        out_dir=None,
    )

    if isinstance(request_budget, dict):
        remaining = int(request_budget.get("remaining", 0))
        if remaining <= 0:
            trace = CallTrace(
                path=path,
                method="GET",
                called_at_utc=called_at_utc,
                params=params_safe,
                http_status=None,
                api_error="request_budget_exhausted",
                api_message="max_requests reached",
                request_id=None,
                retry_after_sec=None,
                rate_limited=False,
                skipped_by_budget=True,
            )
            return None, "request_budget_exhausted", trace
        request_budget["remaining"] = remaining - 1

    if not ignore_cooldown:
        cooldown_until = _read_shop_cooldown_until(
            state_path=state_path,
            shop_key=shop_key,
            now_utc=now_utc,
        )
        if cooldown_until is not None:
            trace = CallTrace(
                path=path,
                method="GET",
                called_at_utc=called_at_utc,
                params=params_safe,
                http_status=429,
                api_error="local_rate_limited",
                api_message=f"cooldown_active_until={cooldown_until.isoformat()}",
                request_id=None,
                retry_after_sec=max(int((cooldown_until - now_utc).total_seconds()), 1),
                rate_limited=True,
                skipped_by_cooldown=True,
            )
            payload = {
                "error": "local_rate_limited",
                "message": f"Cooldown active until {cooldown_until.isoformat()}",
                "cooldown_until_utc": cooldown_until.isoformat(),
            }
            return payload, "", trace

    sign_base = build_sign_base(
        client.partner_id,
        path,
        int(now_utc.timestamp()),
        access_token=access_token,
        shop_id=shop_id,
    )
    sign = sign_hmac_sha256_hex(sign_base, client.partner_key)
    query: dict[str, Any] = {
        "partner_id": client.partner_id,
        "timestamp": int(now_utc.timestamp()),
        "sign": sign,
        "shop_id": shop_id,
        "access_token": access_token,
    }
    if params:
        query.update(params)

    try:
        response = client._client.request(  # noqa: SLF001
            "GET",
            path,
            params=query,
            json=None,
        )
        retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
        payload: dict[str, Any] | None = None
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:  # noqa: BLE001
            payload = None

        api_error, api_message, request_id = _api_error_parts(payload)
        rate_limited = _is_rate_limited(response.status_code, api_error, api_message)
        if rate_limited:
            _update_rate_limit_state(
                state_path=state_path,
                shop_key=shop_key,
                now_utc=now_utc,
                retry_after_sec=retry_after,
                http_status=response.status_code,
                api_error=api_error,
                api_message=api_message,
            )
        elif response.status_code < 400 and _api_ok(payload):
            _clear_rate_limit_state(
                state_path=state_path,
                shop_key=shop_key,
                now_utc=now_utc,
            )

        trace = CallTrace(
            path=path,
            method="GET",
            called_at_utc=called_at_utc,
            params=params_safe,
            http_status=response.status_code,
            api_error=api_error,
            api_message=api_message,
            request_id=request_id,
            retry_after_sec=retry_after,
            rate_limited=rate_limited,
        )
        if payload is not None:
            return payload, "", trace
        return None, f"invalid_payload_type http={response.status_code}", trace
    except Exception as exc:  # noqa: BLE001
        trace = CallTrace(
            path=path,
            method="GET",
            called_at_utc=called_at_utc,
            params=params_safe,
            http_status=None,
            api_error="exception",
            api_message=redact_text(str(exc)),
            request_id=None,
            retry_after_sec=None,
            rate_limited=False,
        )
        return None, redact_text(str(exc)), trace


def _fetch_id_list_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    shop_id: int,
    access_token: str,
    raw_dir: Path,
    redact: bool,
    request_budget: dict[str, int] | None,
    ignore_cooldown: bool,
    rate_limit_state_path: Path,
) -> list[dict[str, str]]:
    all_rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    offset = 0
    limit = 100
    page = 1
    use_offset_limit = True
    while page <= 100:
        params: dict[str, Any] = {"ad_type": "all"}
        if use_offset_limit:
            params["offset"] = str(offset)
            params["limit"] = str(limit)
        payload, error_text, trace = _call_live(
            client=client,
            shop_key=shop_key,
            path="/api/v2/ads/get_product_level_campaign_id_list",
            shop_id=shop_id,
            access_token=access_token,
            params=params,
            request_budget=request_budget,
            ignore_cooldown=ignore_cooldown,
            rate_limit_state_path=rate_limit_state_path,
        )
        if payload is None:
            _write_json(
                raw_dir / f"campaign_id_list_page_{page:02d}.json",
                _payload_with_trace(
                    {"error": "exception", "message": error_text, "params": params},
                    trace,
                ),
                redact=redact,
            )
            break
        _write_json(
            raw_dir / f"campaign_id_list_page_{page:02d}.json",
            _payload_with_trace(payload, trace),
            redact=redact,
        )
        err = _api_error(payload)
        if err:
            if use_offset_limit and "error_param" in err:
                use_offset_limit = False
                continue
            break
        rows = _extract_campaign_ids_from_list_payload(payload)
        for row in rows:
            cid = row.get("campaign_id", "")
            if cid and cid not in seen_ids:
                seen_ids.add(cid)
                all_rows.append(row)
        response = payload.get("response")
        has_next = False
        next_offset = offset + limit
        if isinstance(response, dict):
            has_next = bool(
                response.get("has_next_page")
                or response.get("has_next")
                or response.get("next_page")
            )
            raw_next_offset = response.get("next_offset")
            if raw_next_offset not in (None, ""):
                try:
                    next_offset = int(raw_next_offset)
                except Exception:  # noqa: BLE001
                    next_offset = offset + limit
        if not use_offset_limit or not has_next:
            break
        offset = next_offset
        page += 1
    return all_rows


def _setting_param_candidates(campaign_ids: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    info_values = [
        json.dumps([1, 4], ensure_ascii=True),
        "1,4",
        json.dumps([1], ensure_ascii=True),
        "1",
        json.dumps([1, 2, 3, 4], ensure_ascii=True),
    ]
    id_values = [
        ",".join(campaign_ids),
        json.dumps(campaign_ids, ensure_ascii=True),
    ]
    if campaign_ids and all(str(x).isdigit() for x in campaign_ids):
        id_values.append(json.dumps([int(x) for x in campaign_ids], ensure_ascii=True))
    for info_value in info_values:
        for id_value in id_values:
            out.append({"info_type_list": info_value, "campaign_id_list": id_value})
    return out


def _fetch_setting_info_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    shop_id: int,
    access_token: str,
    campaign_ids: list[str],
    raw_dir: Path,
    redact: bool,
    request_budget: dict[str, int] | None,
    ignore_cooldown: bool,
    rate_limit_state_path: Path,
) -> tuple[list[dict[str, Any]], int, int]:
    rows: list[dict[str, Any]] = []
    ok_chunks = 0
    fail_chunks = 0
    chunk_size = 50
    chosen_params: dict[str, str] | None = None
    chunks = [campaign_ids[i : i + chunk_size] for i in range(0, len(campaign_ids), chunk_size)]
    for idx, chunk in enumerate(chunks, start=1):
        payload: dict[str, Any] | None = None
        error_text = ""
        used_params: dict[str, str] | None = None
        candidates: list[dict[str, str]]
        if chosen_params:
            reuse = dict(chosen_params)
            reuse["campaign_id_list"] = ",".join(chunk)
            candidates = [reuse] + _setting_param_candidates(chunk)
        else:
            candidates = _setting_param_candidates(chunk)
        for candidate in candidates:
            payload, error_text, trace = _call_live(
                client=client,
                shop_key=shop_key,
                path="/api/v2/ads/get_product_level_campaign_setting_info",
                shop_id=shop_id,
                access_token=access_token,
                params=candidate,
                request_budget=request_budget,
                ignore_cooldown=ignore_cooldown,
                rate_limit_state_path=rate_limit_state_path,
            )
            used_params = candidate
            if payload is None:
                continue
            if _api_ok(payload):
                chosen_params = dict(candidate)
                break
            err = _api_error(payload)
            if "error_param" not in err:
                break
        if payload is None:
            fail_chunks += 1
            _write_json(
                raw_dir / f"setting_info_chunk_{idx:02d}.json",
                _payload_with_trace(
                    {
                        "error": "exception",
                        "message": error_text,
                        "params": used_params,
                        "chunk_ids": chunk,
                    },
                    trace,
                ),
                redact=redact,
            )
            continue
        saved_payload = dict(payload)
        saved_payload["__meta"] = {
            "params": used_params,
            "chunk_ids": chunk,
            "ok": _api_ok(payload),
            "api_error": _api_error(payload),
        }
        _write_json(
            raw_dir / f"setting_info_chunk_{idx:02d}.json",
            _payload_with_trace(saved_payload, trace),
            redact=redact,
        )
        if _api_ok(payload):
            ok_chunks += 1
            rows.extend(_extract_setting_rows(payload))
        else:
            fail_chunks += 1
    return rows, ok_chunks, fail_chunks


def _gms_param_candidates(days: int) -> list[dict[str, str]]:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    end_day = datetime.now(tz).date() - timedelta(days=1)
    start_day = end_day - timedelta(days=max(days, 1) - 1)
    return [
        {
            "start_date": start_day.strftime("%d/%m/%Y"),
            "end_date": end_day.strftime("%d/%m/%Y"),
        },
        {
            "start_date": start_day.isoformat(),
            "end_date": end_day.isoformat(),
        },
        {
            "date_from": start_day.strftime("%d/%m/%Y"),
            "date_to": end_day.strftime("%d/%m/%Y"),
        },
        {
            "date_from": start_day.isoformat(),
            "date_to": end_day.isoformat(),
        },
    ]


def _gms_reference_date(days: int) -> datetime.date:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    end_day = datetime.now(tz).date() - timedelta(days=1)
    _ = days  # keep signature stable for future extensions
    return end_day


def _fetch_gms_probe_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    shop_id: int,
    access_token: str,
    days: int,
    raw_dir: Path,
    redact: bool,
    request_budget: dict[str, int] | None,
    ignore_cooldown: bool,
    rate_limit_state_path: Path,
    max_calls: int | None = None,
) -> tuple[list[dict[str, Any]], bool, CallTrace | None, int, bool]:
    calls = 0
    rate_limit_hit = False
    last_trace: CallTrace | None = None
    last_payload: dict[str, Any] | None = None
    candidates = _gms_param_candidates(days)
    for idx, params in enumerate(candidates, start=1):
        if max_calls is not None and calls >= max(1, int(max_calls)):
            break
        payload, error_text, trace = _call_live(
            client=client,
            shop_key=shop_key,
            path="/api/v2/ads/get_gms_campaign_performance",
            shop_id=shop_id,
            access_token=access_token,
            params=params,
            request_budget=request_budget,
            ignore_cooldown=ignore_cooldown,
            rate_limit_state_path=rate_limit_state_path,
        )
        calls += 1
        last_trace = trace
        if payload is None:
            _write_json(
                raw_dir / f"gms_campaign_performance_try_{idx:02d}.json",
                _payload_with_trace(
                    {"error": "exception", "message": error_text, "params": params},
                    trace,
                ),
                redact=redact,
            )
            continue
        saved_payload = dict(payload)
        saved_payload["__meta"] = {"params": params, "ok": _api_ok(payload), "api_error": _api_error(payload)}
        _write_json(
            raw_dir / f"gms_campaign_performance_try_{idx:02d}.json",
            _payload_with_trace(saved_payload, trace),
            redact=redact,
        )
        last_payload = payload
        rate_limit_hit = bool(rate_limit_hit or trace.rate_limited)
        if _api_ok(payload):
            return _extract_gms_rows(payload), True, trace, calls, rate_limit_hit
        if trace.rate_limited:
            break
    return _extract_gms_rows(last_payload), False, last_trace, calls, rate_limit_hit


def _fetch_gms_live(
    *,
    client: ShopeeClient,
    shop_key: str,
    shop_id: int,
    access_token: str,
    days: int,
    raw_dir: Path,
    redact: bool,
    request_budget: dict[str, int] | None,
    ignore_cooldown: bool,
    rate_limit_state_path: Path,
) -> tuple[list[dict[str, Any]], bool]:
    rows, ok, _trace, _calls, _rate_limit = _fetch_gms_probe_live(
        client=client,
        shop_key=shop_key,
        shop_id=shop_id,
        access_token=access_token,
        days=days,
        raw_dir=raw_dir,
        redact=redact,
        request_budget=request_budget,
        ignore_cooldown=ignore_cooldown,
        rate_limit_state_path=rate_limit_state_path,
        max_calls=None,
    )
    return rows, ok


def _write_registry_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "shop_label",
        "campaign_id",
        "ad_name",
        "status",
        "daily_budget",
        "total_budget",
        "start_time",
        "end_time",
        "item_count",
        "item_id_list_json",
        "product_name_list_json",
        "source_endpoint",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _write_gms_registry_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "shop_key",
        "shop_label",
        "campaign_type",
        "campaign_id",
        "campaign_name",
        "daily_budget",
        "total_budget",
        "spend_today",
        "spend_7d",
        "raw_source",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _yes_no_unknown_from_rows(
    rows: list[dict[str, Any]],
    *,
    key: str,
    success_exists: bool,
) -> str:
    if not success_exists:
        return "unknown"
    for row in rows:
        if str(row.get(key) or "").strip():
            return "yes"
    return "no"


def _summarize_gms_verdict(shop_results: list[GmsProbeShopResult]) -> tuple[str, dict[str, str]]:
    success_results = [row for row in shop_results if int(row.gms_ok_count or 0) > 0]
    success_exists = len(success_results) > 0
    all_rows: list[dict[str, Any]] = []
    for row in success_results:
        all_rows.extend(row.normalized_rows)

    has_campaign_level = any(str(row.get("campaign_id") or "").strip() for row in all_rows)
    if not success_exists:
        campaign_level = "unknown"
    elif has_campaign_level:
        campaign_level = "yes"
    else:
        campaign_level = "no"

    gms_name = _yes_no_unknown_from_rows(all_rows, key="campaign_name", success_exists=success_exists)
    gms_budget = "unknown"
    if success_exists:
        gms_budget = "no"
        for row in all_rows:
            if str(row.get("daily_budget") or "").strip() or str(row.get("total_budget") or "").strip():
                gms_budget = "yes"
                break

    bits = {
        "gms_campaign_level_supported": campaign_level,
        "gms_name_supported": gms_name,
        "gms_budget_supported": gms_budget,
    }
    verdict = (
        "VERDICT: "
        f"gms_campaign_level_supported={bits['gms_campaign_level_supported']} "
        f"gms_name_supported={bits['gms_name_supported']} "
        f"gms_budget_supported={bits['gms_budget_supported']}"
    )
    return verdict, bits


def _write_gms_probe_summary(
    *,
    out_dir: Path,
    mode: str,
    days: int,
    max_gms_calls_per_shop: int,
    force_once: bool,
    shop_results: list[GmsProbeShopResult],
    verdict: str,
) -> Path:
    summary_path = out_dir / "summary.md"
    lines = [
        "# Ads GMS Probe Summary",
        "",
        f"- mode: {mode}",
        f"- days: {days}",
        f"- max_gms_calls_per_shop: {max_gms_calls_per_shop}",
        f"- force_once: {1 if force_once else 0}",
        f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Per shop",
    ]
    for row in shop_results:
        lines.extend(
            [
                f"### {row.shop_label} ({row.shop_key})",
                f"- shop_id: {row.shop_id}",
                f"- gms_http: {row.gms_http_status if row.gms_http_status is not None else '-'}",
                f"- gms_api_error: {row.gms_api_error or '-'}",
                f"- gms_ok_count: {row.gms_ok_count}",
                f"- gms_campaign_count: {row.gms_campaign_count}",
                f"- rate_limit_hit: {1 if row.rate_limit_hit else 0}",
                f"- calls_made: {row.calls_made}",
                f"- campaign_level_supported: {row.campaign_level_supported}",
                f"- gms_name_supported: {row.gms_name_supported}",
                f"- gms_budget_supported: {row.gms_budget_supported}",
                f"- probe_reason: {row.probe_reason or '-'}",
                "",
            ]
        )
    lines.extend(
        [
            "## Verdict",
            f"- {verdict}",
            "",
        ]
    )
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def _summarize_verdict(shop_results: list[CampaignProbeShopResult]) -> tuple[str, dict[str, str]]:
    def metric_state(metric: str) -> str:
        states: list[str] = []
        for shop in shop_results:
            if not bool(getattr(shop, "preflight_ok", True)):
                states.append("unknown")
                continue
            if not bool(getattr(shop, "meta_probe_ok", False)):
                states.append("unknown")
                continue
            registry = list(getattr(shop, "registry_rows", []) or [])
            if metric == "name":
                present = any(str(r.get("ad_name") or "").strip() for r in registry)
            elif metric == "budget":
                present = any(
                    str(r.get("daily_budget") or "").strip()
                    or str(r.get("total_budget") or "").strip()
                    for r in registry
                )
            else:
                present = any(int(r.get("item_count") or 0) > 1 for r in registry)
            states.append("yes" if present else "no")
        if any(state == "yes" for state in states):
            return "yes"
        if any(state == "unknown" for state in states):
            return "unknown"
        return "no"

    name_state = metric_state("name")
    budget_state = metric_state("budget")
    multi_item_state = metric_state("multi_item")

    gms_states: list[str] = []
    for shop in shop_results:
        if not bool(getattr(shop, "preflight_ok", True)):
            gms_states.append("?")
            continue
        registry_ids = {str(row.get("campaign_id") or "").strip() for row in shop.registry_rows if str(row.get("campaign_id") or "").strip()}
        gms_ids = {str(x).strip() for x in shop.gms_campaign_ids if str(x).strip()}
        if not gms_ids:
            gms_states.append("?")
            continue
        gms_states.append("yes" if gms_ids.issubset(registry_ids) else "no")
    if not gms_states or all(state == "?" for state in gms_states):
        gms_parity = "?"
    elif any(state == "no" for state in gms_states):
        gms_parity = "no"
    else:
        gms_parity = "yes"
    bits = {
        "name": name_state,
        "budget": budget_state,
        "multi_item": multi_item_state,
        "gms_parity": gms_parity,
    }
    verdict = (
        "VERDICT: "
        f"name={bits['name']} budget={bits['budget']} multi_item={bits['multi_item']} gms_parity={bits['gms_parity']}"
    )
    if bits["gms_parity"] == "no":
        verdict += " (gms_only_via_gms_endpoints)"
    return verdict, bits


def _next_actions(bits: dict[str, str]) -> list[str]:
    if bits["name"] == "unknown" or bits["budget"] == "unknown":
        return [
            "- Probe status is unknown due to token/preflight/rate-limit conditions.",
            "- Refresh/push tokens first, then rerun campaign-probe.",
            "- Keep current report fallback labels until probe is healthy.",
        ]
    if bits["name"] == "yes" and bits["budget"] == "yes":
        return [
            "- Render ad_name instead of campaign_id in reports/alerts.",
            "- Enable budget-based pacing using campaign setting budgets.",
            "- For multi-item campaigns, include top N items by spend/click.",
        ]
    if bits["budget"] == "no":
        return [
            "- Keep budget override for pacing when budget field is unavailable.",
            "- Attach probe artifacts to Shopee support ticket.",
            "- Continue using spend-based anomaly alerts until budget API is exposed.",
        ]
    if bits["name"] == "no":
        return [
            "- Add campaign label override map (CSV/YAML) for operator-friendly naming.",
            "- Keep campaign_id fallback in reports/alerts.",
            "- Update mapping process when name field becomes available.",
        ]
    if bits["gms_parity"] == "no":
        return [
            "- Keep GMS monitoring via gms_* endpoints.",
            "- Merge GMS registry as a separate table in reporting.",
            "- Keep parity check in daily probe until endpoint parity is solved.",
        ]
    return [
        "- Keep this probe in periodic verification flow.",
        "- Regenerate artifacts when endpoint contracts change.",
        "- Use summary.md + registry.csv as ground truth evidence.",
    ]


def _write_summary(
    *,
    out_dir: Path,
    mode: str,
    days: int,
    shop_results: list[CampaignProbeShopResult],
    verdict: str,
    verdict_bits: dict[str, str],
) -> Path:
    summary_path = out_dir / "summary.md"
    lines = [
        "# Ads Campaign Probe Summary",
        "",
        f"- mode: {mode}",
        f"- days: {days}",
        f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Per shop",
    ]
    for shop in shop_results:
        gms_ids = sorted(shop.gms_campaign_ids)
        lines.extend(
            [
                f"### {shop.shop_label} ({shop.shop_key})",
                f"- shop_id: {shop.shop_id}",
                f"- id_list_count: {shop.id_list_count}",
                f"- setting_rows_raw: {shop.setting_rows_raw}",
                f"- registry_rows: {len(shop.registry_rows)}",
                f"- setting_chunks_ok: {shop.setting_chunks_ok}",
                f"- setting_chunks_fail: {shop.setting_chunks_fail}",
                f"- gms_ok: {1 if shop.gms_ok else 0}",
                f"- gms_campaign_ids_count: {len(gms_ids)}",
                f"- preflight_ok: {1 if shop.preflight_ok else 0}",
                f"- preflight_reason: {shop.preflight_reason or '-'}",
                "",
            ]
        )
    lines.extend(
        [
            "## Verdict",
            f"- {verdict}",
            "",
            "## Next actions",
            *_next_actions(verdict_bits),
            "",
        ]
    )
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def _to_decimal_or_none(value: Any):
    from decimal import Decimal

    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _sync_gms_registry_to_db(
    *,
    rows: list[dict[str, Any]],
    as_of_date,
    fetched_at_utc: datetime,
    source_run_dir: str,
) -> int:
    from .models import Phase1AdsGmsCampaignRegistry

    init_db()
    session = SessionLocal()
    upserted = 0
    try:
        for row in rows:
            shop_key = str(row.get("shop_key") or "").strip()
            campaign_id = str(row.get("campaign_id") or "").strip()
            if not shop_key or not campaign_id:
                continue

            existing = (
                session.query(Phase1AdsGmsCampaignRegistry)
                .filter_by(
                    shop_key=shop_key,
                    as_of_date=as_of_date,
                    campaign_id=campaign_id,
                )
                .one_or_none()
            )
            payload = {
                "campaign_type": str(row.get("campaign_type") or "").strip() or None,
                "campaign_name": str(row.get("campaign_name") or "").strip() or None,
                "daily_budget": _to_decimal_or_none(row.get("daily_budget")),
                "total_budget": _to_decimal_or_none(row.get("total_budget")),
                "spend": _to_decimal_or_none(row.get("spend_today")),
                "fetched_at_utc": fetched_at_utc,
                "source_run_dir": source_run_dir,
            }
            if existing is None:
                session.add(
                    Phase1AdsGmsCampaignRegistry(
                        shop_key=shop_key,
                        as_of_date=as_of_date,
                        campaign_id=campaign_id,
                        **payload,
                    )
                )
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
            upserted += 1
        session.commit()
    finally:
        session.close()
    return upserted


def _sync_registry_to_db(shop_key: str, registry_rows: list[dict[str, Any]]) -> None:
    from .models import AdsCampaign

    init_db()
    session = SessionLocal()
    try:
        for row in registry_rows:
            campaign_id = str(row.get("campaign_id") or "").strip()
            if not campaign_id:
                continue
            ad_name = str(row.get("ad_name") or "").strip()
            if not ad_name:
                continue
            budget = _to_decimal_or_none(row.get("daily_budget"))
            existing = (
                session.query(AdsCampaign)
                .filter_by(shop_key=shop_key, campaign_id=campaign_id)
                .one_or_none()
            )
            if existing:
                existing.campaign_name = ad_name
                if budget is not None:
                    existing.daily_budget = budget
            else:
                session.add(
                    AdsCampaign(
                        shop_key=shop_key,
                        campaign_id=campaign_id,
                        campaign_name=ad_name,
                        status=str(row.get("status") or "").strip() or None,
                        daily_budget=budget,
                    )
                )
        session.commit()
    finally:
        session.close()


def run_campaign_probe(
    *,
    settings,
    target_shops: list,
    mode: str,
    days: int,
    out_dir: Path,
    redact: bool,
    fixture_payload: dict[str, Any] | None = None,
    max_requests_per_shop: int | None = None,
    sync_db: bool = True,
    ignore_cooldown: bool = False,
    rate_limit_state_path: Path | str | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_root = out_dir / "raw"
    normalized_root = out_dir / "normalized"
    shop_results: list[CampaignProbeShopResult] = []
    registry_all: list[dict[str, Any]] = []
    mode_value = mode.strip().lower()

    if mode_value == "live":
        if settings.shopee_partner_id is None:
            raise RuntimeError("SHOPEE_PARTNER_ID is required")
        if not settings.shopee_partner_key:
            raise RuntimeError("SHOPEE_PARTNER_KEY is required")
    if mode_value not in {"live", "dry-run", "fixtures"}:
        raise RuntimeError("mode must be one of: live, dry-run, fixtures")

    resolved_rate_limit_state_path = _resolve_rate_limit_state_path(
        rate_limit_state_path=rate_limit_state_path,
        out_dir=out_dir,
    )

    for shop_cfg in target_shops:
        shop_key = shop_cfg.shop_key
        shop_label = shop_cfg.label
        shop_raw = raw_root / shop_key
        registry_rows: list[dict[str, Any]] = []
        gms_ids: set[str] = set()
        id_list_rows: list[dict[str, str]] = []
        setting_rows: list[dict[str, Any]] = []
        setting_ok = 0
        setting_fail = 0
        gms_ok = False
        preflight_ok = True
        preflight_reason = ""
        preflight_http_status: int | None = None
        preflight_api_error: str | None = None
        preflight_api_message: str | None = None
        preflight_request_id: str | None = None
        token_len = 0
        token_sha8 = ""
        meta_probe_ok = False
        meta_probe_reason = ""
        gms_probe_reason = ""

        if mode_value == "dry-run":
            fixture_shop = fixture_payload.get(shop_key, {}) if isinstance(fixture_payload, dict) else {}
            id_payloads = fixture_shop.get("campaign_id_list") if isinstance(fixture_shop, dict) else None
            if isinstance(id_payloads, dict):
                id_payloads = [id_payloads]
            if not isinstance(id_payloads, list):
                id_payloads = []
            for idx, payload in enumerate(id_payloads, start=1):
                _write_json(shop_raw / f"campaign_id_list_page_{idx:02d}.json", payload, redact=redact)
                if isinstance(payload, dict):
                    for row in _extract_campaign_ids_from_list_payload(payload):
                        id_list_rows.append(row)

            setting_payloads = fixture_shop.get("setting_info") if isinstance(fixture_shop, dict) else None
            if isinstance(setting_payloads, dict):
                setting_payloads = [setting_payloads]
            if not isinstance(setting_payloads, list):
                setting_payloads = []
            for idx, payload in enumerate(setting_payloads, start=1):
                _write_json(shop_raw / f"setting_info_chunk_{idx:02d}.json", payload, redact=redact)
                if isinstance(payload, dict) and _api_ok(payload):
                    setting_ok += 1
                    setting_rows.extend(_extract_setting_rows(payload))
                else:
                    setting_fail += 1

            gms_payload = fixture_shop.get("gms_campaign_perf") if isinstance(fixture_shop, dict) else None
            if isinstance(gms_payload, dict):
                _write_json(shop_raw / "gms_campaign_performance_try_01.json", gms_payload, redact=redact)
                gms_ok = _api_ok(gms_payload)
                for row in _extract_gms_rows(gms_payload):
                    cid = _extract_campaign_id(row)
                    if cid:
                        gms_ids.add(cid)
                if not gms_ok:
                    gms_probe_reason = _api_error(gms_payload) or "gms_unavailable"
            meta_probe_ok = setting_ok > 0
            if not meta_probe_ok:
                meta_probe_reason = "setting_unavailable"
        else:
            if mode_value == "live":
                try:
                    access_token, shop_id = _ensure_live_token(settings=settings, shop_cfg=shop_cfg)
                except Exception as exc:  # noqa: BLE001
                    preflight_ok = False
                    preflight_reason = "token_missing"
                    preflight_api_error = "token_missing"
                    preflight_api_message = redact_text(str(exc))
                    _write_json(
                        shop_raw / f"preflight_{shop_key}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json",
                        {
                            "shop_key": shop_key,
                            "endpoint": "/api/v2/ads/get_total_balance",
                            "http_status": None,
                            "api_error": preflight_api_error,
                            "api_message": preflight_api_message,
                            "request_id": None,
                            "token_len": token_len,
                            "token_sha8": token_sha8,
                            "ok": 0,
                            "reason": preflight_reason,
                        },
                        redact=redact,
                    )
                    shop_results.append(
                        CampaignProbeShopResult(
                            shop_key=shop_key,
                            shop_label=shop_label,
                            shop_id=int(shop_cfg.shopee_shop_id or 0),
                            registry_rows=registry_rows,
                            gms_campaign_ids=gms_ids,
                            id_list_count=0,
                            setting_chunks_ok=0,
                            setting_chunks_fail=0,
                            setting_rows_raw=0,
                            gms_ok=False,
                            preflight_ok=False,
                            preflight_reason=preflight_reason,
                            preflight_endpoint="/api/v2/ads/get_total_balance",
                            preflight_http_status=preflight_http_status,
                            preflight_api_error=preflight_api_error,
                            preflight_api_message=preflight_api_message,
                            preflight_request_id=preflight_request_id,
                            token_len=token_len,
                            token_sha8=token_sha8,
                            meta_probe_ok=False,
                            meta_probe_reason="preflight_failed",
                            gms_probe_reason="preflight_failed",
                        )
                    )
                    continue
                client = _build_client(settings)
            else:
                access_token = f"fixture_access_token_{shop_key}"
                shop_id = int(shop_cfg.shopee_shop_id or 0)
                client = _build_fixture_client()

            token_len = len(str(access_token))
            token_sha8 = _token_sha8(access_token)
            request_budget = (
                {"remaining": int(max_requests_per_shop)}
                if max_requests_per_shop is not None and int(max_requests_per_shop) > 0
                else None
            )
            try:
                preflight, preflight_payload = _run_preflight_live(
                    client=client,
                    shop_key=shop_key,
                    shop_id=shop_id,
                    access_token=access_token,
                    token_len=token_len,
                    token_sha8=token_sha8,
                    request_budget=request_budget,
                    ignore_cooldown=ignore_cooldown,
                    rate_limit_state_path=resolved_rate_limit_state_path,
                )
                _write_json(
                    shop_raw / f"preflight_{shop_key}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json",
                    preflight_payload,
                    redact=redact,
                )
                preflight_ok = preflight.ok
                preflight_reason = preflight.reason
                preflight_http_status = preflight.http_status
                preflight_api_error = preflight.api_error
                preflight_api_message = preflight.api_message
                preflight_request_id = preflight.request_id
                if not preflight_ok:
                    meta_probe_ok = False
                    meta_probe_reason = "preflight_failed"
                    gms_probe_reason = "preflight_failed"
                else:
                    id_list_rows = _fetch_id_list_live(
                        client=client,
                        shop_key=shop_key,
                        shop_id=shop_id,
                        access_token=access_token,
                        raw_dir=shop_raw,
                        redact=redact,
                        request_budget=request_budget,
                        ignore_cooldown=ignore_cooldown,
                        rate_limit_state_path=resolved_rate_limit_state_path,
                    )
                    campaign_ids = [row.get("campaign_id", "") for row in id_list_rows if row.get("campaign_id")]
                    setting_rows, setting_ok, setting_fail = _fetch_setting_info_live(
                        client=client,
                        shop_key=shop_key,
                        shop_id=shop_id,
                        access_token=access_token,
                        campaign_ids=campaign_ids,
                        raw_dir=shop_raw,
                        redact=redact,
                        request_budget=request_budget,
                        ignore_cooldown=ignore_cooldown,
                        rate_limit_state_path=resolved_rate_limit_state_path,
                    )
                    meta_probe_ok = setting_ok > 0
                    if not meta_probe_ok:
                        if setting_fail > 0:
                            meta_probe_reason = "setting_failed"
                        else:
                            meta_probe_reason = "setting_unavailable"

                    gms_rows, gms_ok = _fetch_gms_live(
                        client=client,
                        shop_key=shop_key,
                        shop_id=shop_id,
                        access_token=access_token,
                        days=days,
                        raw_dir=shop_raw,
                        redact=redact,
                        request_budget=request_budget,
                        ignore_cooldown=ignore_cooldown,
                        rate_limit_state_path=resolved_rate_limit_state_path,
                    )
                    for row in gms_rows:
                        cid = _extract_campaign_id(row)
                        if cid:
                            gms_ids.add(cid)
                    if not gms_ok:
                        gms_probe_reason = "gms_unavailable"
                        gms_payloads = sorted(shop_raw.glob("gms_campaign_performance_try_*.json"))
                        if gms_payloads:
                            last_payload = _read_json_safely(gms_payloads[-1])
                            trace = (last_payload or {}).get("__trace") if isinstance(last_payload, dict) else {}
                            if isinstance(trace, dict):
                                if int(trace.get("skipped_by_cooldown") or 0) == 1:
                                    gms_probe_reason = "cooldown_active"
                                elif int(trace.get("rate_limited") or 0) == 1:
                                    gms_probe_reason = "rate_limited"
                                elif trace.get("api_error"):
                                    gms_probe_reason = str(trace.get("api_error"))
                    else:
                        gms_probe_reason = ""
            finally:
                client.close()

            shop_id = int(shop_cfg.shopee_shop_id or 0)

        id_set = {row.get("campaign_id", ""): row for row in id_list_rows if row.get("campaign_id")}
        for record in setting_rows:
            campaign_id = _extract_campaign_id(record)
            if not campaign_id:
                continue
            row = _normalize_setting_row(
                shop_label=shop_label,
                campaign_id=campaign_id,
                record=record,
                source_endpoint="setting_info_1_4",
            )
            registry_rows.append(row)

        # Include id-list-only rows so missing name/budget visibility is explicit.
        seen_registry_ids = {str(row.get("campaign_id") or "") for row in registry_rows}
        for cid, meta in id_set.items():
            if cid in seen_registry_ids:
                continue
            registry_rows.append(
                {
                    "shop_label": shop_label,
                    "campaign_id": cid,
                    "ad_name": "",
                    "status": "",
                    "daily_budget": "",
                    "total_budget": "",
                    "start_time": "",
                    "end_time": "",
                    "item_count": 0,
                    "item_id_list_json": "[]",
                    "product_name_list_json": "[]",
                    "source_endpoint": "id_list_only",
                }
            )

        registry_rows.sort(key=lambda x: str(x.get("campaign_id") or ""))
        if mode_value == "live" and sync_db:
            _sync_registry_to_db(shop_key, registry_rows)
        registry_all.extend(registry_rows)
        shop_results.append(
            CampaignProbeShopResult(
                shop_key=shop_key,
                shop_label=shop_label,
                shop_id=int(shop_cfg.shopee_shop_id or 0),
                registry_rows=registry_rows,
                gms_campaign_ids=gms_ids,
                id_list_count=len(id_set),
                setting_chunks_ok=setting_ok,
                setting_chunks_fail=setting_fail,
                setting_rows_raw=len(setting_rows),
                gms_ok=gms_ok,
                preflight_ok=preflight_ok,
                preflight_reason=preflight_reason,
                preflight_endpoint="/api/v2/ads/get_total_balance",
                preflight_http_status=preflight_http_status,
                preflight_api_error=preflight_api_error,
                preflight_api_message=preflight_api_message,
                preflight_request_id=preflight_request_id,
                token_len=token_len,
                token_sha8=token_sha8,
                meta_probe_ok=meta_probe_ok,
                meta_probe_reason=meta_probe_reason,
                gms_probe_reason=gms_probe_reason,
            )
        )

    registry_csv_path = normalized_root / "campaign_registry.csv"
    _write_registry_csv(registry_csv_path, registry_all)
    verdict, verdict_bits = _summarize_verdict(shop_results)
    summary_path = _write_summary(
        out_dir=out_dir,
        mode=mode_value,
        days=days,
        shop_results=shop_results,
        verdict=verdict,
        verdict_bits=verdict_bits,
    )

    return {
        "out_dir": str(out_dir),
        "rate_limit_state_path": str(resolved_rate_limit_state_path),
        "registry_csv": str(registry_csv_path),
        "summary_md": str(summary_path),
        "verdict": verdict,
        "verdict_bits": verdict_bits,
        "shop_results": shop_results,
    }


def run_gms_probe(
    *,
    settings,
    target_shops: list,
    mode: str,
    days: int,
    out_dir: Path,
    redact: bool,
    fixture_payload: dict[str, Any] | None = None,
    max_gms_calls_per_shop: int = 1,
    force_once: bool = False,
    sync_db: bool = False,
    rate_limit_state_path: Path | str | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_root = out_dir / "raw"
    normalized_root = out_dir / "normalized"
    normalized_rows_all: list[dict[str, Any]] = []
    shop_results: list[GmsProbeShopResult] = []
    gms_as_of_date = _gms_reference_date(days)
    fetched_at_utc = datetime.now(timezone.utc)

    mode_value = str(mode or "").strip().lower()
    if mode_value not in {"live", "fixtures"}:
        raise RuntimeError("mode must be one of: live, fixtures")

    if mode_value == "live":
        if settings.shopee_partner_id is None:
            raise RuntimeError("SHOPEE_PARTNER_ID is required")
        if not settings.shopee_partner_key:
            raise RuntimeError("SHOPEE_PARTNER_KEY is required")

    resolved_rate_limit_state_path = _resolve_rate_limit_state_path(
        rate_limit_state_path=rate_limit_state_path,
        out_dir=out_dir,
    )

    for shop_cfg in target_shops:
        shop_key = shop_cfg.shop_key
        shop_label = shop_cfg.label
        shop_id = int(shop_cfg.shopee_shop_id or 0)
        shop_raw = raw_root / shop_key
        gms_http_status: int | None = None
        gms_api_error: str | None = None
        gms_api_message: str | None = None
        gms_request_id: str | None = None
        gms_ok_count = 0
        gms_campaign_count = 0
        rate_limit_hit = False
        calls_made = 0
        normalized_rows: list[dict[str, Any]] = []
        probe_reason = ""

        if mode_value == "fixtures":
            fixture_shop = (
                fixture_payload.get(shop_key, {})
                if isinstance(fixture_payload, dict)
                else {}
            )
            gms_payload = fixture_shop.get("gms_campaign_perf") if isinstance(fixture_shop, dict) else None
            if not isinstance(gms_payload, dict):
                gms_payload = fixture_shop.get("gms_campaign_performance") if isinstance(fixture_shop, dict) else None
            if not isinstance(gms_payload, dict):
                gms_payload = {}
            trace_data = gms_payload.get("__trace") if isinstance(gms_payload, dict) else {}
            _write_json(
                shop_raw / "gms_campaign_performance_try_01.json",
                gms_payload,
                redact=redact,
            )
            calls_made = 1
            gms_http_status = (
                int(trace_data.get("http_status"))
                if isinstance(trace_data, dict) and trace_data.get("http_status") not in (None, "")
                else (200 if _api_ok(gms_payload) else None)
            )
            gms_api_error, gms_api_message, gms_request_id = _api_error_parts(gms_payload)
            if gms_api_error == "0":
                gms_api_error = None
            rate_limit_hit = _is_rate_limited(gms_http_status, gms_api_error, gms_api_message)
            if _api_ok(gms_payload):
                gms_ok_count = 1
            else:
                probe_reason = gms_api_error or "gms_unavailable"
            normalized_rows = _normalize_gms_campaign_rows(
                shop_key=shop_key,
                shop_label=shop_label,
                payload=gms_payload,
                raw_source="gms_campaign_performance_try_01.json",
            )
            gms_campaign_count = len(normalized_rows)
        else:
            access_token = ""
            try:
                access_token, shop_id = _ensure_live_token(settings=settings, shop_cfg=shop_cfg)
            except Exception as exc:  # noqa: BLE001
                probe_reason = "token_missing"
                gms_api_error = "token_missing"
                gms_api_message = redact_text(str(exc))
                _write_json(
                    shop_raw / "gms_campaign_performance_try_01.json",
                    {
                        "error": gms_api_error,
                        "message": gms_api_message,
                        "shop_key": shop_key,
                    },
                    redact=redact,
                )
                calls_made = 0
            else:
                client = _build_client(settings)
                try:
                    rows, gms_ok, trace, calls_made, rate_limit_hit = _fetch_gms_probe_live(
                        client=client,
                        shop_key=shop_key,
                        shop_id=shop_id,
                        access_token=access_token,
                        days=days,
                        raw_dir=shop_raw,
                        redact=redact,
                        request_budget=None,
                        ignore_cooldown=force_once,
                        rate_limit_state_path=resolved_rate_limit_state_path,
                        max_calls=max_gms_calls_per_shop,
                    )
                    if trace is not None:
                        gms_http_status = trace.http_status
                        gms_api_error = trace.api_error
                        gms_api_message = trace.api_message
                        gms_request_id = trace.request_id
                        if gms_api_error == "0":
                            gms_api_error = None
                        if trace.skipped_by_cooldown:
                            probe_reason = "cooldown_active"
                        elif trace.rate_limited:
                            probe_reason = "rate_limited"
                    if gms_ok:
                        gms_ok_count = 1
                        probe_reason = ""
                    elif not probe_reason:
                        probe_reason = gms_api_error or "gms_unavailable"
                    raw_source = (
                        f"gms_campaign_performance_try_{max(calls_made, 1):02d}.json"
                    )
                    normalized_rows = _normalize_gms_campaign_rows(
                        shop_key=shop_key,
                        shop_label=shop_label,
                        payload={"response": {"records": rows}},
                        raw_source=raw_source,
                    )
                    gms_campaign_count = len(normalized_rows)
                finally:
                    client.close()

        normalized_rows_all.extend(normalized_rows)

        campaign_level_supported = "unknown"
        gms_name_supported = "unknown"
        gms_budget_supported = "unknown"
        if gms_ok_count > 0:
            campaign_level_supported = (
                "yes"
                if any(str(row.get("campaign_id") or "").strip() for row in normalized_rows)
                else "no"
            )
            gms_name_supported = (
                "yes"
                if any(str(row.get("campaign_name") or "").strip() for row in normalized_rows)
                else "no"
            )
            gms_budget_supported = (
                "yes"
                if any(
                    str(row.get("daily_budget") or "").strip()
                    or str(row.get("total_budget") or "").strip()
                    for row in normalized_rows
                )
                else "no"
            )

        shop_results.append(
            GmsProbeShopResult(
                shop_key=shop_key,
                shop_label=shop_label,
                shop_id=shop_id,
                gms_http_status=gms_http_status,
                gms_api_error=gms_api_error,
                gms_api_message=gms_api_message,
                gms_request_id=gms_request_id,
                gms_ok_count=gms_ok_count,
                gms_campaign_count=gms_campaign_count,
                rate_limit_hit=bool(rate_limit_hit),
                calls_made=calls_made,
                campaign_level_supported=campaign_level_supported,
                gms_name_supported=gms_name_supported,
                gms_budget_supported=gms_budget_supported,
                probe_reason=probe_reason,
                normalized_rows=normalized_rows,
            )
        )

    gms_registry_csv_path = normalized_root / "gms_campaign_registry.csv"
    _write_gms_registry_csv(gms_registry_csv_path, normalized_rows_all)
    db_upserted = 0
    if sync_db and normalized_rows_all:
        db_upserted = _sync_gms_registry_to_db(
            rows=normalized_rows_all,
            as_of_date=gms_as_of_date,
            fetched_at_utc=fetched_at_utc,
            source_run_dir=str(out_dir),
        )
    verdict, verdict_bits = _summarize_gms_verdict(shop_results)
    summary_path = _write_gms_probe_summary(
        out_dir=out_dir,
        mode=mode_value,
        days=days,
        max_gms_calls_per_shop=max(1, int(max_gms_calls_per_shop)),
        force_once=force_once,
        shop_results=shop_results,
        verdict=verdict,
    )
    return {
        "out_dir": str(out_dir),
        "rate_limit_state_path": str(resolved_rate_limit_state_path),
        "gms_registry_csv": str(gms_registry_csv_path),
        "summary_md": str(summary_path),
        "verdict": verdict,
        "verdict_bits": verdict_bits,
        "as_of_date": gms_as_of_date.isoformat(),
        "db_upserted": db_upserted,
        "shop_results": shop_results,
    }

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date as date_type, datetime, time, timedelta, timezone
import hashlib
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import uuid
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func

from . import __version__
from .ads.campaign_probe import read_ads_rate_limit_status, resolve_rate_limit_state_path_info
from .ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from .config import get_settings, load_shops
from .db import EventLog, SessionLocal, init_db
from .discord_notifier import build_report_url
from .ops.alert_dispatch import dispatch_alert_card
from .shopee.token_store import get_token, upsert_token
from .token_preflight_gate import (
    emit_token_resolved_alerts_with_cooldown,
    evaluate_token_preflight_gate,
    load_token_preflight_gate_status_snapshot,
)

try:
    from .ads.reporting import (
        BREAKDOWN_SCOPE_NOTE,
        BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY,
        GMS_GROUP_SCOPE_AGGREGATE_ONLY,
    )
except Exception:
    # Keep webapp import-safe across mixed production revisions.
    BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY = "product_level_only"
    GMS_GROUP_SCOPE_AGGREGATE_ONLY = "aggregate_only"
    BREAKDOWN_SCOPE_NOTE = (
        "product-level campaigns are per-campaign; "
        "group/shop/auto-selected scopes are aggregate-only"
    )

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=lifespan)

settings = get_settings()
REPORTS_DIR = Path(settings.reports_dir)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
OPS_TIMEZONE_NAME = "Asia/Ho_Chi_Minh"
OPS_TIMEZONE = ZoneInfo(OPS_TIMEZONE_NAME)
_TOKEN_IMPORT_EVENT_MESSAGE = "phase1_token_import_event"
_TOKEN_IMPORT_SUMMARY_MESSAGE = "phase1_token_import_summary"


@app.middleware("http")
async def reports_token_guard(request: Request, call_next):
    settings = get_settings()
    token = settings.report_access_token
    if token and request.url.path.startswith("/reports"):
        provided = request.query_params.get("token")
        if provided != token:
            return PlainTextResponse("Unauthorized", status_code=401)
    return await call_next(request)


@app.middleware("http")
async def ops_no_store_headers(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/ops/phase1/"):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/reports", include_in_schema=False)
def reports_redirect(request: Request) -> RedirectResponse:
    token = request.query_params.get("token", "").strip()
    if token:
        return RedirectResponse(url=f"/reports/?token={quote(token)}")
    return RedirectResponse(url="/reports/")


@app.get("/reports/", response_class=HTMLResponse, include_in_schema=False)
def reports_index(request: Request) -> HTMLResponse:
    shop_dirs = sorted([p for p in REPORTS_DIR.iterdir() if p.is_dir()], key=lambda p: p.name)
    catalog: dict[str, dict[str, Any]] = {}
    for shop_dir in shop_dirs:
        shop_key = shop_dir.name
        daily_dir = shop_dir / "daily"
        weekly_dir = shop_dir / "weekly"
        daily_midday: list[str] = []
        daily_final: list[str] = []
        weekly: list[str] = []

        if daily_dir.exists():
            for path in sorted(daily_dir.glob("*_midday.html")):
                name = path.name
                if len(name) >= 17:
                    daily_midday.append(name[:10])
            for path in sorted(daily_dir.glob("*_final.html")):
                name = path.name
                if len(name) >= 16:
                    daily_final.append(name[:10])

        if weekly_dir.exists():
            for path in sorted(weekly_dir.glob("*.html"), reverse=True):
                weekly.append(path.stem)

        daily_all = sorted(set(daily_midday + daily_final))
        default_daily = (daily_final[-1] if daily_final else (daily_all[-1] if daily_all else ""))
        default_week = weekly[0] if weekly else ""
        catalog[shop_key] = {
            "daily_midday": daily_midday,
            "daily_final": daily_final,
            "daily_all": daily_all,
            "weekly": weekly,
            "default_daily": default_daily,
            "default_week": default_week,
        }

    token_value = request.query_params.get("token", "").strip()
    default_shop = sorted(catalog.keys())[0] if catalog else ""
    shops_options = "".join(
        [
            f"<option value='{shop_key}'>{shop_key}</option>"
            for shop_key in sorted(catalog.keys())
        ]
    )
    token_text = "enabled" if token_value else "disabled"
    lines = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Reports</title>",
        "<style>",
        ":root{--bg:#f7fafc;--card:#ffffff;--line:#d7e0ea;--text:#0f172a;--muted:#475569;--brand:#0f766e}",
        "*{box-sizing:border-box}",
        "body{margin:0;padding:24px;background:linear-gradient(160deg,#f8fbff 0%,#eef7ff 40%,#f6fbf7 100%);color:var(--text);font-family:'Pretendard','Noto Sans KR','Segoe UI',sans-serif}",
        ".wrap{max-width:1040px;margin:0 auto;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:24px;box-shadow:0 14px 36px rgba(15,23,42,.08)}",
        "h1{font-size:28px;line-height:1.2;margin:0 0 8px 0}",
        "h2{font-size:18px;margin:20px 0 8px 0}",
        ".muted{color:var(--muted);font-size:13px;margin-bottom:8px}",
        ".row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:8px 0 10px 0}",
        "label{font-size:13px;color:var(--muted)}",
        "select,input,button{height:38px;border:1px solid var(--line);border-radius:10px;padding:0 10px;font-size:14px;background:#fff;color:var(--text)}",
        "select,input{min-width:180px}",
        "button{background:var(--brand);border-color:var(--brand);color:#fff;font-weight:700;cursor:pointer}",
        "button:disabled{opacity:.5;cursor:not-allowed}",
        ".section{border:1px solid var(--line);border-radius:12px;padding:12px;background:#fbfeff}",
        ".meta{font-size:12px;color:var(--muted)}",
        ".links ul{padding-left:18px;margin:8px 0}",
        ".links li{margin:4px 0}",
        "@media (max-width:640px){body{padding:12px}.wrap{padding:14px;border-radius:12px}h1{font-size:22px}.row{gap:8px}select,input{min-width:140px;flex:1}}",
        "</style>",
        "</head>",
        "<body>",
        "<main class='wrap'>",
        "<h1>Reports</h1>",
        f"<div class='muted'>Token guard: {token_text} | Base path: /reports</div>",
    ]
    if catalog:
        lines.extend(
            [
                "<div class='section'>",
                "<h2>조회 대상</h2>",
                "<div class='row'>",
                "<label for='shop-key'>Shop</label>",
                f"<select id='shop-key'>{shops_options}</select>",
                "</div>",
                "<div class='meta' id='shop-meta'></div>",
                "</div>",
                "<div class='section'>",
                "<h2>Daily Report (final)</h2>",
                "<div class='row'>",
                "<label for='daily-date'>Date</label>",
                "<input id='daily-date' type='date'>",
                "<button id='daily-open-btn' type='button'>조회</button>",
                "</div>",
                "<div class='meta' id='daily-meta'></div>",
                "</div>",
                "<div class='section'>",
                "<h2>Weekly Report</h2>",
                "<div class='row'>",
                "<label for='weekly-week'>Week</label>",
                "<select id='weekly-week'></select>",
                "<button id='weekly-open-btn' type='button'>조회</button>",
                "</div>",
                "<div class='meta' id='weekly-meta'></div>",
                "</div>",
                "<div class='links'>",
                "<h2>Raw directories</h2>",
                "<ul>",
            ]
        )
        for shop_key in sorted(catalog.keys()):
            lines.append(f"<li><a href='/reports/{shop_key}'>{shop_key}</a></li>")
        lines.extend(
            [
                "</ul>",
                "</div>",
                "<script>",
                f"const reportCatalog = {json.dumps(catalog, ensure_ascii=False)};",
                f"const defaultShopKey = {json.dumps(default_shop)};",
                f"const initialToken = {json.dumps(token_value)};",
                "function withToken(path){ if(!initialToken){ return path; } return `${path}?token=${encodeURIComponent(initialToken)}`; }",
                "function getShopCatalog(){ const s=document.getElementById('shop-key').value; return reportCatalog[s] || null; }",
                "function setWeeklyOptions(shopData){ const weekly=document.getElementById('weekly-week'); weekly.innerHTML=''; const rows=(shopData && shopData.weekly) ? shopData.weekly : []; if(rows.length===0){ const o=document.createElement('option'); o.value=''; o.textContent='(no weekly reports)'; weekly.appendChild(o); weekly.disabled=true; return; } weekly.disabled=false; for(const weekId of rows){ const o=document.createElement('option'); o.value=weekId; o.textContent=weekId; weekly.appendChild(o);} weekly.value=(shopData.default_week || rows[0]); }",
                "function setDailyDefaults(shopData){ const dateInput=document.getElementById('daily-date'); const rows=shopData.daily_final || []; if(rows.length>0){ dateInput.value=rows[rows.length-1]; } else if((shopData.daily_all||[]).length>0){ dateInput.value=shopData.daily_all[shopData.daily_all.length-1]; } else { dateInput.value=''; } }",
                "function refreshMeta(){ const shop=document.getElementById('shop-key').value; const c=getShopCatalog(); const dailyMeta=document.getElementById('daily-meta'); const weeklyMeta=document.getElementById('weekly-meta'); const shopMeta=document.getElementById('shop-meta'); if(!c){ shopMeta.textContent='No shop data'; dailyMeta.textContent=''; weeklyMeta.textContent=''; return; } const finalCount=(c.daily_final||[]).length; const weeklyCount=(c.weekly||[]).length; const allRows=(c.daily_all||[]); let dailyRange='-'; if(allRows.length>0){ dailyRange=`${allRows[0]} -> ${allRows[allRows.length-1]}`; } shopMeta.textContent=`shop=${shop} | daily total days=${allRows.length} | weekly files=${weeklyCount}`; dailyMeta.textContent=`available final range: ${dailyRange} | final=${finalCount}`; weeklyMeta.textContent=(weeklyCount>0) ? `available weeks: ${weeklyCount}` : 'no weekly reports'; }",
                "function onShopChange(){ const c=getShopCatalog(); if(!c){ return; } setWeeklyOptions(c); setDailyDefaults(c); refreshMeta(); }",
                "function openDaily(){ const shop=document.getElementById('shop-key').value; const date=document.getElementById('daily-date').value; if(!shop || !date){ return; } const path=`/reports/${shop}/daily/${date}_final.html`; window.location.href=withToken(path); }",
                "function openWeekly(){ const shop=document.getElementById('shop-key').value; const week=document.getElementById('weekly-week').value; if(!shop || !week){ return; } const path=`/reports/${shop}/weekly/${week}.html`; window.location.href=withToken(path); }",
                "document.getElementById('shop-key').addEventListener('change', onShopChange);",
                "document.getElementById('daily-open-btn').addEventListener('click', openDaily);",
                "document.getElementById('weekly-open-btn').addEventListener('click', openWeekly);",
                "if(defaultShopKey){ document.getElementById('shop-key').value=defaultShopKey; onShopChange(); }",
                "</script>",
            ]
        )
    else:
        lines.extend(
            [
                "<div class='section'>",
                "<h2>조회 대상</h2>",
                "<div class='row'>",
                "<label for='shop-key'>Shop</label>",
                "<select id='shop-key' disabled><option value=''>no shops</option></select>",
                "</div>",
                "<div class='meta'>No reports yet.</div>",
                "</div>",
                "<div class='section'>",
                "<h2>Daily Report</h2>",
                "<div class='row'>",
                "<label for='daily-date'>Date</label>",
                "<input id='daily-date' type='date' disabled>",
                "<button id='daily-open-btn' type='button' disabled>조회</button>",
                "</div>",
                "</div>",
                "<div class='section'>",
                "<h2>Weekly Report</h2>",
                "<div class='row'>",
                "<label for='weekly-week'>Week</label>",
                "<select id='weekly-week' disabled><option value=''>no weeks</option></select>",
                "<button id='weekly-open-btn' type='button' disabled>조회</button>",
                "</div>",
                "</div>",
            ]
        )
    lines.append("</main></body></html>")
    return HTMLResponse("\n".join(lines))


app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")


def _sha256_8(value: str | None) -> str:
    if not value:
        return "-"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _extract_appsscript_token_map(data: object) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict):
        return {}
    shops_section = data.get("shops")
    tokens_section = data.get("tokens")
    if isinstance(shops_section, dict):
        source = shops_section
    elif isinstance(tokens_section, dict):
        source = tokens_section
    else:
        source = data
    token_map: dict[str, dict[str, Any]] = {}
    for key, value in source.items():
        key_str = str(key)
        shop_id: str | None = None
        if key_str.isdigit():
            shop_id = key_str
        elif key_str.startswith("SHOPEE_TOKEN_DATA_"):
            suffix = key_str.replace("SHOPEE_TOKEN_DATA_", "", 1)
            if suffix.isdigit():
                shop_id = suffix

        payload = value
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                continue
        if shop_id is None and isinstance(payload, dict):
            nested_shop_id = payload.get("shop_id")
            try:
                nested_shop_id_value = int(nested_shop_id)
            except Exception:
                nested_shop_id_value = 0
            if nested_shop_id_value > 0:
                shop_id = str(nested_shop_id_value)
        if shop_id is None:
            continue
        if isinstance(payload, dict):
            token_map[shop_id] = payload
    return token_map


def _resolve_phase1_shop_id_map() -> dict[str, str]:
    settings = get_settings()
    phase1_keys = {"samord", "minmin"}
    shops = {shop.shop_key: shop for shop in load_shops()}
    env_ids = {
        "samord": settings.shopee_samord_shop_id,
        "minmin": settings.shopee_minmin_shop_id,
    }
    mapping: dict[str, str] = {}
    for shop_key in sorted(phase1_keys):
        shop_id = env_ids.get(shop_key)
        shop_cfg = shops.get(shop_key)
        if shop_id is None and shop_cfg and shop_cfg.shopee_shop_id is not None:
            shop_id = int(shop_cfg.shopee_shop_id)
        if shop_id is not None:
            mapping[str(int(shop_id))] = shop_key
    return mapping


def _resolve_ops_token() -> str | None:
    settings = get_settings()
    candidates = [
        settings.ops_token,
        settings.dotori_ops_token,
        settings.reports_token,
        settings.report_access_token,
    ]
    for value in candidates:
        if value and value.strip():
            return value.strip()
    return None


def _extract_request_ops_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return request.headers.get("X-OPS-TOKEN", "").strip()


def _require_ops_auth(request: Request) -> None:
    expected_token = _resolve_ops_token()
    if not expected_token:
        raise HTTPException(status_code=503, detail="ops_token_not_configured")

    provided_token = _extract_request_ops_token(request)
    if not provided_token or provided_token != expected_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _to_utc_iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _unix_to_utc_iso_or_none(value: object) -> str | None:
    try:
        parsed = int(value)
    except Exception:
        return None
    if parsed < 0:
        return None
    return datetime.fromtimestamp(parsed, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _compute_access_expires_in_sec(access_expires_at: datetime | None) -> int:
    if access_expires_at is None:
        return -1
    now = _now_utc()
    dt = access_expires_at if access_expires_at.tzinfo else access_expires_at.replace(
        tzinfo=timezone.utc
    )
    return int((dt.astimezone(timezone.utc) - now).total_seconds())


def _resolve_phase1_shops() -> dict[str, dict[str, object]]:
    settings = get_settings()
    env_shop_ids = {
        "samord": settings.shopee_samord_shop_id,
        "minmin": settings.shopee_minmin_shop_id,
    }
    configs = {shop.shop_key: shop for shop in load_shops()}
    rows: dict[str, dict[str, object]] = {}
    for key in ("samord", "minmin"):
        cfg = configs.get(key)
        shop_id = env_shop_ids.get(key)
        label = key.upper()
        if cfg:
            label = cfg.label or label
            if cfg.shopee_shop_id is not None:
                shop_id = int(cfg.shopee_shop_id)
        rows[key] = {
            "shop_key": key,
            "label": label,
            "shop_id": int(shop_id) if shop_id is not None else None,
        }
    return rows


def _normalize_event_time_to_iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return _to_utc_iso_or_none(value)
    if isinstance(value, str):
        parsed = _parse_iso_datetime(value)
        if parsed is not None:
            return _to_utc_iso_or_none(parsed)
    if isinstance(value, (int, float)):
        return _unix_to_utc_iso_or_none(value)
    return None


def _load_latest_token_import_state(
    *,
    session,
    shop_keys: set[str],
) -> tuple[dict[str, dict[str, str | None]], dict[str, str | None]]:
    per_shop: dict[str, dict[str, str | None]] = {}
    global_last_at: str | None = None
    global_request_id: str | None = None
    global_token_mode: str | None = None
    rows = (
        session.query(EventLog.message, EventLog.meta_json)
        .filter(
            EventLog.message.in_(
                [_TOKEN_IMPORT_EVENT_MESSAGE, _TOKEN_IMPORT_SUMMARY_MESSAGE]
            )
        )
        .order_by(EventLog.id.desc())
        .limit(2000)
        .all()
    )
    for row in rows:
        raw_message = row[0] if isinstance(row, tuple) else getattr(row, "message", None)
        raw_meta = row[1] if isinstance(row, tuple) else getattr(row, "meta_json", None)
        if not raw_meta:
            continue
        try:
            payload = json.loads(raw_meta)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        message = str(raw_message or "")
        imported_at = _normalize_event_time_to_iso(
            payload.get("imported_at_utc") or payload.get("imported_at")
        )
        request_id = (
            str(payload.get("request_id")).strip() if payload.get("request_id") else None
        )
        token_mode = (
            str(payload.get("token_mode")).strip().lower()
            if payload.get("token_mode") is not None
            else None
        )
        if message == _TOKEN_IMPORT_SUMMARY_MESSAGE and global_last_at is None:
            global_last_at = imported_at
            global_request_id = request_id
            global_token_mode = token_mode
            continue
        if message != _TOKEN_IMPORT_EVENT_MESSAGE:
            continue
        shop_key = str(payload.get("shop_key") or "").strip()
        if not shop_key or shop_key not in shop_keys or shop_key in per_shop:
            continue
        source = str(payload.get("source") or "").strip() or None
        per_shop[shop_key] = {
            "token_source": source,
            "token_mode": token_mode,
            "token_import_last_at": imported_at,
            "token_import_last_request_id": request_id,
        }
        if global_last_at is None and imported_at is not None:
            global_last_at = imported_at
            global_request_id = request_id
            global_token_mode = token_mode
    return per_shop, {
        "token_import_last_at": global_last_at,
        "token_import_last_request_id": global_request_id,
        "token_mode": global_token_mode,
    }


def _pick_shop_next_action(*, token_len: int, access_ttl: int, gate_state: str) -> str | None:
    if token_len <= 0:
        return "Run Apps Script push now: refreshAndPushPhase1TokensToServer()"
    if access_ttl <= 0:
        return "Run Apps Script push now: access token expired"
    if gate_state == "blocked":
        return "Run Apps Script push now and re-check gate status"
    return None


def _build_phase1_token_status_payload(
    *,
    with_meta: bool = False,
) -> dict[str, dict[str, Any]] | dict[str, Any]:
    phase1_shops = _resolve_phase1_shops()
    gate_snapshot = load_token_preflight_gate_status_snapshot(
        shops=[
            SimpleNamespace(
                shop_key=key,
                label=str(meta.get("label") or key.upper()),
                shopee_shop_id=meta.get("shop_id"),
            )
            for key, meta in phase1_shops.items()
        ]
    )

    init_db()
    session = SessionLocal()
    try:
        shop_keys = set(phase1_shops.keys())
        import_meta_by_shop, import_meta_global = _load_latest_token_import_state(
            session=session,
            shop_keys=shop_keys,
        )
        shops_payload: dict[str, dict[str, Any]] = {}
        for shop_key, meta in phase1_shops.items():
            token = get_token(session, shop_key)
            access_token = token.access_token if token else None
            token_len = len(access_token) if access_token else 0
            refresh_token = token.refresh_token if token else None
            has_refresh_token = 1 if str(refresh_token or "").strip() else 0
            gate = gate_snapshot.get(shop_key, {})
            access_ttl = _compute_access_expires_in_sec(
                token.access_token_expires_at if token else None
            )
            refresh_ttl = _compute_access_expires_in_sec(
                token.refresh_token_expires_at if token else None
            )
            import_meta = import_meta_by_shop.get(shop_key, {})
            token_source = str(import_meta.get("token_source") or "").strip()
            if not token_source:
                token_source = "db" if token_len > 0 else "missing"
            token_mode = str(import_meta.get("token_mode") or "").strip().lower()
            if not token_mode:
                token_mode = "legacy" if has_refresh_token == 1 else "access_only"
            gate_state = str(gate.get("gate_state") or "unknown")
            shops_payload[shop_key] = {
                "shop_id": int(meta.get("shop_id")) if meta.get("shop_id") is not None else None,
                "token_len": token_len,
                "token_sha8": _sha256_8(access_token),
                "access_expires_in_sec": access_ttl,
                "expires_in_sec": access_ttl,
                "updated_at": _to_utc_iso_or_none(token.updated_at if token else None),
                "gate_state": gate_state,
                "cooldown_until": _unix_to_utc_iso_or_none(gate.get("cooldown_until")),
                "resolved_cooldown_until": _unix_to_utc_iso_or_none(
                    gate.get("resolved_cooldown_until")
                ),
                "token_source": token_source,
                "token_mode": token_mode,
                "has_refresh_token": has_refresh_token,
                "refresh_expires_in_sec": refresh_ttl,
                "token_import_last_at": import_meta.get("token_import_last_at"),
                "token_import_last_request_id": import_meta.get(
                    "token_import_last_request_id"
                ),
                "next_action": _pick_shop_next_action(
                    token_len=token_len,
                    access_ttl=access_ttl,
                    gate_state=gate_state,
                ),
            }
    finally:
        session.close()

    if with_meta:
        global_token_mode = (
            str(import_meta_global.get("token_mode") or "").strip().lower()
            if isinstance(import_meta_global, dict)
            else ""
        )
        if not global_token_mode:
            has_refresh_any = any(
                int((row.get("has_refresh_token") or 0)) == 1
                for row in shops_payload.values()
                if isinstance(row, dict)
            )
            global_token_mode = "legacy" if has_refresh_any else "access_only"
        return {
            "shops": shops_payload,
            "token_import_last_at": import_meta_global.get("token_import_last_at"),
            "token_import_last_request_id": import_meta_global.get(
                "token_import_last_request_id"
            ),
            "token_mode": global_token_mode,
        }
    return shops_payload


def _local_to_schedule_payload(local_dt: datetime) -> dict[str, str]:
    local_norm = local_dt.astimezone(OPS_TIMEZONE).replace(second=0, microsecond=0)
    utc_value = local_norm.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "local": local_norm.strftime("%Y-%m-%d %H:%M"),
        "utc": utc_value,
    }


def _next_daily_local(now_local: datetime, hour: int) -> datetime:
    candidate = now_local.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate


def _next_weekly_local(now_local: datetime) -> datetime:
    days_until_monday = (7 - now_local.weekday()) % 7
    target_date = now_local.date() + timedelta(days=days_until_monday)
    candidate = datetime.combine(target_date, time(hour=9, minute=0), tzinfo=OPS_TIMEZONE)
    if candidate <= now_local:
        candidate += timedelta(days=7)
    return candidate


def _next_quarter_hour_local(now_local: datetime) -> datetime:
    probe = now_local + timedelta(minutes=1)
    probe = probe.replace(second=0, microsecond=0)
    remainder = probe.minute % 15
    if remainder:
        probe += timedelta(minutes=(15 - remainder))
    return probe


def _build_phase1_schedule_payload(now_utc: datetime | None = None) -> dict[str, Any]:
    base_utc = now_utc if now_utc is not None else _now_utc()
    local_now = base_utc.astimezone(OPS_TIMEZONE)

    return {
        "daily_final": _local_to_schedule_payload(_next_daily_local(local_now, hour=0)),
        "daily_midday": _local_to_schedule_payload(_next_daily_local(local_now, hour=13)),
        "weekly": _local_to_schedule_payload(_next_weekly_local(local_now)),
        "alerts_15m": _local_to_schedule_payload(_next_quarter_hour_local(local_now)),
        "is_business_hours": 9 <= local_now.hour < 18,
    }


def _resolve_db_engine_name() -> str:
    database_url = str(get_settings().database_url or "").lower()
    if database_url.startswith("sqlite"):
        return "sqlite"
    if database_url.startswith("postgresql"):
        return "postgresql"
    if database_url.startswith("mysql"):
        return "mysql"
    if "://" in database_url:
        return database_url.split("://", 1)[0]
    return "unknown"


def _to_local_iso_or_none(value: object) -> str | None:
    if not isinstance(value, datetime):
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(OPS_TIMEZONE).isoformat()


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip() or str(default)
    try:
        value = int(raw)
    except ValueError:
        value = int(default)
    return value if value > 0 else int(default)


def _build_phase1_db_payload() -> dict[str, Any]:
    shop_keys = list(_resolve_phase1_shops().keys())
    empty_latest = {
        shop_key: {
            "daily_latest_date": None,
            "snapshot_latest_at": None,
        }
        for shop_key in shop_keys
    }
    init_db()
    session = SessionLocal()
    try:
        row_counts = {
            "ads_campaign": int(
                session.query(func.count(AdsCampaign.id))
                .filter(AdsCampaign.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "ads_daily": int(
                session.query(func.count(AdsCampaignDaily.id))
                .filter(AdsCampaignDaily.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "ads_snapshot": int(
                session.query(func.count(AdsCampaignSnapshot.id))
                .filter(AdsCampaignSnapshot.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
        }
        latest_ingest: dict[str, dict[str, str | None]] = {}
        for shop_key in shop_keys:
            daily_latest = (
                session.query(func.max(AdsCampaignDaily.date))
                .filter(AdsCampaignDaily.shop_key == shop_key)
                .scalar()
            )
            snapshot_latest = (
                session.query(func.max(AdsCampaignSnapshot.ts))
                .filter(AdsCampaignSnapshot.shop_key == shop_key)
                .scalar()
            )
            latest_ingest[shop_key] = {
                "daily_latest_date": (
                    daily_latest.isoformat() if daily_latest is not None else None
                ),
                "snapshot_latest_at": _to_local_iso_or_none(snapshot_latest),
            }
        return {
            "ok": True,
            "engine": _resolve_db_engine_name(),
            "row_counts": row_counts,
            "latest_ingest": latest_ingest,
        }
    except Exception:  # noqa: BLE001
        return {
            "ok": False,
            "engine": _resolve_db_engine_name(),
            "error": "db_unavailable",
            "row_counts": {
                "ads_campaign": 0,
                "ads_daily": 0,
                "ads_snapshot": 0,
            },
            "latest_ingest": empty_latest,
        }
    finally:
        session.close()


def _build_phase1_doctor_notify_payload() -> dict[str, dict[str, Any]]:
    phase1_shops = _resolve_phase1_shops()
    defaults = {
        shop_key: {
            "last_action": "never",
            "last_sent_at": None,
            "cooldown_until": None,
            "resolved_cooldown_until": None,
        }
        for shop_key in phase1_shops.keys()
    }

    init_db()
    session = SessionLocal()
    try:
        from .ops.doctor_notify import OpsDoctorNotifyState

        rows = session.query(OpsDoctorNotifyState).all()
        by_label = {
            str(row.shop_label or "").strip(): row
            for row in rows
            if str(row.shop_label or "").strip()
        }
        by_label_upper = {key.upper(): row for key, row in by_label.items()}

        out: dict[str, dict[str, Any]] = {}
        for shop_key, meta in phase1_shops.items():
            label = str(meta.get("label") or shop_key.upper())
            row = by_label.get(label) or by_label_upper.get(label.upper())
            if row is None:
                out[shop_key] = dict(defaults[shop_key])
                continue

            cooldown_until = _parse_iso_datetime(getattr(row, "cooldown_until", None))
            resolved_cooldown_until = _parse_iso_datetime(
                getattr(row, "resolved_cooldown_until", None)
            )
            raw_action = str(getattr(row, "last_action", "") or "").strip().lower()
            last_action = raw_action if raw_action in {"alert", "resolved", "ok"} else ""
            last_sent_at = _parse_iso_datetime(getattr(row, "last_sent_at", None))
            if not last_action:
                alert_at = _parse_iso_datetime(getattr(row, "last_alert_at", None))
                resolved_at = _parse_iso_datetime(getattr(row, "last_resolved_at", None))
                if alert_at and (resolved_at is None or alert_at > resolved_at):
                    last_action = "alert"
                    last_sent_at = alert_at
                elif resolved_at is not None:
                    last_action = "resolved"
                    last_sent_at = resolved_at
                else:
                    last_action = "ok"
                    last_sent_at = None

            out[shop_key] = {
                "last_action": last_action,
                "last_sent_at": _to_utc_iso_or_none(last_sent_at),
                "cooldown_until": _to_utc_iso_or_none(cooldown_until),
                "resolved_cooldown_until": _to_utc_iso_or_none(resolved_cooldown_until),
            }
        return out
    except Exception:  # noqa: BLE001
        return defaults
    finally:
        session.close()


def _pick_latest_html_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists() or not directory.is_dir():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None

    def _daily_date_key(path: Path) -> tuple[int, int, int] | None:
        match = re.match(r"^(\d{4})-(\d{2})-(\d{2})_(?:midday|final)\.html$", path.name)
        if not match:
            return None
        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))

    def _weekly_key(path: Path) -> tuple[int, int] | None:
        match = re.match(r"^(\d{4})-W(\d{2})\.html$", path.name)
        if not match:
            return None
        return (int(match.group(1)), int(match.group(2)))

    def _sort_key(path: Path) -> tuple[int, tuple[int, ...], float]:
        # Prefer report-id chronology over mtime so re-rendering an old day
        # does not move "latest" pointers backwards.
        if pattern in {"*_midday.html", "*_final.html"}:
            parsed = _daily_date_key(path)
            if parsed is not None:
                return (2, parsed, float(path.stat().st_mtime))
        elif pattern == "*.html":
            parsed_week = _weekly_key(path)
            if parsed_week is not None:
                return (1, parsed_week, float(path.stat().st_mtime))
        return (0, (), float(path.stat().st_mtime))

    candidates.sort(key=_sort_key, reverse=True)
    return candidates[0]


def _is_safe_report_relpath(value: str) -> bool:
    raw = value.replace("\\", "/").strip()
    if not raw or raw.startswith("/") or raw.startswith("\\"):
        return False
    parts = Path(raw).parts
    return ".." not in parts


def _build_report_pointer(
    *,
    reports_root: Path,
    file_path: Path | None,
    now_utc: datetime,
    stale_after_hours: int,
) -> dict[str, Any] | None:
    if file_path is None:
        return None
    try:
        relpath = file_path.resolve().relative_to(reports_root.resolve()).as_posix()
    except Exception:  # noqa: BLE001
        return None
    if not _is_safe_report_relpath(relpath):
        return None

    url_value: str | None = None
    _, masked_url = build_report_url(f"reports/{relpath}")
    if masked_url:
        url_value = masked_url
    stat = file_path.stat()
    updated_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    age_hours = round(max((now_utc - updated_at).total_seconds(), 0.0) / 3600.0, 2)

    return {
        "relpath": relpath,
        "size": int(stat.st_size),
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
        "age_hours": age_hours,
        "is_stale": bool(age_hours > stale_after_hours),
        "url": url_value,
    }


def _build_phase1_reports_payload(now_utc: datetime | None = None) -> dict[str, Any]:
    settings = get_settings()
    reports_root = Path(settings.reports_dir)
    base_url = (settings.report_base_url or "").strip() or None
    now_value = now_utc or _now_utc()
    stale_after_hours = int(_status_thresholds()["report_stale_after_hours"])

    latest: dict[str, dict[str, Any]] = {}
    for shop_key in _resolve_phase1_shops().keys():
        daily_dir = reports_root / shop_key / "daily"
        weekly_dir = reports_root / shop_key / "weekly"
        daily_midday = _pick_latest_html_file(daily_dir, "*_midday.html")
        daily_final = _pick_latest_html_file(daily_dir, "*_final.html")
        weekly = _pick_latest_html_file(weekly_dir, "*.html")
        latest[shop_key] = {
            "daily_midday": _build_report_pointer(
                reports_root=reports_root,
                file_path=daily_midday,
                now_utc=now_value,
                stale_after_hours=stale_after_hours,
            ),
            "daily_final": _build_report_pointer(
                reports_root=reports_root,
                file_path=daily_final,
                now_utc=now_value,
                stale_after_hours=stale_after_hours,
            ),
            "weekly": _build_report_pointer(
                reports_root=reports_root,
                file_path=weekly,
                now_utc=now_value,
                stale_after_hours=stale_after_hours,
            ),
        }
        if base_url is None:
            for kind in ("daily_midday", "daily_final", "weekly"):
                pointer = latest[shop_key].get(kind)
                if isinstance(pointer, dict):
                    pointer["url"] = None

    return {
        "base_url": base_url,
        "latest": latest,
    }


def _parse_iso_date(value: object) -> date_type | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return date_type.fromisoformat(raw)
    except ValueError:
        return None


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _status_thresholds() -> dict[str, int]:
    return {
        "daily_stale_after_days": _parse_positive_int_env(
            "DOTORI_STATUS_DAILY_STALE_AFTER_DAYS", 2
        ),
        "snapshot_stale_after_minutes": _parse_positive_int_env(
            "DOTORI_STATUS_SNAPSHOT_STALE_AFTER_MINUTES", 90
        ),
        "report_stale_after_hours": _parse_positive_int_env(
            "DOTORI_STATUS_REPORT_STALE_AFTER_HOURS", 48
        ),
    }


def _build_phase1_freshness_payload(
    *,
    db_payload: dict[str, Any],
    reports_payload: dict[str, Any],
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now_value = now_utc or _now_utc()
    now_local = now_value.astimezone(OPS_TIMEZONE)
    thresholds = _status_thresholds()
    per_shop: dict[str, dict[str, Any]] = {}

    latest_ingest = db_payload.get("latest_ingest", {}) if isinstance(db_payload, dict) else {}
    report_latest = reports_payload.get("latest", {}) if isinstance(reports_payload, dict) else {}

    for shop_key in _resolve_phase1_shops().keys():
        notes: list[str] = []
        ingest_row = latest_ingest.get(shop_key, {}) if isinstance(latest_ingest, dict) else {}
        daily_latest = _parse_iso_date(
            ingest_row.get("daily_latest_date") if isinstance(ingest_row, dict) else None
        )
        if daily_latest is None:
            daily_is_stale = True
            notes.append("daily_missing")
        else:
            age_days = (now_local.date() - daily_latest).days
            daily_is_stale = age_days > int(thresholds["daily_stale_after_days"])
            if daily_is_stale:
                notes.append(f"daily_age_days={age_days}")

        snapshot_dt = _parse_iso_datetime(
            ingest_row.get("snapshot_latest_at") if isinstance(ingest_row, dict) else None
        )
        if snapshot_dt is None:
            snapshot_is_stale = True
            notes.append("snapshot_missing")
        else:
            age_minutes = int(
                (now_local - snapshot_dt.astimezone(OPS_TIMEZONE)).total_seconds() / 60
            )
            snapshot_is_stale = age_minutes > int(thresholds["snapshot_stale_after_minutes"])
            if snapshot_is_stale:
                notes.append(f"snapshot_age_minutes={age_minutes}")

        report_row = report_latest.get(shop_key, {}) if isinstance(report_latest, dict) else {}
        report_detail = {
            "daily_midday_is_stale": False,
            "daily_final_is_stale": False,
            "weekly_is_stale": False,
        }
        existing_report_stale_flags: list[bool] = []
        for kind in ("daily_midday", "daily_final", "weekly"):
            pointer = report_row.get(kind) if isinstance(report_row, dict) else None
            if isinstance(pointer, dict):
                stale_flag = bool(pointer.get("is_stale"))
                existing_report_stale_flags.append(stale_flag)
                detail_key = f"{kind}_is_stale"
                if detail_key in report_detail:
                    report_detail[detail_key] = stale_flag
            else:
                notes.append(f"report_missing_{kind}")

        reports_is_stale = any(existing_report_stale_flags)
        if reports_is_stale:
            notes.append("reports_stale=1")

        per_shop[shop_key] = {
            "daily_is_stale": bool(daily_is_stale),
            "snapshot_is_stale": bool(snapshot_is_stale),
            "reports_is_stale": bool(reports_is_stale),
            "reports_detail": report_detail,
            "notes": notes,
        }

    return {
        "thresholds": thresholds,
        "per_shop": per_shop,
    }


def _build_ads_rate_limit_config_payload() -> dict[str, Any]:
    resolved = resolve_rate_limit_state_path_info(
        rate_limit_state_path=None,
        out_dir=None,
    )
    state_path = Path(resolved.get("path") or "")
    source = str(resolved.get("source") or "fallback")
    parent = state_path.parent
    parent_exists = bool(parent.exists())
    access_writable = bool(parent_exists and os.access(parent, os.W_OK))
    writable_probe_ok = False
    writable_probe_error: str | None = None

    if parent_exists:
        try:
            with state_path.open("a", encoding="utf-8"):
                pass
            writable_probe_ok = True
        except Exception as exc:  # noqa: BLE001
            writable_probe_error = f"{type(exc).__name__}: {exc}"
    else:
        writable_probe_error = f"parent_missing={parent}"

    return {
        "state_path_effective": str(state_path),
        "state_path_source": source,
        "parent_dir": str(parent),
        "parent_dir_exists": parent_exists,
        "parent_dir_writable": bool(access_writable and writable_probe_ok),
        "os_access_writable": access_writable,
        "writable_probe_ok": writable_probe_ok,
        "writable_probe_error": writable_probe_error,
    }


def _ads_rate_limit_state_path_hint() -> str:
    return (
        "Run: sudo mkdir -p /var/lib/dotori_shopee_automation && "
        "sudo chown -R dotori:dotori /var/lib/dotori_shopee_automation && "
        "sudo chmod 775 /var/lib/dotori_shopee_automation && "
        "sudo systemctl restart dotori_shopee_automation_phase1.service"
    )


def _make_issue(*, shop: str, code: str, severity: str, hint: str) -> dict[str, str]:
    return {
        "shop": shop,
        "code": code,
        "severity": severity,
        "hint": hint,
    }


def _extract_daily_report_date_from_pointer(
    pointer: object,
    *,
    kind: str,
) -> date_type | None:
    if not isinstance(pointer, dict):
        return None
    relpath = str(pointer.get("relpath") or "").strip()
    if not relpath:
        return None
    name = Path(relpath).name
    match = re.match(r"^(\d{4}-\d{2}-\d{2})_(midday|final)\.html$", name)
    if not match:
        return None
    if match.group(2) != kind:
        return None
    try:
        return date_type.fromisoformat(match.group(1))
    except ValueError:
        return None


def _build_phase1_issues(
    *,
    token_payload: dict[str, Any],
    db_payload: dict[str, Any],
    reports_payload: dict[str, Any],
    freshness_payload: dict[str, Any],
    ads_rate_limit_config: dict[str, Any],
    now_utc: datetime,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    now_local = now_utc.astimezone(OPS_TIMEZONE)
    expected_final_date = now_local.date() - timedelta(days=1)
    expected_midday_date = (
        now_local.date()
        if (now_local.hour, now_local.minute) >= (13, 0)
        else now_local.date() - timedelta(days=1)
    )
    freshness_per_shop = (
        freshness_payload.get("per_shop", {}) if isinstance(freshness_payload, dict) else {}
    )
    report_latest = reports_payload.get("latest", {}) if isinstance(reports_payload, dict) else {}
    db_ok = bool(db_payload.get("ok")) if isinstance(db_payload, dict) else False
    parent_exists = bool(ads_rate_limit_config.get("parent_dir_exists"))
    parent_writable = bool(ads_rate_limit_config.get("parent_dir_writable"))
    state_path_effective = str(ads_rate_limit_config.get("state_path_effective") or "").strip()
    writable_probe_error = str(ads_rate_limit_config.get("writable_probe_error") or "").strip()
    state_path_hint = _ads_rate_limit_state_path_hint()

    for shop_key in _resolve_phase1_shops().keys():
        if not parent_exists:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="ADS_RATE_LIMIT_STATE_PATH_PARENT_MISSING",
                    severity="warn",
                    hint=(
                        f"rate_limit_state_path={state_path_effective} "
                        f"missing_parent. {state_path_hint}"
                    ).strip(),
                )
            )
        elif not parent_writable:
            hint_parts = [f"rate_limit_state_path={state_path_effective}", "not_writable."]
            if writable_probe_error:
                hint_parts.append(f"error={writable_probe_error}")
            hint_parts.append(state_path_hint)
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="ADS_RATE_LIMIT_STATE_PATH_NOT_WRITABLE",
                    severity="warn",
                    hint=" ".join(hint_parts).strip(),
                )
            )

        token_row = token_payload.get(shop_key, {}) if isinstance(token_payload, dict) else {}
        token_len = int(token_row.get("token_len") or 0) if isinstance(token_row, dict) else 0
        access_ttl = (
            int(token_row.get("access_expires_in_sec") or -1)
            if isinstance(token_row, dict)
            else -1
        )
        gate_state = (
            str(token_row.get("gate_state") or "unknown")
            if isinstance(token_row, dict)
            else "unknown"
        )
        if token_len <= 0:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="TOKEN_MISSING",
                    severity="error",
                    hint="Run Apps Script export + token push/sync",
                )
            )
        elif access_ttl <= 0:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="TOKEN_EXPIRED",
                    severity="error",
                    hint="Refresh token in Apps Script and push again",
                )
            )
        if gate_state == "blocked":
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="TOKEN_GATE_BLOCKED",
                    severity="warn",
                    hint="Check token preflight gate and resume token sync",
                )
            )

        freshness_row = (
            freshness_per_shop.get(shop_key, {})
            if isinstance(freshness_per_shop, dict)
            else {}
        )
        if bool(freshness_row.get("daily_is_stale")):
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="DAILY_STALE",
                    severity="warn",
                    hint="Run daily ingest/report jobs and verify fixtures/live feed",
                )
            )
        if bool(freshness_row.get("snapshot_is_stale")):
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="SNAPSHOT_STALE",
                    severity="warn",
                    hint="Run `ops phase1 alerts run-once` or check scheduler",
                )
            )
        report_row = report_latest.get(shop_key, {}) if isinstance(report_latest, dict) else {}
        for kind, code, severity, hint in (
            (
                "daily_midday",
                "REPORT_MISSING_DAILY_MIDDAY",
                "warn",
                "Run: ops phase1 schedule run-once --job daily-midday --date YYYY-MM-DD --shops samord,minmin",
            ),
            (
                "daily_final",
                "REPORT_MISSING_DAILY_FINAL",
                "warn",
                "Run: ops phase1 schedule run-once --job daily-final --date YYYY-MM-DD --shops samord,minmin",
            ),
            (
                "weekly",
                "REPORT_MISSING_WEEKLY",
                "info",
                "Run weekly job or: ops phase1 schedule run-once --job weekly --date YYYY-MM-DD --shops samord,minmin",
            ),
        ):
            pointer = report_row.get(kind) if isinstance(report_row, dict) else None
            if pointer is None:
                issues.append(
                    _make_issue(
                        shop=shop_key,
                        code=code,
                        severity=severity,
                        hint=hint,
                    )
                )
                continue
            if bool(pointer.get("is_stale")):
                stale_code = f"REPORT_STALE_{kind.upper()}"
                stale_hint = (
                    "Run report job to refresh stale output"
                    if kind != "weekly"
                    else "Run weekly job (Mon 09:00 window) or manual weekly run-once"
                )
                issues.append(
                    _make_issue(
                        shop=shop_key,
                        code=stale_code,
                        severity="info",
                        hint=stale_hint,
                    )
                )

        final_pointer = report_row.get("daily_final") if isinstance(report_row, dict) else None
        final_date = _extract_daily_report_date_from_pointer(final_pointer, kind="final")
        if final_date is not None and final_date < expected_final_date:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="REPORT_LAG_DAILY_FINAL",
                    severity="warn",
                    hint=(
                        f"latest_final={final_date.isoformat()} "
                        f"expected_at_least={expected_final_date.isoformat()}; "
                        "run daily-final scheduler/reconcile"
                    ),
                )
            )

        midday_pointer = report_row.get("daily_midday") if isinstance(report_row, dict) else None
        midday_date = _extract_daily_report_date_from_pointer(midday_pointer, kind="midday")
        if midday_date is not None and midday_date < expected_midday_date:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="REPORT_LAG_DAILY_MIDDAY",
                    severity="info",
                    hint=(
                        f"latest_midday={midday_date.isoformat()} "
                        f"expected_at_least={expected_midday_date.isoformat()}; "
                        "run daily-midday scheduler/reconcile"
                    ),
                )
            )

        if not db_ok:
            issues.append(
                _make_issue(
                    shop=shop_key,
                    code="DB_UNAVAILABLE",
                    severity="error",
                    hint="Check DATABASE_URL and DB file permissions",
                )
            )

    severity_order = {"error": 0, "warn": 1, "info": 2}
    issues.sort(
        key=lambda row: (
            str(row.get("shop") or ""),
            severity_order.get(str(row.get("severity") or "info"), 9),
            str(row.get("code") or ""),
        )
    )
    return issues


def build_phase1_status_payload(*, now_utc: datetime | None = None) -> dict[str, Any]:
    now_value = now_utc or _now_utc()
    token_bundle = _build_phase1_token_status_payload(with_meta=True)
    shops_payload = token_bundle.get("shops") if isinstance(token_bundle, dict) else {}
    shops_payload = shops_payload if isinstance(shops_payload, dict) else {}
    shop_keys = sorted(shops_payload.keys())
    db_payload = _build_phase1_db_payload()
    reports_payload = _build_phase1_reports_payload(now_utc=now_value)
    doctor_notify_payload = _build_phase1_doctor_notify_payload()
    ads_rate_limit_payload = read_ads_rate_limit_status(
        shop_keys=shop_keys,
        now_utc=now_value,
    )
    ads_rate_limit_config = _build_ads_rate_limit_config_payload()
    freshness_payload = _build_phase1_freshness_payload(
        db_payload=db_payload,
        reports_payload=reports_payload,
        now_utc=now_value,
    )
    issues = _build_phase1_issues(
        token_payload=shops_payload,
        db_payload=db_payload,
        reports_payload=reports_payload,
        freshness_payload=freshness_payload,
        ads_rate_limit_config=ads_rate_limit_config,
        now_utc=now_value,
    )
    blocked_shops = [
        shop_key
        for shop_key, row in shops_payload.items()
        if isinstance(row, dict) and str(row.get("gate_state") or "") == "blocked"
    ]
    paused = bool(blocked_shops)
    paused_reason = "TOKEN_GATE_BLOCKED" if paused else None
    needs_token_action = any(
        str(issue.get("code") or "")
        in {"TOKEN_MISSING", "TOKEN_EXPIRED", "TOKEN_GATE_BLOCKED"}
        for issue in issues
        if isinstance(issue, dict)
    )
    next_action = (
        "Run Apps Script push now: refreshAndPushPhase1TokensToServer()"
        if needs_token_action
        else None
    )
    return {
        "ok": True,
        "phase": "phase1",
        "server_time": now_value.isoformat().replace("+00:00", "Z"),
        "timezone": OPS_TIMEZONE_NAME,
        "version": __version__,
        "auth": "ok",
        "shops": shop_keys,
        "token": shops_payload,
        "token_import_last_at": token_bundle.get("token_import_last_at")
        if isinstance(token_bundle, dict)
        else None,
        "token_import_last_request_id": token_bundle.get("token_import_last_request_id")
        if isinstance(token_bundle, dict)
        else None,
        "token_mode": token_bundle.get("token_mode") if isinstance(token_bundle, dict) else None,
        "token_gate": {
            "paused": paused,
            "paused_reason": paused_reason,
            "blocked_shops": blocked_shops,
        },
        "paused": paused,
        "paused_reason": paused_reason,
        "next_action": next_action,
        "schedule": _build_phase1_schedule_payload(now_utc=now_value),
        "db": db_payload,
        "reports": reports_payload,
        "capabilities": {
            "breakdown_scope": BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY,
            "gms_group_scope": GMS_GROUP_SCOPE_AGGREGATE_ONLY,
            "note": BREAKDOWN_SCOPE_NOTE,
        },
        "ads_rate_limit": ads_rate_limit_payload,
        "ads_rate_limit_config": ads_rate_limit_config,
        "doctor_notify": doctor_notify_payload,
        "freshness": freshness_payload,
        "issues": issues,
    }


@app.get("/ops/phase1/token/ping")
def ops_phase1_token_ping(request: Request) -> dict[str, Any]:
    _require_ops_auth(request)
    shops = list(_resolve_phase1_shops().keys())
    return {
        "ok": True,
        "phase": "phase1",
        "shops": shops,
        "server_time": _now_utc().isoformat().replace("+00:00", "Z"),
        "version": __version__,
        "auth": "ok",
    }


@app.get("/ops/phase1/token/status")
def ops_phase1_token_status(request: Request) -> dict[str, Any]:
    _require_ops_auth(request)
    token_bundle = _build_phase1_token_status_payload(with_meta=True)
    shops_payload = token_bundle.get("shops") if isinstance(token_bundle, dict) else {}
    shops_payload = shops_payload if isinstance(shops_payload, dict) else {}
    return {
        "ok": True,
        "phase": "phase1",
        "server_time": _now_utc().isoformat().replace("+00:00", "Z"),
        "shops": shops_payload,
        "token_import_last_at": token_bundle.get("token_import_last_at")
        if isinstance(token_bundle, dict)
        else None,
        "token_import_last_request_id": token_bundle.get("token_import_last_request_id")
        if isinstance(token_bundle, dict)
        else None,
        "token_mode": token_bundle.get("token_mode") if isinstance(token_bundle, dict) else None,
    }


@app.get("/ops/phase1/status")
def ops_phase1_status(request: Request) -> dict[str, Any]:
    _require_ops_auth(request)
    return build_phase1_status_payload()


def _parse_epoch_seconds(raw_value: object, *, field_name: str) -> int:
    try:
        parsed = int(raw_value)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid {field_name}: expected integer epoch seconds",
        ) from exc
    if parsed <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"invalid {field_name}: expected positive integer epoch seconds",
        )
    if parsed >= 1_000_000_000_000:
        parsed = int(parsed / 1000)
    return parsed


def _to_epoch_seconds_or_none(value: datetime | None) -> int | None:
    if value is None:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def _parse_nonnegative_int(value: object, *, field_name: str, shop_id: str) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid payload for shop_id={shop_id}: {field_name} must be integer",
        ) from exc
    if parsed < 0:
        raise HTTPException(
            status_code=400,
            detail=(
                f"invalid payload for shop_id={shop_id}: "
                f"{field_name} must be non-negative integer"
            ),
        )
    return parsed


def _resolve_access_expire_timestamp(
    *,
    payload: dict[str, Any],
    shop_id: str,
) -> int | None:
    for key in (
        "expire_timestamp",
        "access_expire_timestamp",
        "access_token_expire_timestamp",
        "access_expires_at",
    ):
        if key in payload and payload.get(key) is not None:
            return _parse_epoch_seconds(payload.get(key), field_name=key)
    if "expires_in" in payload and payload.get("expires_in") is not None:
        expires_in = _parse_nonnegative_int(
            payload.get("expires_in"),
            field_name="expires_in",
            shop_id=shop_id,
        )
        return int(_now_utc().timestamp()) + expires_in
    return None


def _validate_token_row(
    *,
    shop_id: str,
    payload: object,
) -> tuple[str, int | None, int]:
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400, detail=f"invalid payload for shop_id={shop_id}: expected object"
        )

    access = payload.get("access_token")
    if not isinstance(access, str) or not access.strip():
        raise HTTPException(
            status_code=400,
            detail=f"invalid payload for shop_id={shop_id}: access_token must be non-empty string",
        )
    access_expire_timestamp = _resolve_access_expire_timestamp(
        payload=payload,
        shop_id=shop_id,
    )
    discarded_refresh_tokens = 1 if bool(str(payload.get("refresh_token") or "").strip()) else 0
    return access.strip(), access_expire_timestamp, discarded_refresh_tokens


def _resolve_gate_target_shops() -> list[SimpleNamespace]:
    phase1_shops = _resolve_phase1_shops()
    rows: list[SimpleNamespace] = []
    for shop_key, meta in phase1_shops.items():
        rows.append(
            SimpleNamespace(
                shop_key=shop_key,
                label=str(meta.get("label") or shop_key.upper()),
                shopee_shop_id=meta.get("shop_id"),
            )
        )
    return rows


def _maybe_resume_after_token_import(
    *,
    now_utc: datetime,
) -> dict[str, Any]:
    target_shops = _resolve_gate_target_shops()
    if not target_shops:
        return {
            "checked": False,
            "ok": False,
            "blocked_before": False,
            "blocked_after": True,
            "auto_resumed": False,
            "rows": [],
        }

    before_snapshot = load_token_preflight_gate_status_snapshot(shops=target_shops)
    blocked_before = any(
        str((before_snapshot.get(shop.shop_key) or {}).get("gate_state") or "") == "blocked"
        for shop in target_shops
    )
    min_access_ttl_sec = _parse_positive_int_env("DOTORI_MIN_ACCESS_TTL_SEC", 120)
    gate_result = evaluate_token_preflight_gate(
        shops=target_shops,
        min_access_ttl_sec=min_access_ttl_sec,
        now_utc=now_utc,
    )
    resolved_cooldown_sec = _parse_positive_int_env(
        "DOTORI_TOKEN_RESOLVED_COOLDOWN_SEC",
        3600,
    )
    resolved_result = emit_token_resolved_alerts_with_cooldown(
        shops=target_shops,
        gate_result=gate_result,
        cooldown_sec=resolved_cooldown_sec,
        send_discord=False,
        now_utc=now_utc,
    )
    blocked_after = not bool(gate_result.get("ok"))
    auto_resumed = bool(blocked_before and not blocked_after)
    return {
        "checked": True,
        "ok": bool(gate_result.get("ok")),
        "reason": str(gate_result.get("reason") or "UNKNOWN"),
        "blocked_before": blocked_before,
        "blocked_after": blocked_after,
        "auto_resumed": auto_resumed,
        "run_soon_marker_enqueued": auto_resumed,
        "min_access_ttl_sec": min_access_ttl_sec,
        "resolved_emitted": bool(resolved_result.get("resolved_emitted")),
        "rows": gate_result.get("rows") if isinstance(gate_result.get("rows"), list) else [],
    }


def _emit_token_import_failure_alert(
    *,
    reason: str,
    request_id: str,
    source: str = "appsscript_push",
) -> None:
    summary = str(reason or "token_import_failed").strip()
    if len(summary) > 200:
        summary = summary[:197].rstrip() + "..."
    try:
        dispatch_alert_card(
            title="CẢNH BÁO CRITICAL - Token push thất bại",
            severity="CRITICAL",
            event_code="TOKEN_PUSH_FAILED",
            detail_lines=[
                f"Request ID: {request_id or '-'}",
                f"Nguồn: {source}",
                f"Lỗi: {summary}",
            ],
            action_line="Kiểm tra /ops/phase1/token/import và server logs.",
            dedup_key=f"token_push_failed:{summary}",
            cooldown_sec=900,
            send_discord=True,
            shop_label="OPS",
            webhook_url=get_settings().discord_webhook_alerts_url,
            meta={"request_id": request_id, "source": source, "reason": summary},
        )
    except Exception:
        # Keep token import endpoint behavior deterministic even if alert dispatch fails.
        return


@app.post("/ops/phase1/token/import")
def ops_phase1_token_import(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    _require_ops_auth(request)

    token_map = _extract_appsscript_token_map(payload)
    if not token_map:
        raise HTTPException(status_code=400, detail="invalid payload: token map is empty")

    token_mode_raw = payload.get("token_mode")
    token_mode = str(token_mode_raw).strip() if token_mode_raw is not None else ""
    if not token_mode:
        token_mode = "legacy"
    source_raw = payload.get("source")
    source = str(source_raw).strip() if source_raw is not None else "push"
    pushed_at_raw = payload.get("pushed_at")
    pushed_at: int | None = None
    if pushed_at_raw is not None:
        pushed_at = _parse_epoch_seconds(pushed_at_raw, field_name="pushed_at")
    request_id = str(payload.get("request_id") or request.headers.get("X-Request-ID") or "").strip()
    if not request_id:
        request_id = uuid.uuid4().hex
    imported_at_utc = _now_utc()
    imported_at_utc_iso = imported_at_utc.isoformat().replace("+00:00", "Z")

    phase1_shop_id_map = _resolve_phase1_shop_id_map()
    if not phase1_shop_id_map:
        _emit_token_import_failure_alert(
            reason="phase1 shop mapping is empty",
            request_id=request_id,
            source=source,
        )
        raise HTTPException(status_code=500, detail="phase1 shop mapping is empty")

    ignored_shop_ids: list[str] = []
    processed_shops: list[str] = []
    token_fingerprints: dict[str, dict[str, object]] = {}
    token_sha8: dict[str, str] = {}
    discarded_refresh_tokens = 0

    init_db()
    session = SessionLocal()
    try:
        imported_total = 0
        noop_total = 0
        for shop_id, token_payload in token_map.items():
            shop_key = phase1_shop_id_map.get(str(shop_id))
            if shop_key is None:
                ignored_shop_ids.append(str(shop_id))
                continue

            access, access_expire_ts, discarded_refresh = _validate_token_row(
                shop_id=str(shop_id), payload=token_payload
            )
            discarded_refresh_tokens += int(discarded_refresh)
            existing = get_token(session, shop_key=shop_key)
            is_noop = False
            if existing is not None:
                existing_access_expire_ts = _to_epoch_seconds_or_none(existing.access_token_expires_at)
                existing_expire_matches = (
                    existing_access_expire_ts == access_expire_ts
                    if access_expire_ts is not None
                    else existing_access_expire_ts is None
                )
                is_noop = (
                    existing.access_token == access
                    and existing_expire_matches
                    and not str(existing.refresh_token or "").strip()
                    and existing.refresh_token_expires_at is None
                )
            if is_noop:
                noop_total += 1
            else:
                upsert_token(
                    session,
                    shop_key=shop_key,
                    shop_id=int(shop_id),
                    access_token=access,
                    refresh_token="",
                    access_token_expires_at=(
                        datetime.fromtimestamp(access_expire_ts, tz=timezone.utc)
                        if access_expire_ts is not None
                        else None
                    ),
                    refresh_token_expires_at=None,
                )
                imported_total += 1
            processed_shops.append(shop_key)
            sha8 = _sha256_8(access)
            token_sha8[shop_key] = sha8
            token_fingerprints[shop_key] = {
                "token_len": len(access),
                "token_sha8": sha8,
            }
            access_expires_at_iso = (
                datetime.fromtimestamp(access_expire_ts, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
                if access_expire_ts is not None
                else None
            )
            access_expires_in_sec = (
                int(access_expire_ts - int(imported_at_utc.timestamp()))
                if access_expire_ts is not None
                else -1
            )
            session.add(
                EventLog(
                    level="INFO",
                    message=_TOKEN_IMPORT_EVENT_MESSAGE,
                    meta_json=json.dumps(
                        {
                            "shop_key": shop_key,
                            "shop_id": int(shop_id),
                            "source": source,
                            "token_mode": token_mode,
                            "request_id": request_id,
                            "imported_at_utc": imported_at_utc_iso,
                            "token_len": len(access),
                            "token_sha8": sha8,
                            "imported": 0 if is_noop else 1,
                            "noop": 1 if is_noop else 0,
                            "discarded_refresh_token": int(discarded_refresh),
                            "access_expires_at": access_expires_at_iso,
                            "access_expires_in_sec": access_expires_in_sec,
                        },
                        ensure_ascii=True,
                    ),
                )
            )
        session.commit()
    except HTTPException:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        _emit_token_import_failure_alert(
            reason=str(exc),
            request_id=request_id,
            source=source,
        )
        raise HTTPException(status_code=500, detail="token_import_failed") from exc
    finally:
        session.close()

    resume_result: dict[str, Any]
    try:
        resume_result = _maybe_resume_after_token_import(now_utc=imported_at_utc)
    except Exception:
        resume_result = {
            "checked": False,
            "ok": False,
            "reason": "GATE_RECHECK_FAILED",
            "blocked_before": False,
            "blocked_after": True,
            "auto_resumed": False,
            "run_soon_marker_enqueued": False,
            "rows": [],
        }

    init_db()
    summary_session = SessionLocal()
    try:
        summary_session.add(
            EventLog(
                level="INFO",
                message=_TOKEN_IMPORT_SUMMARY_MESSAGE,
                meta_json=json.dumps(
                    {
                        "source": source,
                        "token_mode": token_mode,
                        "request_id": request_id,
                        "imported_at_utc": imported_at_utc_iso,
                        "imported_total": imported_total,
                        "noop_total": noop_total,
                        "discarded_refresh_tokens": discarded_refresh_tokens,
                        "updated_shops": processed_shops,
                        "ignored_shop_ids": ignored_shop_ids,
                        "auto_resume": resume_result,
                    },
                    ensure_ascii=True,
                ),
            )
        )
        summary_session.commit()
    finally:
        summary_session.close()

    return {
        "ok": True,
        "request_id": request_id,
        "token_mode": token_mode,
        "source": source,
        "pushed_at": pushed_at,
        "imported_at_utc": imported_at_utc_iso,
        "imported": imported_total,
        "noop": noop_total,
        "shops": sorted(processed_shops),
        "token_sha8": token_sha8,
        "imported_total": imported_total,
        "updated_shops": processed_shops,
        "noop_total": noop_total,
        "discarded_refresh_tokens": discarded_refresh_tokens,
        "ignored_shop_ids": ignored_shop_ids,
        "token_fingerprints": token_fingerprints,
        "auto_resume": resume_result,
    }

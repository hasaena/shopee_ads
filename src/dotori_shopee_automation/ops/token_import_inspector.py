from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..config import get_settings, load_shops
from ..db import EventLog, SessionLocal, init_db
from ..shopee.token_store import get_token

TOKEN_IMPORT_EVENT_MESSAGE = "phase1_token_import_event"
TOKEN_IMPORT_SUMMARY_MESSAGE = "phase1_token_import_summary"
TOKEN_IMPORT_INGRESS_MESSAGE = "phase1_token_import_ingress"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
INGRESS_EVENTS_JSONL_NAME = "import_ingress_events_sanitized.jsonl"


def artifact_root() -> Path:
    root = PROJECT_ROOT / "return_and_review" / "collaboration" / "artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def ingress_jsonl_path() -> Path:
    return artifact_root() / INGRESS_EVENTS_JSONL_NAME


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return path


def append_jsonl(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(line)
        fp.write("\n")
    return path


def _sha8(value: object) -> str:
    text = str(value or "")
    if not text:
        return "-"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _to_iso(value: object) -> str | None:
    if not isinstance(value, datetime):
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:  # noqa: BLE001
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return None


def _ttl_seconds(expires_at: datetime | None, *, now_utc: datetime) -> int | None:
    if expires_at is None:
        return None
    return int(expires_at.timestamp() - now_utc.timestamp())


def _database_path(database_url: str) -> str | None:
    if not database_url.startswith("sqlite:///"):
        return None
    path_value = database_url[len("sqlite:///") :]
    if not path_value:
        return None
    return str(Path(path_value))


def _sanitize_database_url(database_url: str) -> str:
    if database_url.startswith("sqlite:///"):
        return database_url
    parsed = urlsplit(str(database_url or ""))
    if not parsed.netloc or "@" not in parsed.netloc:
        return str(database_url or "")
    left, right = parsed.netloc.rsplit("@", 1)
    if ":" in left:
        username, _password = left.split(":", 1)
        safe_left = f"{username}:***"
    else:
        safe_left = "***"
    return urlunsplit(
        (parsed.scheme, f"{safe_left}@{right}", parsed.path, parsed.query, parsed.fragment)
    )


def _resolve_shop_cfg(shop_key: str):
    for row in load_shops():
        if row.shop_key == shop_key:
            return row
    raise ValueError(f"unknown shop_key: {shop_key}")


def _resolve_shop_id(shop_cfg) -> int | None:
    settings = get_settings()
    env_key = f"SHOPEE_{shop_cfg.shop_key.upper()}_SHOP_ID"
    env_raw = str(os.environ.get(env_key, "")).strip()
    if env_raw:
        return _to_int(env_raw)
    settings_attr = f"shopee_{shop_cfg.shop_key.lower()}_shop_id"
    from_settings = getattr(settings, settings_attr, None)
    if from_settings is not None:
        return _to_int(from_settings)
    if shop_cfg.shopee_shop_id is not None:
        return _to_int(shop_cfg.shopee_shop_id)
    return None


def _decode_meta(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:  # noqa: BLE001
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def infer_import_path(
    *,
    source: str | None,
    message: str | None,
    ingress_kind: str | None = None,
) -> str:
    src = str(source or "").lower()
    msg = str(message or "").lower()
    kind = str(ingress_kind or "").lower()
    if kind == "http_endpoint":
        return "endpoint:/ops/phase1/token/import"
    if kind == "testclient":
        return "testclient"
    if kind == "fixture_loader":
        return "fixture"
    if kind == "manual_script":
        return "manual_local"
    if msg == TOKEN_IMPORT_EVENT_MESSAGE or "appsscript" in src or "gas" in src:
        return "endpoint:/ops/phase1/token/import"
    if "file" in src and "sync" in src:
        return "cli:file-sync"
    if "cli" in src and "appsscript" in src:
        return "cli:appsscript"
    if "fixture" in src:
        return "fixture"
    return "unknown"


def classify_provenance_kind(
    *,
    source: str | None,
    request_id: str | None,
    import_path: str | None,
    ingress_kind: str | None,
    user_agent_family: str | None,
    is_recent_import: bool,
) -> tuple[str, str, list[str]]:
    src = str(source or "").lower()
    req = str(request_id or "").lower()
    path = str(import_path or "").lower()
    ingress = str(ingress_kind or "").lower()
    ua = str(user_agent_family or "").lower()

    fixture_markers = ("fixture", "sample", "seed")
    test_markers = ("test", "testclient", "pytest", "task", "debug")
    manual_markers = ("manual", "local", "script")

    reasons: list[str] = []
    if any(marker in src for marker in fixture_markers) or "fixture" in path or ingress == "fixture_loader":
        reasons.append("fixture_marker_detected")
        return ("fixture", "weak", reasons)

    if (
        ingress == "testclient"
        or "testclient" in ua
        or any(marker in src for marker in test_markers)
        or any(req.startswith(prefix) for prefix in ("task", "test", "fixture"))
    ):
        reasons.append("testclient_marker_detected")
        return ("testclient", "weak", reasons)

    if ingress == "manual_script" or any(marker in src for marker in manual_markers):
        reasons.append("manual_marker_detected")
        return ("manual_local", "weak", reasons)

    gas_markers = (
        "appsscript" in src
        or "gas" in src
        or "google_apps_script" in ua
        or "apps_script" in ua
        or "googleappsscript" in ua
    )
    non_test_req = not any(marker in req for marker in ("task", "test", "fixture", "manual"))

    if ingress == "http_endpoint":
        reasons.append("http_endpoint_ingress")
        if gas_markers:
            reasons.append("gas_marker_detected")
        if non_test_req:
            reasons.append("request_id_non_test")
        if is_recent_import:
            reasons.append("recent_import")
        if gas_markers and non_test_req and is_recent_import:
            return ("endpoint_http_probable_real_live", "strong", reasons)
        if gas_markers and non_test_req:
            return ("endpoint_http_probable_gas", "medium", reasons)
        if is_recent_import and non_test_req:
            return ("endpoint_http_unknown", "medium", reasons)
        return ("endpoint_http_unknown", "weak", reasons)

    if gas_markers and non_test_req:
        reasons.append("gas_marker_without_http_ingress")
        return ("endpoint_http_probable_gas", "medium", reasons)

    reasons.append("insufficient_ingress_evidence")
    return ("endpoint_http_unknown", "weak", reasons)


def _legacy_kind_from_v2(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized in {"fixture"}:
        return "fixture"
    if normalized in {"testclient", "manual_local"}:
        return "testclient"
    if normalized in {"endpoint_http_probable_real_live", "endpoint_http_probable_gas"}:
        return "real_gas"
    return "unknown"


def _latest_import_event_meta(session, *, shop_key: str) -> dict[str, Any] | None:
    rows = (
        session.query(EventLog.id, EventLog.created_at, EventLog.message, EventLog.meta_json)
        .filter(EventLog.message.in_([TOKEN_IMPORT_EVENT_MESSAGE, TOKEN_IMPORT_SUMMARY_MESSAGE]))
        .order_by(EventLog.id.desc())
        .limit(4000)
        .all()
    )
    for row in rows:
        payload = _decode_meta(row[3])
        if str(row[2]) != TOKEN_IMPORT_EVENT_MESSAGE:
            continue
        if str(payload.get("shop_key") or "").strip() != shop_key:
            continue
        return {
            "event_id": int(row[0]),
            "created_at": _to_iso(row[1]) or str(row[1]),
            "message": str(row[2]),
            "meta": payload,
        }
    return None


def _latest_import_ingress_meta(session, *, shop_key: str) -> dict[str, Any] | None:
    rows = (
        session.query(EventLog.id, EventLog.created_at, EventLog.message, EventLog.meta_json)
        .filter(EventLog.message == TOKEN_IMPORT_INGRESS_MESSAGE)
        .order_by(EventLog.id.desc())
        .limit(4000)
        .all()
    )
    for row in rows:
        payload = _decode_meta(row[3])
        if str(payload.get("shop_key") or "").strip() != shop_key:
            continue
        return {
            "event_id": int(row[0]),
            "created_at": _to_iso(row[1]) or str(row[1]),
            "message": str(row[2]),
            "meta": payload,
        }
    return None


def list_recent_ingress_events(
    *,
    shop_key: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_db()
    session = SessionLocal()
    try:
        rows = (
            session.query(EventLog.id, EventLog.created_at, EventLog.message, EventLog.meta_json)
            .filter(EventLog.message == TOKEN_IMPORT_INGRESS_MESSAGE)
            .order_by(EventLog.id.desc())
            .limit(max(1, int(limit)))
            .all()
        )
    finally:
        session.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        meta = _decode_meta(row[3])
        if shop_key and str(meta.get("shop_key") or "").strip() != shop_key:
            continue
        item = {
            "event_id": int(row[0]),
            "created_at": _to_iso(row[1]) or str(row[1]),
            "message": str(row[2]),
            "meta": meta,
        }
        out.append(item)
    return out


def build_recent_ingress_summary(
    *,
    shop_key: str,
    since_seconds: int = 86400,
    max_rows: int = 500,
    max_recent_seconds: int = 300,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    now = now_utc or datetime.now(timezone.utc)
    rows = list_recent_ingress_events(shop_key=shop_key, limit=max_rows)
    since = max(1, int(since_seconds))
    window_start = now.timestamp() - since
    filtered: list[dict[str, Any]] = []
    for row in rows:
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        created_at_dt = _parse_dt(row.get("created_at"))
        server_received_at_dt = _parse_dt(meta.get("server_received_at")) or created_at_dt
        if server_received_at_dt is None:
            continue
        if server_received_at_dt.timestamp() < window_start:
            continue
        ingress_kind = str(meta.get("ingress_kind") or "").strip().lower() or None
        source = str(meta.get("payload_source") or "").strip() or None
        request_id = str(meta.get("request_id") or "").strip() or None
        ua_family = str(meta.get("user_agent_family") or "").strip().lower() or None
        age_seconds = int(now.timestamp() - server_received_at_dt.timestamp())
        is_recent = age_seconds <= int(max_recent_seconds)
        import_path = infer_import_path(
            source=source,
            message=TOKEN_IMPORT_INGRESS_MESSAGE,
            ingress_kind=ingress_kind,
        )
        provenance_kind, provenance_confidence, provenance_reasons = classify_provenance_kind(
            source=source,
            request_id=request_id,
            import_path=import_path,
            ingress_kind=ingress_kind,
            user_agent_family=ua_family,
            is_recent_import=is_recent,
        )
        filtered.append(
            {
                "event_id": _to_int(row.get("event_id")),
                "server_received_at": _to_iso(server_received_at_dt),
                "event_age_seconds": age_seconds,
                "ingress_kind": ingress_kind,
                "request_path": str(meta.get("request_path") or "").strip() or None,
                "method": str(meta.get("method") or "").strip() or None,
                "request_id": request_id,
                "server_generated_request_id": str(meta.get("server_generated_request_id") or "").strip() or None,
                "user_agent_family": ua_family,
                "user_agent_sha8": str(meta.get("user_agent_sha8") or "").strip() or None,
                "remote_addr_hash": str(meta.get("remote_addr_hash") or "").strip() or None,
                "content_length": _to_int(meta.get("content_length")),
                "shop_key": str(meta.get("shop_key") or "").strip() or None,
                "shop_id": _to_int(meta.get("shop_id")),
                "token_sha8": str(meta.get("token_sha8") or "").strip() or None,
                "expires_at": str(meta.get("expires_at") or "").strip() or None,
                "has_refresh_token": _to_int(meta.get("has_refresh_token")),
                "payload_source": source,
                "payload_token_mode": str(meta.get("payload_token_mode") or "").strip() or None,
                "import_path": import_path,
                "provenance_kind": provenance_kind,
                "provenance_confidence": provenance_confidence,
                "provenance_reasons": provenance_reasons,
                "database_url": _sanitize_database_url(str(settings.database_url)),
                "db_path": _database_path(str(settings.database_url)),
            }
        )
    filtered.sort(
        key=lambda row: str(row.get("server_received_at") or ""),
        reverse=True,
    )
    endpoint_hits = [
        row
        for row in filtered
        if str(row.get("ingress_kind") or "") == "http_endpoint"
    ]
    probable_rows = [
        row
        for row in filtered
        if str(row.get("provenance_kind") or "") in {"endpoint_http_probable_real_live", "endpoint_http_probable_gas"}
    ]
    return {
        "generated_at_utc": now.isoformat(),
        "shop_key": shop_key,
        "since_seconds": since,
        "window_start_utc": datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat(),
        "database_url": _sanitize_database_url(str(settings.database_url)),
        "db_path": _database_path(str(settings.database_url)),
        "total_events": len(filtered),
        "endpoint_hit_count": len(endpoint_hits),
        "probable_gas_or_live_count": len(probable_rows),
        "events": filtered,
    }


def write_ingress_events_jsonl_snapshot(
    *,
    shop_keys: list[str] | None = None,
    limit: int = 500,
) -> Path:
    selected = {str(key).strip() for key in (shop_keys or []) if str(key).strip()}
    rows = list_recent_ingress_events(shop_key=None, limit=limit)
    filtered: list[dict[str, Any]] = []
    for row in reversed(rows):
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        current_shop = str(meta.get("shop_key") or "").strip()
        if selected and current_shop not in selected:
            continue
        filtered.append(meta)
    path = ingress_jsonl_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not filtered:
        path.write_text("", encoding="utf-8")
        return path
    with path.open("w", encoding="utf-8") as fp:
        for payload in filtered:
            fp.write(json.dumps(payload, ensure_ascii=True))
            fp.write("\n")
    return path


def inspect_latest_ingress(*, shop_key: str) -> dict[str, Any]:
    settings = get_settings()
    init_db()
    session = SessionLocal()
    try:
        ingress = _latest_import_ingress_meta(session, shop_key=shop_key)
    finally:
        session.close()
    if ingress is None:
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "shop_key": shop_key,
            "database_url": _sanitize_database_url(str(settings.database_url)),
            "event_found": 0,
            "ingress": None,
        }
    meta = ingress.get("meta") if isinstance(ingress.get("meta"), dict) else {}
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "shop_key": shop_key,
        "database_url": _sanitize_database_url(str(settings.database_url)),
        "event_found": 1,
        "event_id": ingress.get("event_id"),
        "created_at": ingress.get("created_at"),
        "ingress": meta,
    }


def inspect_latest_import(
    *,
    shop_key: str,
    max_recent_seconds: int = 300,
    suspicious_ttl_seconds: int = 86400,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = now_utc or datetime.now(timezone.utc)
    settings = get_settings()
    shop_cfg = _resolve_shop_cfg(shop_key)
    resolved_shop_id = _resolve_shop_id(shop_cfg)

    init_db()
    session = SessionLocal()
    try:
        token = get_token(session, shop_key)
        event = _latest_import_event_meta(session, shop_key=shop_key)
        ingress_event = _latest_import_ingress_meta(session, shop_key=shop_key)
    finally:
        session.close()

    access = str(getattr(token, "access_token", "") or "") if token is not None else ""
    refresh = str(getattr(token, "refresh_token", "") or "") if token is not None else ""
    expires_at_dt = _parse_dt(getattr(token, "access_token_expires_at", None) if token is not None else None)
    updated_at_dt = _parse_dt(getattr(token, "updated_at", None) if token is not None else None)

    source = None
    request_id = None
    token_mode = None
    imported_at_dt: datetime | None = None
    expires_at_at_import: str | None = None
    ttl_seconds_at_import: int | None = None
    import_shop_id: int | None = None
    message_name = None
    import_event_id = None
    if event:
        meta = event.get("meta") if isinstance(event.get("meta"), dict) else {}
        source = str(meta.get("source") or "").strip() or None
        request_id = str(meta.get("request_id") or "").strip() or None
        token_mode = str(meta.get("token_mode") or "").strip() or None
        imported_at_dt = _parse_dt(meta.get("imported_at_utc")) or _parse_dt(event.get("created_at"))
        expires_at_at_import = str(meta.get("access_expires_at") or "").strip() or None
        ttl_seconds_at_import = _to_int(meta.get("access_expires_in_sec"))
        import_shop_id = _to_int(meta.get("shop_id"))
        message_name = str(event.get("message") or "")
        import_event_id = _to_int(event.get("event_id"))

    ingress_meta: dict[str, Any] = {}
    ingress_event_id: int | None = None
    if ingress_event:
        ingress_meta = ingress_event.get("meta") if isinstance(ingress_event.get("meta"), dict) else {}
        ingress_event_id = _to_int(ingress_event.get("event_id"))
    server_received_at_dt = _parse_dt(ingress_meta.get("server_received_at")) if ingress_meta else None
    if imported_at_dt is None and server_received_at_dt is not None:
        imported_at_dt = server_received_at_dt
    if imported_at_dt is None:
        imported_at_dt = updated_at_dt

    if not source:
        source = str(ingress_meta.get("payload_source") or "").strip() or None
    if not request_id:
        request_id = str(ingress_meta.get("request_id") or "").strip() or None
    if not token_mode:
        token_mode = str(ingress_meta.get("payload_token_mode") or "").strip() or None

    if ttl_seconds_at_import is None and imported_at_dt is not None and expires_at_at_import:
        exp_import_dt = _parse_dt(expires_at_at_import)
        if exp_import_dt is not None:
            ttl_seconds_at_import = int(exp_import_dt.timestamp() - imported_at_dt.timestamp())

    import_age_seconds: int | None = None
    if imported_at_dt is not None:
        import_age_seconds = int(now.timestamp() - imported_at_dt.timestamp())
    is_recent_import = bool(import_age_seconds is not None and import_age_seconds <= int(max_recent_seconds))

    ttl_now = _ttl_seconds(expires_at_dt, now_utc=now)
    expires_at_iso = _to_iso(expires_at_dt)
    suspicious_reasons: list[str] = []
    if expires_at_dt is not None and expires_at_dt.year >= 2030:
        suspicious_reasons.append("expiry_year_2030_or_later")
    if ttl_now is None:
        suspicious_reasons.append("missing_expires_at")
    else:
        if ttl_now <= 0:
            suspicious_reasons.append("ttl_non_positive")
        if ttl_now > int(suspicious_ttl_seconds):
            suspicious_reasons.append("ttl_over_threshold")
        if ttl_now > 7 * 24 * 3600:
            suspicious_reasons.append("ttl_over_7d")

    ingress_kind = str(ingress_meta.get("ingress_kind") or "").strip().lower() or None
    user_agent_family = str(ingress_meta.get("user_agent_family") or "").strip().lower() or None
    import_path = infer_import_path(
        source=source,
        message=message_name,
        ingress_kind=ingress_kind,
    )
    provenance_kind, provenance_confidence, provenance_reasons = classify_provenance_kind(
        source=source,
        request_id=request_id,
        import_path=import_path,
        ingress_kind=ingress_kind,
        user_agent_family=user_agent_family,
        is_recent_import=is_recent_import,
    )

    sanitized_db_url = _sanitize_database_url(str(settings.database_url))
    return {
        "generated_at_utc": now.isoformat(),
        "shop_key": shop_key,
        "shop_label": shop_cfg.label,
        "resolved_shop_id": resolved_shop_id,
        "database_url": sanitized_db_url,
        "db_path": _database_path(str(settings.database_url)),
        "import_event_id": import_event_id,
        "ingress_event_id": ingress_event_id,
        "imported_at": _to_iso(imported_at_dt),
        "import_age_seconds": import_age_seconds,
        "is_recent_import": int(is_recent_import),
        "source": source,
        "request_id": request_id,
        "token_mode": token_mode,
        "import_path": import_path,
        "provenance_kind": provenance_kind,
        "provenance_confidence": provenance_confidence,
        "provenance_reasons": provenance_reasons,
        "provenance_kind_legacy": _legacy_kind_from_v2(provenance_kind),
        "import_shop_id": import_shop_id,
        "token_sha8": _sha8(access),
        "token_len": len(access),
        "has_refresh_token": int(bool(refresh)),
        "expires_at": expires_at_iso,
        "ttl_seconds_now": ttl_now,
        "ttl_seconds_at_import": ttl_seconds_at_import,
        "expires_at_at_import": expires_at_at_import,
        "updated_at": _to_iso(updated_at_dt),
        "suspicious_expiry": int(bool(suspicious_reasons)),
        "suspicious_reasons": suspicious_reasons,
        "server_received_at": _to_iso(server_received_at_dt),
        "server_generated_request_id": str(ingress_meta.get("server_generated_request_id") or "").strip() or None,
        "request_path": str(ingress_meta.get("request_path") or "").strip() or None,
        "method": str(ingress_meta.get("method") or "").strip() or None,
        "user_agent_family": user_agent_family,
        "user_agent_sha8": str(ingress_meta.get("user_agent_sha8") or "").strip() or None,
        "remote_addr_hash": str(ingress_meta.get("remote_addr_hash") or "").strip() or None,
        "content_length": _to_int(ingress_meta.get("content_length")),
        "payload_source": str(ingress_meta.get("payload_source") or "").strip() or None,
        "payload_token_mode": str(ingress_meta.get("payload_token_mode") or "").strip() or None,
        "ingress_kind": ingress_kind,
    }


def build_sanity_warnings(
    *,
    inspection: dict[str, Any],
    suspicious_ttl_seconds: int = 86400,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def _add(code: str, message: str, value: object) -> None:
        out.append({"code": code, "severity": "warn", "message": message, "value": value})

    if not inspection.get("imported_at"):
        _add("missing_imported_at", "latest import timestamp missing", inspection.get("imported_at"))
    if not inspection.get("source"):
        _add("missing_source", "import source missing", inspection.get("source"))
    if not inspection.get("request_id"):
        _add("missing_request_id", "import request_id missing", inspection.get("request_id"))
    if not inspection.get("token_mode"):
        _add("missing_token_mode", "import token_mode missing", inspection.get("token_mode"))
    if inspection.get("resolved_shop_id") in (None, 0):
        _add("missing_shop_id", "resolved shop_id missing", inspection.get("resolved_shop_id"))

    ttl_now = _to_int(inspection.get("ttl_seconds_now"))
    if ttl_now is None:
        _add("missing_ttl", "token ttl cannot be computed", inspection.get("ttl_seconds_now"))
    else:
        if ttl_now <= 0:
            _add("ttl_non_positive", "token appears expired/non-positive ttl", ttl_now)
        if ttl_now > int(suspicious_ttl_seconds):
            _add("ttl_over_threshold", "token ttl larger than expected threshold", ttl_now)

    if int(inspection.get("has_refresh_token") or 0) == 1:
        _add("has_refresh_token", "access-only flow should normally have empty refresh token", 1)

    ingress_kind = str(inspection.get("ingress_kind") or "").strip().lower()
    if ingress_kind == "http_endpoint" and not inspection.get("source"):
        _add(
            "missing_source_payload_but_http_ingress_present",
            "payload source missing but HTTP ingress metadata exists",
            ingress_kind,
        )
    if (
        ingress_kind == "http_endpoint"
        and not inspection.get("request_id")
        and inspection.get("server_generated_request_id")
    ):
        _add(
            "missing_request_id_payload_but_server_request_id_generated",
            "payload request_id missing but server-generated request id exists",
            inspection.get("server_generated_request_id"),
        )
    if int(inspection.get("has_refresh_token") or 0) == 1:
        mode = str(inspection.get("token_mode") or inspection.get("payload_token_mode") or "").strip().lower()
        if mode in {"", "legacy", "access_only"}:
            _add(
                "unexpected_refresh_token_for_access_only_pipeline",
                "refresh token present in access-only oriented pipeline",
                mode or "missing",
            )
    db_path = str(inspection.get("db_path") or "").lower()
    if ttl_now is not None and ttl_now <= 0 and "phase1_live.db" in db_path:
        _add(
            "expired_latest_row_in_live_db",
            "latest token row in live db appears expired",
            ttl_now,
        )
    if (
        ingress_kind == "http_endpoint"
        and int(inspection.get("is_recent_import") or 0) == 1
        and str(inspection.get("provenance_confidence") or "").strip().lower() == "weak"
    ):
        _add(
            "endpoint_import_recent_but_provenance_weak",
            "endpoint import is recent but provenance confidence is weak",
            inspection.get("provenance_kind"),
        )

    kind = str(inspection.get("provenance_kind") or "endpoint_http_unknown")
    if kind in {"testclient", "fixture", "manual_local", "endpoint_http_unknown"}:
        _add(
            "provenance_not_probable_live",
            "latest import is not classified as probable real live ingress",
            kind,
        )
    for reason in inspection.get("suspicious_reasons") or []:
        _add(f"suspicious_{reason}", "suspicious expiry signal detected", reason)
    return out


def build_provenance_payload(*, inspection: dict[str, Any]) -> dict[str, Any]:
    kind = str(inspection.get("provenance_kind") or "endpoint_http_unknown")
    confidence = str(inspection.get("provenance_confidence") or "weak")
    source = str(inspection.get("source") or "")
    request_id = str(inspection.get("request_id") or "")
    ingress_kind = str(inspection.get("ingress_kind") or "")
    ua_family = str(inspection.get("user_agent_family") or "")
    criteria: list[dict[str, Any]] = []
    criteria.append(
        {
            "name": "ingress_is_http_endpoint",
            "matched": int(ingress_kind == "http_endpoint"),
            "evidence": ingress_kind,
        }
    )
    criteria.append(
        {
            "name": "source_or_ua_has_gas_marker",
            "matched": int(
                ("appsscript" in source.lower())
                or ("gas" in source.lower())
                or ("google_apps_script" in ua_family.lower())
            ),
            "evidence": {"source": source, "user_agent_family": ua_family},
        }
    )
    criteria.append(
        {
            "name": "request_id_non_test_prefix",
            "matched": int(not request_id.lower().startswith(("task", "test", "fixture", "manual"))),
            "evidence": request_id,
        }
    )
    criteria.append(
        {
            "name": "import_is_recent",
            "matched": int(bool(inspection.get("is_recent_import"))),
            "evidence": inspection.get("import_age_seconds"),
        }
    )
    matched_count = sum(int(row.get("matched") or 0) for row in criteria)
    return {
        "generated_at_utc": inspection.get("generated_at_utc"),
        "shop_key": inspection.get("shop_key"),
        "provenance_kind": kind,
        "provenance_confidence": confidence,
        "criteria": criteria,
        "matched_criteria_count": matched_count,
        "classification_note": (
            "kind/confidence is inferred from server-side ingress metadata + request/source markers + recency."
        ),
        "inspection_ref": inspection,
    }


def build_latest_imports_snapshot(
    *,
    shop_keys: list[str],
    max_recent_seconds: int = 300,
    suspicious_ttl_seconds: int = 86400,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for shop_key in shop_keys:
        rows.append(
            inspect_latest_import(
                shop_key=shop_key,
                max_recent_seconds=max_recent_seconds,
                suspicious_ttl_seconds=suspicious_ttl_seconds,
                now_utc=now,
            )
        )
    return {"generated_at_utc": now.isoformat(), "rows": rows}


def extract_import_fingerprint(inspection: dict[str, Any]) -> dict[str, Any]:
    return {
        "import_event_id": _to_int(inspection.get("import_event_id")),
        "ingress_event_id": _to_int(inspection.get("ingress_event_id")),
        "imported_at": inspection.get("imported_at"),
        "token_sha8": inspection.get("token_sha8"),
        "request_id": inspection.get("request_id"),
        "server_generated_request_id": inspection.get("server_generated_request_id"),
    }


def has_import_changed(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    keys = [
        "import_event_id",
        "ingress_event_id",
        "imported_at",
        "token_sha8",
        "request_id",
        "server_generated_request_id",
    ]
    for key in keys:
        if previous.get(key) != current.get(key):
            return True
    return False

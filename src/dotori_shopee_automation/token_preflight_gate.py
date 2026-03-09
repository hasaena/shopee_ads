from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Any

from .db import EventLog, SessionLocal, init_db
from .discord_notifier import send
from .shopee.token_store import get_token

_FAIL_VERDICTS = {"missing", "unknown", "expired", "short_ttl"}
_TOKEN_TTL_LOW_MESSAGE = "token_preflight_ttl_low"
_TOKEN_TTL_RESOLVED_MESSAGE = "token_preflight_ttl_ok_resolved"
_GATE_STATUS_MESSAGE = "token_preflight_gate_status"


def evaluate_token_preflight_gate(
    *,
    shops,
    min_access_ttl_sec: int,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = _normalize_now(now_utc)

    init_db()
    session = SessionLocal()
    rows: list[dict[str, Any]] = []
    try:
        for shop in shops:
            token = get_token(session, shop.shop_key)
            access_token = token.access_token if token else None
            access_expires_at = token.access_token_expires_at if token else None
            access_ttl = _compute_access_ttl(now, access_expires_at)
            access_expires_in_sec = int(access_ttl) if access_ttl is not None else -1
            verdict = _compute_verdict(
                access_token=access_token,
                access_expires_in_sec=access_ttl,
                min_access_ttl_sec=min_access_ttl_sec,
            )
            rows.append(
                {
                    "shop_key": shop.shop_key,
                    "shop_label": shop.label,
                    "shop_id": int(getattr(shop, "shopee_shop_id", 0) or 0),
                    "token_verdict": verdict,
                    "access_expires_in_sec": access_expires_in_sec,
                    "min_access_ttl_sec": int(min_access_ttl_sec),
                }
            )
    finally:
        session.close()

    ok = all(row["token_verdict"] == "ok" for row in rows)
    reason = "OK"
    if not ok:
        for name in ("missing", "unknown", "expired", "short_ttl"):
            if any(str(row.get("token_verdict")) == name for row in rows):
                reason = _reason_from_verdict(name)
                break

    return {
        "ok": bool(ok),
        "reason": reason,
        "checked_at_utc": now.isoformat(),
        "min_access_ttl_sec": int(min_access_ttl_sec),
        "rows": rows,
        "operator_instruction": (
            "Run Apps Script diag_TOKEN(shop_id), re-export "
            "shopee_tokens_export.json, then sync tokens."
        ),
    }


def emit_token_ttl_alerts_with_cooldown(
    *,
    shops,
    gate_result: dict[str, Any],
    cooldown_sec: int,
    send_discord: bool,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = _normalize_now(now_utc)
    now_ts = int(now.timestamp())
    cooldown = max(int(cooldown_sec), 0)

    shop_map = {str(shop.shop_key): shop for shop in shops}
    dry_run = os.environ.get("DISCORD_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    rows = gate_result.get("rows") if isinstance(gate_result, dict) else []
    valid_rows = [row for row in rows if isinstance(row, dict)]

    init_db()
    session = SessionLocal()
    emitted = 0
    suppressed = 0
    out_rows: list[dict[str, Any]] = []
    try:
        status_map = _load_gate_status_map(session=session)
        for row in valid_rows:
            shop_key = str(row.get("shop_key") or "")
            if not shop_key:
                continue
            status = _normalize_gate_status(
                status_map.get(shop_key),
                shop_key=shop_key,
                fallback_label=str(row.get("shop_label") or ""),
                fallback_shop_id=int(row.get("shop_id") or 0),
            )
            verdict = str(row.get("token_verdict") or "missing")
            access_ttl_sec = int(row.get("access_expires_in_sec") or -1)
            min_ttl_sec = int(row.get("min_access_ttl_sec") or 0)
            shop_label = str(status.get("shop_label") or row.get("shop_label") or shop_key.upper())
            shop_id = int(status.get("shop_id") or row.get("shop_id") or 0)

            status["shop_label"] = shop_label
            status["shop_id"] = shop_id
            status["last_verdict"] = verdict
            status["last_access_ttl_sec"] = access_ttl_sec
            status["min_required_ttl_sec"] = min_ttl_sec
            status["updated_at"] = now_ts
            status["gate_state"] = "blocked" if verdict in _FAIL_VERDICTS else "ok"

            if verdict in _FAIL_VERDICTS:
                cooldown_until_ts = int(status.get("cooldown_until") or -1)
                if cooldown_until_ts > now_ts:
                    suppressed += 1
                    out_rows.append(
                        {
                            "shop_key": shop_key,
                            "shop_label": shop_label,
                            "shop_id": shop_id,
                            "emitted": 0,
                            "suppressed": 1,
                            "dry_run": 0,
                            "cooldown_until_utc": _ts_to_iso(cooldown_until_ts),
                            "token_verdict": verdict,
                            "access_expires_in_sec": access_ttl_sec,
                            "min_access_ttl_sec": min_ttl_sec,
                        }
                    )
                else:
                    message = _build_token_alert_message(row=row)
                    if send_discord:
                        shop_cfg = shop_map.get(shop_key)
                        webhook_url = (
                            getattr(shop_cfg, "discord_webhook_url", None) if shop_cfg else None
                        )
                        send(
                            "alerts",
                            message,
                            shop_label=shop_label,
                            webhook_url=webhook_url,
                        )
                    cooldown_until_ts = now_ts + cooldown
                    status["last_alert_at"] = now_ts
                    status["cooldown_until"] = cooldown_until_ts
                    session.add(
                        EventLog(
                            level="WARN",
                            message=_TOKEN_TTL_LOW_MESSAGE,
                            meta_json=_safe_json(
                                {
                                    "shop_key": shop_key,
                                    "shop_label": shop_label,
                                    "shop_id": shop_id,
                                    "token_verdict": verdict,
                                    "access_expires_in_sec": access_ttl_sec,
                                    "min_access_ttl_sec": min_ttl_sec,
                                    "cooldown_sec": cooldown,
                                    "cooldown_until": cooldown_until_ts,
                                    "cooldown_until_utc": _ts_to_iso(cooldown_until_ts),
                                    "message": message,
                                }
                            ),
                        )
                    )
                    emitted += 1
                    out_rows.append(
                        {
                            "shop_key": shop_key,
                            "shop_label": shop_label,
                            "shop_id": shop_id,
                            "emitted": 1,
                            "suppressed": 0,
                            "dry_run": 1 if dry_run else 0,
                            "cooldown_until_utc": _ts_to_iso(cooldown_until_ts),
                            "token_verdict": verdict,
                            "access_expires_in_sec": access_ttl_sec,
                            "min_access_ttl_sec": min_ttl_sec,
                        }
                    )

            session.add(
                EventLog(
                    level="WARN" if verdict in _FAIL_VERDICTS else "INFO",
                    message=_GATE_STATUS_MESSAGE,
                    meta_json=_safe_json(status),
                )
            )
        session.commit()
    finally:
        session.close()

    return {
        "cooldown_sec": cooldown,
        "send_discord": int(send_discord),
        "dry_run": int(dry_run),
        "emitted": emitted,
        "suppressed": suppressed,
        "rows": out_rows,
    }


def emit_token_resolved_alerts_with_cooldown(
    *,
    shops,
    gate_result: dict[str, Any],
    cooldown_sec: int,
    send_discord: bool,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = _normalize_now(now_utc)
    now_ts = int(now.timestamp())
    cooldown = max(int(cooldown_sec), 0)
    dry_run = os.environ.get("DISCORD_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    shop_map = {str(shop.shop_key): shop for shop in shops}

    rows = gate_result.get("rows") if isinstance(gate_result, dict) else []
    valid_rows = [row for row in rows if isinstance(row, dict)]

    init_db()
    session = SessionLocal()
    emitted = 0
    suppressed = 0
    out_rows: list[dict[str, Any]] = []
    try:
        status_map = _load_gate_status_map(session=session)
        for row in valid_rows:
            shop_key = str(row.get("shop_key") or "")
            if not shop_key:
                continue
            status = _normalize_gate_status(
                status_map.get(shop_key),
                shop_key=shop_key,
                fallback_label=str(row.get("shop_label") or ""),
                fallback_shop_id=int(row.get("shop_id") or 0),
            )
            verdict = str(row.get("token_verdict") or "missing")
            access_ttl_sec = int(row.get("access_expires_in_sec") or -1)
            min_ttl_sec = int(row.get("min_access_ttl_sec") or 0)
            shop_label = str(status.get("shop_label") or row.get("shop_label") or shop_key.upper())
            shop_id = int(status.get("shop_id") or row.get("shop_id") or 0)
            previous_verdict = str(status.get("last_verdict") or "")
            was_blocked = previous_verdict in _FAIL_VERDICTS
            resolved_until_ts = int(status.get("resolved_cooldown_until") or -1)
            within_resolved_cooldown = (
                resolved_until_ts > now_ts and int(status.get("last_resolved_at") or -1) >= 0
            )
            transitioned = 1 if (was_blocked or within_resolved_cooldown) else 0

            status["shop_label"] = shop_label
            status["shop_id"] = shop_id
            status["last_verdict"] = verdict
            status["last_access_ttl_sec"] = access_ttl_sec
            status["min_required_ttl_sec"] = min_ttl_sec
            status["updated_at"] = now_ts
            status["gate_state"] = "blocked" if verdict in _FAIL_VERDICTS else "ok"

            row_out: dict[str, Any] = {
                "shop_key": shop_key,
                "shop_label": shop_label,
                "shop_id": shop_id,
                "emitted": 0,
                "suppressed": 0,
                "dry_run": 0,
                "http_status": -1,
                "token_verdict": verdict,
                "access_expires_in_sec": access_ttl_sec,
                "min_access_ttl_sec": min_ttl_sec,
                "transitioned_from_blocked": transitioned,
                "resolved_cooldown_until_utc": _ts_to_iso(resolved_until_ts),
            }
            if verdict == "ok" and transitioned == 1:
                if resolved_until_ts > now_ts:
                    suppressed += 1
                    row_out["suppressed"] = 1
                    row_out["resolved_cooldown_until_utc"] = _ts_to_iso(resolved_until_ts)
                else:
                    message = _build_token_resolved_message(row=row)
                    if send_discord:
                        shop_cfg = shop_map.get(shop_key)
                        webhook_url = (
                            getattr(shop_cfg, "discord_webhook_url", None) if shop_cfg else None
                        )
                        send(
                            "alerts",
                            message,
                            shop_label=shop_label,
                            webhook_url=webhook_url,
                        )
                    resolved_until_ts = now_ts + cooldown
                    status["last_resolved_at"] = now_ts
                    status["resolved_cooldown_until"] = resolved_until_ts
                    emitted += 1
                    row_out["emitted"] = 1
                    row_out["dry_run"] = 1 if dry_run else 0
                    row_out["http_status"] = -1 if dry_run else 200
                    row_out["resolved_cooldown_until_utc"] = _ts_to_iso(resolved_until_ts)
                    session.add(
                        EventLog(
                            level="INFO",
                            message=_TOKEN_TTL_RESOLVED_MESSAGE,
                            meta_json=_safe_json(
                                {
                                    "shop_key": shop_key,
                                    "shop_label": shop_label,
                                    "shop_id": shop_id,
                                    "token_verdict": verdict,
                                    "access_expires_in_sec": access_ttl_sec,
                                    "min_access_ttl_sec": min_ttl_sec,
                                    "cooldown_sec": cooldown,
                                    "resolved_cooldown_until": resolved_until_ts,
                                    "resolved_cooldown_until_utc": _ts_to_iso(resolved_until_ts),
                                    "message": message,
                                }
                            ),
                        )
                    )
            out_rows.append(row_out)
            session.add(
                EventLog(
                    level="INFO" if verdict == "ok" else "WARN",
                    message=_GATE_STATUS_MESSAGE,
                    meta_json=_safe_json(status),
                )
            )
        session.commit()
    finally:
        session.close()

    return {
        "cooldown_sec": cooldown,
        "send_discord": int(send_discord),
        "dry_run": int(dry_run),
        "emitted": emitted,
        "suppressed": suppressed,
        "resolved_emitted": bool(emitted > 0),
        "resolved_cooldown_skipped": bool(suppressed > 0),
        "rows": out_rows,
    }


def load_token_preflight_gate_status_snapshot(
    *,
    shops,
) -> dict[str, dict[str, Any]]:
    init_db()
    session = SessionLocal()
    out: dict[str, dict[str, Any]] = {}
    try:
        status_map = _load_gate_status_map(session=session)
        for shop in shops:
            shop_key = str(getattr(shop, "shop_key", "") or "")
            if not shop_key:
                continue
            status = _normalize_gate_status(
                status_map.get(shop_key),
                shop_key=shop_key,
                fallback_label=str(getattr(shop, "label", "") or ""),
                fallback_shop_id=int(getattr(shop, "shopee_shop_id", 0) or 0),
            )
            status["gate_state"] = _gate_state_from_verdict(str(status.get("last_verdict") or ""))
            out[shop_key] = status
    finally:
        session.close()
    return out


def write_token_preflight_gate_artifacts(
    *,
    base_dir: Path,
    gate_result: dict[str, Any],
    alert_result: dict[str, Any] | None = None,
    resolved_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_dir.mkdir(parents=True, exist_ok=True)
    resolved = resolved_result or {}
    payload = {
        "gate": gate_result,
        "alerts": alert_result or {},
        "resolved": resolved,
        "resolved_emitted": bool(
            resolved.get("resolved_emitted")
            if isinstance(resolved, dict) and "resolved_emitted" in resolved
            else int(resolved.get("emitted", 0)) > 0
            if isinstance(resolved, dict)
            else False
        ),
        "resolved_cooldown_skipped": bool(
            resolved.get("resolved_cooldown_skipped")
            if isinstance(resolved, dict) and "resolved_cooldown_skipped" in resolved
            else int(resolved.get("suppressed", 0)) > 0
            if isinstance(resolved, dict)
            else False
        ),
    }
    json_path = base_dir / "preflight_gate_summary.json"
    md_path = base_dir / "preflight_gate_summary.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    md_lines = _build_preflight_markdown(payload)
    md_path.write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
    }


def _build_preflight_markdown(payload: dict[str, Any]) -> list[str]:
    gate = payload.get("gate") if isinstance(payload, dict) else {}
    gate = gate if isinstance(gate, dict) else {}
    rows = gate.get("rows") if isinstance(gate.get("rows"), list) else []
    alerts = payload.get("alerts") if isinstance(payload, dict) else {}
    alerts = alerts if isinstance(alerts, dict) else {}
    resolved = payload.get("resolved") if isinstance(payload, dict) else {}
    resolved = resolved if isinstance(resolved, dict) else {}
    resolved_rows = resolved.get("rows") if isinstance(resolved.get("rows"), list) else []
    lines = [
        "# Token Preflight Gate Summary",
        "",
        f"- checked_at_utc: {gate.get('checked_at_utc', '-')}",
        f"- ok: {1 if bool(gate.get('ok')) else 0}",
        f"- reason: {gate.get('reason', '-')}",
        f"- min_access_ttl_sec: {gate.get('min_access_ttl_sec', '-')}",
        f"- instruction: {gate.get('operator_instruction', '-')}",
        "",
        "| shop_key | shop_label | shop_id | token_verdict | access_expires_in_sec | min_access_ttl_sec |",
        "|---|---|---:|---|---:|---:|",
    ]
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| "
            f"{row.get('shop_key', '-')} | "
            f"{row.get('shop_label', '-')} | "
            f"{row.get('shop_id', '-')} | "
            f"{row.get('token_verdict', '-')} | "
            f"{row.get('access_expires_in_sec', '-')} | "
            f"{row.get('min_access_ttl_sec', '-')} |"
        )
    lines.extend(
        [
            "",
            "## Token TTL alert cooldown",
            f"- emitted: {alerts.get('emitted', 0)}",
            f"- suppressed: {alerts.get('suppressed', 0)}",
            f"- cooldown_sec: {alerts.get('cooldown_sec', 0)}",
            f"- dry_run: {alerts.get('dry_run', 0)}",
            "",
            "## Resume / Resolved",
            f"- resolved_emitted: {1 if bool(payload.get('resolved_emitted')) else 0}",
            f"- resolved_cooldown_skipped: {1 if bool(payload.get('resolved_cooldown_skipped')) else 0}",
            f"- emitted: {resolved.get('emitted', 0)}",
            f"- suppressed: {resolved.get('suppressed', 0)}",
            f"- cooldown_sec: {resolved.get('cooldown_sec', 0)}",
            f"- dry_run: {resolved.get('dry_run', 0)}",
            "",
            "| shop_key | transitioned_from_blocked | emitted | suppressed | resolved_cooldown_until_utc |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for row in resolved_rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| "
            f"{row.get('shop_key', '-')} | "
            f"{row.get('transitioned_from_blocked', 0)} | "
            f"{row.get('emitted', 0)} | "
            f"{row.get('suppressed', 0)} | "
            f"{row.get('resolved_cooldown_until_utc', '-')} |"
        )
    return lines


def _compute_access_ttl(now_utc: datetime, access_expires_at: datetime | None) -> int | None:
    if access_expires_at is None:
        return None
    value = access_expires_at
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return int((value - now_utc).total_seconds())


def _compute_verdict(
    *,
    access_token: str | None,
    access_expires_in_sec: int | None,
    min_access_ttl_sec: int,
) -> str:
    if not access_token:
        return "missing"
    if access_expires_in_sec is None:
        return "unknown"
    if access_expires_in_sec <= 0:
        return "expired"
    if access_expires_in_sec < int(min_access_ttl_sec):
        return "short_ttl"
    return "ok"


def _reason_from_verdict(verdict: str) -> str:
    mapping = {
        "missing": "MISSING_TOKEN",
        "unknown": "UNKNOWN_ACCESS_EXPIRY",
        "expired": "ACCESS_TOKEN_EXPIRED",
        "short_ttl": "ACCESS_TTL_TOO_LOW",
    }
    return mapping.get(verdict, "UNKNOWN")


def _build_token_alert_message(*, row: dict[str, Any]) -> str:
    ttl = int(row.get("access_expires_in_sec") or -1)
    min_ttl = int(row.get("min_access_ttl_sec") or 0)
    return (
        f"TOKEN_TTL_LOW access_ttl_sec={ttl} min_required={min_ttl}. "
        "Action: re-export tokens from Apps Script and sync."
    )


def _build_token_resolved_message(*, row: dict[str, Any]) -> str:
    ttl = int(row.get("access_expires_in_sec") or -1)
    min_ttl = int(row.get("min_access_ttl_sec") or 0)
    return f"TOKEN_TTL_OK (resolved) access_ttl_sec={ttl} min_required={min_ttl}"


def _load_gate_status_map(*, session) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rows = (
        session.query(EventLog.meta_json)
        .filter(EventLog.message == _GATE_STATUS_MESSAGE)
        .order_by(EventLog.id.desc())
        .limit(2000)
        .all()
    )
    for row in rows:
        raw_meta = row[0] if isinstance(row, tuple) else getattr(row, "meta_json", None)
        if not raw_meta:
            continue
        try:
            payload = json.loads(raw_meta)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(payload, dict):
            continue
        shop_key = str(payload.get("shop_key") or "")
        if not shop_key or shop_key in out:
            continue
        out[shop_key] = payload
    return out


def _normalize_gate_status(
    payload: dict[str, Any] | None,
    *,
    shop_key: str,
    fallback_label: str,
    fallback_shop_id: int,
) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    last_verdict = str(data.get("last_verdict") or "unknown")
    cooldown_until = _to_unix_ts(data.get("cooldown_until"))
    if cooldown_until < 0:
        cooldown_until = _to_unix_ts(data.get("cooldown_until_utc"))
    resolved_cooldown_until = _to_unix_ts(data.get("resolved_cooldown_until"))
    if resolved_cooldown_until < 0:
        resolved_cooldown_until = _to_unix_ts(data.get("resolved_cooldown_until_utc"))
    status = {
        "shop_key": shop_key,
        "shop_label": str(data.get("shop_label") or fallback_label or shop_key.upper()),
        "shop_id": int(data.get("shop_id") or fallback_shop_id or 0),
        "last_verdict": last_verdict,
        "last_alert_at": _to_unix_ts(data.get("last_alert_at")),
        "last_resolved_at": _to_unix_ts(data.get("last_resolved_at")),
        "cooldown_until": cooldown_until,
        "resolved_cooldown_until": resolved_cooldown_until,
        "min_required_ttl_sec": _to_int(data.get("min_required_ttl_sec"), default=0),
        "last_access_ttl_sec": _to_int(data.get("last_access_ttl_sec"), default=-1),
        "gate_state": _gate_state_from_verdict(last_verdict),
        "updated_at": _to_unix_ts(data.get("updated_at")),
    }
    if status["last_alert_at"] < 0:
        status["last_alert_at"] = _to_unix_ts(data.get("last_alert_at_utc"))
    if status["last_resolved_at"] < 0:
        status["last_resolved_at"] = _to_unix_ts(data.get("last_resolved_at_utc"))
    return status


def _gate_state_from_verdict(verdict: str) -> str:
    value = str(verdict or "").strip().lower()
    if value == "ok":
        return "ok"
    if value in _FAIL_VERDICTS:
        return "blocked"
    return "unknown"


def _normalize_now(value: datetime | None) -> datetime:
    now = value or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _to_int(value: Any, *, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return int(stripped)
        except ValueError:
            return default
    return default


def _to_unix_ts(value: Any) -> int:
    if value is None:
        return -1
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return -1
        try:
            return int(stripped)
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return -1
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp())
    return -1


def _ts_to_iso(value: int) -> str:
    if value < 0:
        return "-"
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _safe_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, default=str)

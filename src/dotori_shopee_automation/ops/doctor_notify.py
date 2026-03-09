from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Column, String, inspect, text

from ..db import Base

_SEVERITY_ORDER = {
    "info": 0,
    "warn": 1,
    "error": 2,
}


class OpsDoctorNotifyState(Base):
    __tablename__ = "ops_doctor_notify_state"

    shop_label = Column(String(50), primary_key=True)
    last_alert_at = Column(String(40), nullable=True)
    last_alert_level = Column(String(10), nullable=True)
    last_resolved_at = Column(String(40), nullable=True)
    last_action = Column(String(20), nullable=True)
    last_sent_at = Column(String(40), nullable=True)
    cooldown_until = Column(String(40), nullable=True)
    resolved_cooldown_until = Column(String(40), nullable=True)


def ensure_doctor_notify_state_columns(engine) -> None:
    try:
        inspector = inspect(engine)
        columns = {col["name"] for col in inspector.get_columns("ops_doctor_notify_state")}
    except Exception:
        return
    statements: list[str] = []
    if "last_action" not in columns:
        statements.append(
            "ALTER TABLE ops_doctor_notify_state ADD COLUMN last_action VARCHAR(20)"
        )
    if "last_sent_at" not in columns:
        statements.append(
            "ALTER TABLE ops_doctor_notify_state ADD COLUMN last_sent_at VARCHAR(40)"
        )
    if not statements:
        return
    for stmt in statements:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:
            continue


def severity_rank(level: str) -> int:
    return _SEVERITY_ORDER.get(str(level or "").strip().lower(), -1)


def parse_min_severity(level: str) -> str:
    value = str(level or "").strip().lower()
    if value not in _SEVERITY_ORDER:
        raise ValueError("min_severity must be one of: warn,error")
    return value


def run_doctor_notify_cycle(
    *,
    session,
    shops: list[dict[str, str]],
    payload: dict[str, Any],
    min_severity: str,
    cooldown_sec: int,
    resolved_cooldown_sec: int,
    persist_state: bool = True,
    record_sent_state: bool = True,
    now_utc: datetime | None = None,
    max_issues: int = 20,
) -> list[dict[str, Any]]:
    threshold = parse_min_severity(min_severity)
    threshold_rank = severity_rank(threshold)
    now = _normalize_now(now_utc)

    issues = payload.get("issues", []) if isinstance(payload, dict) else []
    valid_issues = [row for row in issues if isinstance(row, dict)]
    by_shop: dict[str, list[dict[str, Any]]] = {}
    for row in valid_issues:
        shop_key = str(row.get("shop") or "").strip()
        if not shop_key:
            continue
        by_shop.setdefault(shop_key, []).append(row)

    decisions: list[dict[str, Any]] = []
    for shop in shops:
        shop_key = str(shop.get("shop_key") or "").strip()
        if not shop_key:
            continue
        shop_label = str(shop.get("shop_label") or shop_key.upper())
        state = _load_or_create_state(session=session, shop_label=shop_label)
        shop_issues = by_shop.get(shop_key, [])
        selected_issues = [
            row for row in shop_issues if severity_rank(str(row.get("severity") or "")) >= threshold_rank
        ]

        row_out: dict[str, Any] = {
            "shop_key": shop_key,
            "shop_label": shop_label,
            "min_severity": threshold,
            "action": "ok",
            "level": "-",
            "would_send": 0,
            "cooldown_skip": 0,
            "resolved_cooldown_skip": 0,
            "issue_count": len(selected_issues),
            "issue_codes": [],
            "message": "",
            "cooldown_until_utc": str(state.cooldown_until or "-"),
            "resolved_cooldown_until_utc": str(state.resolved_cooldown_until or "-"),
            "updated_state": 0,
        }

        if selected_issues:
            alert_level = _highest_issue_level(selected_issues)
            alert_codes = _issue_codes(selected_issues, max_items=max_issues)
            cooldown_until = _parse_utc_iso(state.cooldown_until)
            row_out["action"] = "alert"
            row_out["level"] = alert_level
            row_out["issue_codes"] = alert_codes
            row_out["message"] = (
                f"OPS_DOCTOR {alert_level.upper()} issues={len(selected_issues)} "
                f"codes={','.join(alert_codes)} min_severity={threshold}"
            )
            if cooldown_until is not None and cooldown_until > now:
                if persist_state:
                    state.last_action = "alert"
                row_out["cooldown_skip"] = 1
                row_out["cooldown_until_utc"] = _to_utc_iso(cooldown_until)
                decisions.append(row_out)
                continue

            row_out["would_send"] = 1
            if persist_state:
                state.last_action = "alert"
                state.cooldown_until = _to_utc_iso(
                    now + timedelta(seconds=max(int(cooldown_sec), 0))
                )
                if record_sent_state:
                    state.last_alert_at = _to_utc_iso(now)
                    state.last_alert_level = alert_level
                    state.last_sent_at = _to_utc_iso(now)
                row_out["updated_state"] = 1
                row_out["cooldown_until_utc"] = str(state.cooldown_until or "-")
            decisions.append(row_out)
            continue

        last_alert_at = _parse_utc_iso(state.last_alert_at)
        last_resolved_at = _parse_utc_iso(state.last_resolved_at)
        unresolved = bool(
            last_alert_at is not None
            and (last_resolved_at is None or last_resolved_at < last_alert_at)
        )
        if not unresolved:
            if persist_state:
                state.last_action = "ok"
                row_out["updated_state"] = 1
            decisions.append(row_out)
            continue

        resolved_cooldown_until = _parse_utc_iso(state.resolved_cooldown_until)
        previous_level = str(state.last_alert_level or "-").lower()
        if previous_level not in {"warn", "error"}:
            previous_level = "-"
        row_out["action"] = "resolved"
        row_out["level"] = previous_level
        row_out["message"] = (
            f"OPS_DOCTOR RESOLVED previous_level={previous_level.upper()}"
        )
        if resolved_cooldown_until is not None and resolved_cooldown_until > now:
            if persist_state:
                state.last_action = "resolved"
            row_out["resolved_cooldown_skip"] = 1
            row_out["resolved_cooldown_until_utc"] = _to_utc_iso(resolved_cooldown_until)
            decisions.append(row_out)
            continue

        row_out["would_send"] = 1
        if persist_state:
            state.last_action = "resolved"
            state.resolved_cooldown_until = _to_utc_iso(
                now + timedelta(seconds=max(int(resolved_cooldown_sec), 0))
            )
            if record_sent_state:
                state.last_resolved_at = _to_utc_iso(now)
                state.last_sent_at = _to_utc_iso(now)
            row_out["updated_state"] = 1
            row_out["resolved_cooldown_until_utc"] = str(state.resolved_cooldown_until or "-")
        decisions.append(row_out)

    decisions.sort(key=lambda row: str(row.get("shop_label") or ""))
    return decisions


def _load_or_create_state(*, session, shop_label: str) -> OpsDoctorNotifyState:
    row = (
        session.query(OpsDoctorNotifyState)
        .filter(OpsDoctorNotifyState.shop_label == shop_label)
        .one_or_none()
    )
    if row is not None:
        return row
    row = OpsDoctorNotifyState(shop_label=shop_label)
    session.add(row)
    return row


def _highest_issue_level(issues: list[dict[str, Any]]) -> str:
    ranked = sorted(
        [str(row.get("severity") or "info").lower() for row in issues],
        key=severity_rank,
        reverse=True,
    )
    return ranked[0] if ranked else "info"


def _issue_codes(issues: list[dict[str, Any]], *, max_items: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    limit = max(int(max_items), 1)
    for row in issues:
        code = str(row.get("code") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
        if len(out) >= limit:
            break
    if not out:
        out.append("NONE")
    return out


def _normalize_now(value: datetime | None) -> datetime:
    now = value or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

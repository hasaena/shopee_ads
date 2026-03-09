from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from ..db import EventLog, SessionLocal, init_db
from ..discord_notifier import send as discord_send
from .alert_state import record_sent, should_send_with_cooldown


def dispatch_alert_card(
    *,
    title: str,
    severity: str,
    event_code: str,
    detail_lines: list[str] | None = None,
    action_line: str | None = None,
    dedup_key: str,
    cooldown_sec: int,
    send_discord: bool,
    shop_label: str | None = None,
    webhook_url: str | None = None,
    now_utc: datetime | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, object]:
    now_value = now_utc or datetime.now(timezone.utc)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)
    severity_value = str(severity or "INFO").strip().upper()
    event_value = str(event_code or "UNKNOWN").strip() or "UNKNOWN"
    lines = [f"{title}"]
    lines.append(f"Sự kiện: {event_value}")
    if detail_lines:
        for row in detail_lines:
            text = str(row or "").strip()
            if text:
                lines.append(text)
    if action_line:
        lines.append(f"Hành động: {action_line}")
    message = "\n".join(lines)

    init_db()
    session = SessionLocal()
    try:
        should_send, reason = should_send_with_cooldown(
            session=session,
            dedup_key=dedup_key,
            now_utc=now_value,
            cooldown_sec=cooldown_sec,
        )
        if not send_discord:
            should_send = False
            reason = "send_disabled"

        log_meta = {
            "dedup_key": dedup_key,
            "severity": severity_value,
            "event_code": event_value,
            "title": title,
            "shop_label": shop_label,
            "cooldown_sec": int(max(cooldown_sec, 0)),
            "reason": reason,
            "meta": meta or {},
        }

        if should_send:
            discord_send(
                "alerts",
                message,
                shop_label=shop_label,
                webhook_url=webhook_url,
            )
            record_sent(
                session=session,
                dedup_key=dedup_key,
                now_utc=now_value,
                severity=severity_value,
                title=title,
                meta_json=json.dumps(log_meta, ensure_ascii=False),
            )
            session.add(
                EventLog(
                    level="WARN" if severity_value in {"WARN", "CRITICAL"} else "INFO",
                    message="phase1_alert_sent",
                    meta_json=json.dumps(log_meta, ensure_ascii=True, default=str),
                )
            )
            session.commit()
            return {
                "sent": True,
                "suppressed": False,
                "reason": "sent",
                "message": message,
            }

        session.add(
            EventLog(
                level="INFO",
                message="phase1_alert_suppressed",
                meta_json=json.dumps(log_meta, ensure_ascii=True, default=str),
            )
        )
        session.commit()
        return {
            "sent": False,
            "suppressed": True,
            "reason": reason,
            "message": message,
        }
    finally:
        session.close()

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import Column, DateTime, String, Text

from ..db import Base


class Phase1AlertState(Base):
    __tablename__ = "phase1_alert_state"

    dedup_key = Column(String(220), primary_key=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)
    last_severity = Column(String(20), nullable=True)
    last_title = Column(String(300), nullable=True)
    last_meta_json = Column(Text, nullable=True)


def should_send_with_cooldown(
    *,
    session,
    dedup_key: str,
    now_utc: datetime,
    cooldown_sec: int,
) -> tuple[bool, str]:
    key = str(dedup_key or "").strip()
    if not key:
        return True, "no_key"
    cooldown = max(int(cooldown_sec), 0)
    if cooldown <= 0:
        return True, "cooldown_disabled"

    row = (
        session.query(Phase1AlertState)
        .filter(Phase1AlertState.dedup_key == key)
        .one_or_none()
    )
    if row is None or row.last_sent_at is None:
        return True, "first_send"

    last_sent = row.last_sent_at
    now_value = now_utc
    if last_sent.tzinfo is None:
        last_sent = last_sent.replace(tzinfo=timezone.utc)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)
    elapsed = now_value - last_sent
    if elapsed >= timedelta(seconds=cooldown):
        return True, "cooldown_elapsed"
    return False, "cooldown_active"


def record_sent(
    *,
    session,
    dedup_key: str,
    now_utc: datetime,
    severity: str,
    title: str,
    meta_json: str | None = None,
) -> None:
    key = str(dedup_key or "").strip()
    if not key:
        return
    row = (
        session.query(Phase1AlertState)
        .filter(Phase1AlertState.dedup_key == key)
        .one_or_none()
    )
    if row is None:
        row = Phase1AlertState(dedup_key=key)
        session.add(row)
    row.last_sent_at = now_utc
    row.last_severity = str(severity or "").strip().upper() or "INFO"
    row.last_title = str(title or "").strip()[:300]
    row.last_meta_json = meta_json

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String, Text, Index

from ..db import Base


class AdsIncident(Base):
    __tablename__ = "ads_incident"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    incident_type = Column(String(50), nullable=False)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False)
    severity = Column(String(20), nullable=False)
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    meta_json = Column(Text, nullable=True)
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_notified_at = Column(DateTime(timezone=True), nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_ads_incident_shop_status_seen", "shop_key", "status", "last_seen_at"),
    )


def get_open_incident(
    session,
    shop_key: str,
    incident_type: str,
    entity_type: str,
    entity_id: str | None,
) -> AdsIncident | None:
    return (
        session.query(AdsIncident)
        .filter_by(
            shop_key=shop_key,
            incident_type=incident_type,
            entity_type=entity_type,
            entity_id=entity_id,
            status="OPEN",
        )
        .one_or_none()
    )


def open_or_update_incident(
    session,
    now: datetime,
    shop_key: str,
    incident_type: str,
    entity_type: str,
    entity_id: str | None,
    severity: str,
    title: str,
    message: str,
    meta_json: str | None,
) -> tuple[AdsIncident, bool]:
    incident = get_open_incident(session, shop_key, incident_type, entity_type, entity_id)
    created = False
    if incident is None:
        incident = AdsIncident(
            shop_key=shop_key,
            incident_type=incident_type,
            entity_type=entity_type,
            entity_id=entity_id,
            status="OPEN",
            severity=severity,
            title=title,
            message=message,
            meta_json=meta_json,
            first_seen_at=now,
            last_seen_at=now,
            last_notified_at=None,
            resolved_at=None,
        )
        session.add(incident)
        created = True
    else:
        incident.severity = severity
        incident.title = title
        incident.message = message
        incident.meta_json = meta_json
        incident.last_seen_at = now
    return incident, created


def resolve_incident(session, incident: AdsIncident, now: datetime) -> None:
    incident.status = "RESOLVED"
    incident.resolved_at = now
    incident.last_seen_at = now

from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine, func
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import get_settings

_engine = None
SessionLocal = sessionmaker(autocommit=False, autoflush=False)

Base = declarative_base()


class EventLog(Base):
    __tablename__ = "event_log"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    level = Column(String(20), nullable=False)
    message = Column(String(500), nullable=False)
    meta_json = Column(Text, nullable=True)


def init_db() -> None:
    # Import models so they are registered on Base before create_all.
    from .ads import models as _ads_models  # noqa: F401
    from .ads import incidents as _ads_incidents  # noqa: F401
    from .ops import doctor_notify as _doctor_notify  # noqa: F401
    from .ops import alert_state as _alert_state  # noqa: F401
    from .shopee import token_store as _token_store  # noqa: F401

    Base.metadata.create_all(bind=get_engine())
    _token_store.ensure_refresh_token_expires_at_column(get_engine())
    _token_store.ensure_updated_at_column(get_engine())
    _doctor_notify.ensure_doctor_notify_state_columns(get_engine())


def get_engine():
    global _engine
    settings = get_settings()
    if _engine is None or str(_engine.url) != settings.database_url:
        _engine = create_engine(settings.database_url, future=True)
        SessionLocal.configure(bind=_engine)
    return _engine

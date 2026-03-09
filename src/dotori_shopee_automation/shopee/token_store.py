from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import Column, DateTime, Integer, String, Text, inspect, text

from ..db import Base


class ShopeeToken(Base):
    __tablename__ = "shopee_tokens"

    shop_key = Column(String(50), primary_key=True)
    shop_id = Column(Integer, nullable=False)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    access_token_expires_at = Column(DateTime(timezone=True), nullable=True)
    refresh_token_expires_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


def get_token(session, shop_key: str) -> ShopeeToken | None:
    return session.query(ShopeeToken).filter_by(shop_key=shop_key).one_or_none()


def upsert_token(
    session,
    shop_key: str,
    shop_id: int,
    access_token: str,
    refresh_token: str,
    access_token_expires_at: datetime | None,
    refresh_token_expires_at: datetime | None = None,
) -> ShopeeToken:
    now = datetime.now(timezone.utc)
    token = get_token(session, shop_key)
    if token is None:
        token = ShopeeToken(
            shop_key=shop_key,
            shop_id=shop_id,
            access_token=access_token,
            refresh_token=refresh_token,
            access_token_expires_at=access_token_expires_at,
            refresh_token_expires_at=refresh_token_expires_at,
            updated_at=now,
        )
        session.add(token)
    else:
        token.shop_id = shop_id
        token.access_token = access_token
        token.refresh_token = refresh_token
        token.access_token_expires_at = access_token_expires_at
        token.refresh_token_expires_at = refresh_token_expires_at
        token.updated_at = now
    return token


def ensure_refresh_token_expires_at_column(engine) -> None:
    try:
        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("shopee_tokens")]
    except Exception:
        return


def ensure_updated_at_column(engine) -> None:
    try:
        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("shopee_tokens")]
    except Exception:
        return
    if "updated_at" in columns:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("ALTER TABLE shopee_tokens ADD COLUMN updated_at DATETIME")
            )
    except Exception:
        return
    if "refresh_token_expires_at" in columns:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE shopee_tokens ADD COLUMN refresh_token_expires_at DATETIME"
                )
            )
    except Exception:
        return


def needs_refresh(expires_at: datetime | None, skew_seconds: int = 300) -> bool:
    if expires_at is None:
        return True
    now = datetime.now(timezone.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at - now <= timedelta(seconds=skew_seconds)

from __future__ import annotations

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)

from ..db import Base


class AdsCampaign(Base):
    __tablename__ = "ads_campaign"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    campaign_id = Column(String(100), nullable=False)
    campaign_name = Column(String(200), nullable=False)
    status = Column(String(50), nullable=True)
    daily_budget = Column(Numeric(18, 2), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("shop_key", "campaign_id", name="uq_ads_campaign_shop_campaign"),
    )


class AdsCampaignDaily(Base):
    __tablename__ = "ads_campaign_daily"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    campaign_id = Column(String(100), nullable=False)
    date = Column(Date, nullable=False)
    spend = Column(Numeric(18, 2), nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    orders = Column(Integer, nullable=False)
    gmv = Column(Numeric(18, 2), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "shop_key",
            "campaign_id",
            "date",
            name="uq_ads_campaign_daily_shop_campaign_date",
        ),
    )


class AdsCampaignSnapshot(Base):
    __tablename__ = "ads_campaign_snapshot"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    campaign_id = Column(String(100), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    spend_today = Column(Numeric(18, 2), nullable=False)
    impressions_today = Column(Integer, nullable=False)
    clicks_today = Column(Integer, nullable=False)
    orders_today = Column(Integer, nullable=False)
    gmv_today = Column(Numeric(18, 2), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "shop_key",
            "campaign_id",
            "ts",
            name="uq_ads_campaign_snapshot_shop_campaign_ts",
        ),
    )


class AdsAccountBalanceSnapshot(Base):
    __tablename__ = "ads_account_balance_snapshot"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    total_balance = Column(Numeric(18, 2), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "shop_key",
            "ts",
            name="uq_ads_account_balance_snapshot_shop_ts",
        ),
    )


class Phase1AdsGmsCampaignRegistry(Base):
    __tablename__ = "phase1_ads_gms_campaign_registry"

    id = Column(Integer, primary_key=True)
    shop_key = Column(String(50), nullable=False)
    as_of_date = Column(Date, nullable=False)
    campaign_id = Column(String(120), nullable=False)
    campaign_type = Column(String(50), nullable=True)
    campaign_name = Column(String(255), nullable=True)
    daily_budget = Column(Numeric(18, 2), nullable=True)
    total_budget = Column(Numeric(18, 2), nullable=True)
    spend = Column(Numeric(18, 2), nullable=True)
    fetched_at_utc = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    source_run_dir = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "shop_key",
            "as_of_date",
            "campaign_id",
            name="uq_phase1_ads_gms_registry_shop_date_campaign",
        ),
    )

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from .models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from .provider_base import AdsProvider, Campaign, DailyMetric, SnapshotMetric
from ..db import EventLog, SessionLocal, init_db


def ingest_daily(
    shop_key: str,
    provider: AdsProvider,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, int]:
    init_db()
    start = start or date.min
    end = end or date.max
    session = SessionLocal()
    try:
        daily_rows = provider.fetch_daily(shop_key, start, end)
        campaigns = _campaigns_from_daily(daily_rows)
        campaigns_count = _upsert_campaigns(session, shop_key, campaigns)
        daily_count = _upsert_daily(session, shop_key, daily_rows)
        session.commit()

        summary = {
            "campaigns": campaigns_count,
            "daily": daily_count,
            "snapshots": 0,
        }
        _log_event(session, "INFO", f"ads_ingest_daily shop={shop_key}", summary)
        session.commit()
        return summary
    except (ValueError, SQLAlchemyError) as exc:
        session.rollback()
        _log_event(session, "ERROR", f"ads_ingest_daily_failed shop={shop_key}", {"error": str(exc)})
        session.commit()
        raise
    finally:
        session.close()


def ingest_snapshot(
    shop_key: str,
    provider: AdsProvider,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, int]:
    init_db()
    start = start or datetime.min.replace(tzinfo=timezone.utc)
    end = end or datetime.max.replace(tzinfo=timezone.utc)
    session = SessionLocal()
    try:
        snapshot_rows = provider.fetch_snapshots(shop_key, start, end)
        campaigns = _campaigns_from_snapshot(snapshot_rows)
        campaigns_count = _upsert_campaigns(session, shop_key, campaigns)
        snapshot_count = _upsert_snapshot(session, shop_key, snapshot_rows)
        session.commit()

        summary = {
            "campaigns": campaigns_count,
            "daily": 0,
            "snapshots": snapshot_count,
        }
        _log_event(session, "INFO", f"ads_ingest_snapshot shop={shop_key}", summary)
        session.commit()
        return summary
    except (ValueError, SQLAlchemyError) as exc:
        session.rollback()
        _log_event(
            session,
            "ERROR",
            f"ads_ingest_snapshot_failed shop={shop_key}",
            {"error": str(exc)},
        )
        session.commit()
        raise
    finally:
        session.close()


def summary_daily(shop_key: str, target_date: date) -> dict[str, object]:
    init_db()
    session = SessionLocal()
    try:
        totals_row = session.execute(
            select(
                func.coalesce(func.sum(AdsCampaignDaily.spend), 0),
                func.coalesce(func.sum(AdsCampaignDaily.impressions), 0),
                func.coalesce(func.sum(AdsCampaignDaily.clicks), 0),
                func.coalesce(func.sum(AdsCampaignDaily.orders), 0),
                func.coalesce(func.sum(AdsCampaignDaily.gmv), 0),
            ).where(
                AdsCampaignDaily.shop_key == shop_key,
                AdsCampaignDaily.date == target_date,
            )
        ).one()
        totals = {
            "spend": totals_row[0],
            "impressions": totals_row[1],
            "clicks": totals_row[2],
            "orders": totals_row[3],
            "gmv": totals_row[4],
        }

        top_rows = session.execute(
            select(
                AdsCampaignDaily.campaign_id,
                AdsCampaign.campaign_name,
                AdsCampaignDaily.spend,
            )
            .join(
                AdsCampaign,
                (AdsCampaign.shop_key == AdsCampaignDaily.shop_key)
                & (AdsCampaign.campaign_id == AdsCampaignDaily.campaign_id),
            )
            .where(
                AdsCampaignDaily.shop_key == shop_key,
                AdsCampaignDaily.date == target_date,
            )
            .order_by(AdsCampaignDaily.spend.desc())
            .limit(5)
        ).all()

        top_campaigns = [
            {
                "campaign_id": row[0],
                "campaign_name": row[1],
                "spend": row[2],
            }
            for row in top_rows
        ]

        return {"totals": totals, "top_campaigns": top_campaigns}
    finally:
        session.close()


def _campaigns_from_daily(rows: list[DailyMetric]) -> list[Campaign]:
    seen: dict[str, Campaign] = {}
    for row in rows:
        seen[row.campaign_id] = Campaign(
            campaign_id=row.campaign_id,
            campaign_name=row.campaign_name,
            status=row.status,
            daily_budget=row.daily_budget,
        )
    return list(seen.values())


def _campaigns_from_snapshot(rows: list[SnapshotMetric]) -> list[Campaign]:
    seen: dict[str, Campaign] = {}
    for row in rows:
        seen[row.campaign_id] = Campaign(
            campaign_id=row.campaign_id,
            campaign_name=row.campaign_name,
            status=row.status,
            daily_budget=row.daily_budget,
        )
    return list(seen.values())


def _upsert_campaigns(
    session,
    shop_key: str,
    campaigns: list[Campaign],
) -> int:
    count = 0
    for campaign in campaigns:
        existing = (
            session.query(AdsCampaign)
            .filter_by(shop_key=shop_key, campaign_id=campaign.campaign_id)
            .one_or_none()
        )
        if existing:
            existing.campaign_name = campaign.campaign_name
            if campaign.status is not None:
                existing.status = campaign.status
            if campaign.daily_budget is not None:
                existing.daily_budget = campaign.daily_budget
        else:
            session.add(
                AdsCampaign(
                    shop_key=shop_key,
                    campaign_id=campaign.campaign_id,
                    campaign_name=campaign.campaign_name,
                    status=campaign.status,
                    daily_budget=campaign.daily_budget,
                )
            )
        count += 1
    return count


def _upsert_daily(
    session,
    shop_key: str,
    rows: list[DailyMetric],
) -> int:
    count = 0
    for row in rows:
        existing = (
            session.query(AdsCampaignDaily)
            .filter_by(shop_key=shop_key, campaign_id=row.campaign_id, date=row.date)
            .one_or_none()
        )
        if existing:
            existing.spend = row.spend
            existing.impressions = row.impressions
            existing.clicks = row.clicks
            existing.orders = row.orders
            existing.gmv = row.gmv
        else:
            session.add(
                AdsCampaignDaily(
                    shop_key=shop_key,
                    campaign_id=row.campaign_id,
                    date=row.date,
                    spend=row.spend,
                    impressions=row.impressions,
                    clicks=row.clicks,
                    orders=row.orders,
                    gmv=row.gmv,
                )
            )
        count += 1
    return count


def _upsert_snapshot(
    session,
    shop_key: str,
    rows: list[SnapshotMetric],
) -> int:
    count = 0
    for row in rows:
        existing = (
            session.query(AdsCampaignSnapshot)
            .filter_by(shop_key=shop_key, campaign_id=row.campaign_id, ts=row.ts)
            .one_or_none()
        )
        if existing:
            existing.spend_today = row.spend_today
            existing.impressions_today = row.impressions_today
            existing.clicks_today = row.clicks_today
            existing.orders_today = row.orders_today
            existing.gmv_today = row.gmv_today
        else:
            session.add(
                AdsCampaignSnapshot(
                    shop_key=shop_key,
                    campaign_id=row.campaign_id,
                    ts=row.ts,
                    spend_today=row.spend_today,
                    impressions_today=row.impressions_today,
                    clicks_today=row.clicks_today,
                    orders_today=row.orders_today,
                    gmv_today=row.gmv_today,
                )
            )
        count += 1
    return count


def _log_event(session, level: str, message: str, meta: dict[str, object]) -> None:
    session.add(
        EventLog(
            level=level,
            message=message,
            meta_json=json.dumps(meta, ensure_ascii=True),
        )
    )

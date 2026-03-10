from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import and_

from .campaign_labels import resolve_campaign_display_name
from .incidents import AdsIncident, get_open_incident, open_or_update_incident, resolve_incident
from .models import AdsAccountBalanceSnapshot, AdsCampaign, AdsCampaignSnapshot
try:
    from .reporting import load_report_totals_source
except ImportError:
    # Backward compatibility for older deployed reporting modules.
    def load_report_totals_source(*args, **kwargs):  # type: ignore[override]
        return {"totals_source": "unknown", "totals_basis": "unknown"}
from ..config import get_settings, resolve_timezone
from ..db import EventLog
from ..discord_notifier import send as discord_send

_DAY_SCOPED_INCIDENT_TYPES = {
    "pacing_overspend",
    "pacing_underspend",
    "health_no_impressions",
    "health_spend_no_orders",
    "spend_spike_60m",
    "ctr_drop_60m",
    "cvr_drop_60m",
}


@dataclass(frozen=True)
class ActiveAlert:
    incident_type: str
    entity_type: str
    entity_id: str | None
    severity: str
    title: str
    campaign_name: str | None
    shop_key: str | None
    meta: dict[str, Any]


@dataclass(frozen=True)
class DetectionResult:
    alerts: list[ActiveAlert]


def detect_alerts(shop_key: str, now: datetime, session) -> DetectionResult:
    settings = get_settings()
    tz = resolve_timezone(settings.timezone)
    now_local = _ensure_tz(now, tz)
    now_db = now_local.replace(tzinfo=None)
    day_start_db = datetime.combine(now_local.date(), time.min)
    window_start = now_local - timedelta(minutes=60)

    rows = (
        session.query(
            AdsCampaignSnapshot.campaign_id,
            AdsCampaign.campaign_name,
            AdsCampaignSnapshot.ts,
            AdsCampaignSnapshot.spend_today,
            AdsCampaignSnapshot.impressions_today,
            AdsCampaignSnapshot.clicks_today,
            AdsCampaignSnapshot.orders_today,
            AdsCampaignSnapshot.gmv_today,
            AdsCampaign.status,
            AdsCampaign.daily_budget,
        )
        .join(
            AdsCampaign,
            and_(
                AdsCampaign.shop_key == AdsCampaignSnapshot.shop_key,
                AdsCampaign.campaign_id == AdsCampaignSnapshot.campaign_id,
            ),
        )
        .filter(
            AdsCampaignSnapshot.shop_key == shop_key,
            AdsCampaignSnapshot.ts >= day_start_db,
            AdsCampaignSnapshot.ts <= now_db,
        )
        .order_by(AdsCampaignSnapshot.ts.asc())
        .all()
    )

    campaigns: dict[str, dict[str, Any]] = {}
    for row in rows:
        campaign_id = row[0]
        campaign_name = row[1]
        ts = _normalize_dt(row[2], tz)
        entry = campaigns.setdefault(
            campaign_id,
            {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "campaign_status": row[8],
                "daily_budget": row[9],
                "latest": None,
                "window_earliest": None,
                "window_latest": None,
                "baseline_before_window": None,
            },
        )
        entry["campaign_status"] = row[8]
        entry["daily_budget"] = row[9]

        latest = entry["latest"]
        if latest is None or ts > latest["ts"]:
            entry["latest"] = _row_to_metrics(row, ts)

        if ts <= window_start:
            baseline = entry["baseline_before_window"]
            if baseline is None or ts > baseline["ts"]:
                entry["baseline_before_window"] = _row_to_metrics(row, ts)

        if window_start <= ts <= now_local:
            earliest = entry["window_earliest"]
            if earliest is None or ts < earliest["ts"]:
                entry["window_earliest"] = _row_to_metrics(row, ts)
            latest_window = entry["window_latest"]
            if latest_window is None or ts > latest_window["ts"]:
                entry["window_latest"] = _row_to_metrics(row, ts)

    elapsed_minutes = max((now_local - datetime.combine(now_local.date(), time.min, tzinfo=tz)).total_seconds() / 60, 0)
    elapsed_ratio = min(elapsed_minutes / 1440, 1)
    daily_totals_source = load_alerts_source_totals(
        session=session,
        shop_key=shop_key,
        target_date=now_local.date(),
    )

    alerts: list[ActiveAlert] = []
    for entry in campaigns.values():
        latest = entry["latest"]
        if latest is None:
            continue
        campaign_status = str(entry.get("campaign_status") or "").strip().upper()
        if campaign_status in {"PAUSED", "STOPPED", "ENDED", "INACTIVE", "DELETED"}:
            continue
        budget = entry["daily_budget"]
        if budget is not None:
            budget = _to_decimal(budget)
        spend_today = _to_decimal(latest["spend"])
        impressions_today = int(latest["impressions"])
        clicks_today = int(latest["clicks"])
        orders_today = int(latest["orders"])
        gmv_today = _to_decimal(latest["gmv"])

        spent_ratio = None
        if budget and budget > 0:
            spent_ratio = spend_today / budget

        # Rule 1: overspend
        if spent_ratio is not None:
            if spent_ratio - Decimal(str(elapsed_ratio)) >= Decimal("0.30") and spent_ratio >= Decimal("0.40"):
                severity = "WARN"
                if spent_ratio >= Decimal("0.80") and elapsed_ratio < 0.60:
                    severity = "CRITICAL"
                alerts.append(
                    ActiveAlert(
                        incident_type="pacing_overspend",
                        entity_type="campaign",
                        entity_id=entry["campaign_id"],
                        severity=severity,
                        title="Toc do pacing vuot nhanh",
                        campaign_name=entry["campaign_name"],
                        shop_key=shop_key,
                        meta={
                            "spend_today": spend_today,
                            "daily_budget": budget,
                            "spent_ratio": spent_ratio,
                            "elapsed_ratio": Decimal(str(elapsed_ratio)),
                            "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                            "shop_spend_total": daily_totals_source.get("spend"),
                            "shop_orders_total": daily_totals_source.get("orders"),
                            "shop_impressions_total": daily_totals_source.get("impressions"),
                        },
                    )
                )

        # Window metrics for rules 2-4
        earliest = entry["window_earliest"]
        latest_window = entry["window_latest"]
        baseline = entry["baseline_before_window"]

        delta_latest = latest_window or latest
        if earliest and latest_window:
            delta_earliest = earliest
        elif baseline:
            delta_earliest = baseline
        else:
            delta_earliest = None

        if delta_earliest and delta_latest:
            impr_delta = int(delta_latest["impressions"]) - int(delta_earliest["impressions"])
            click_delta = int(delta_latest["clicks"]) - int(delta_earliest["clicks"])
            spend_delta = _to_decimal(delta_latest["spend"]) - _to_decimal(delta_earliest["spend"])
            orders_delta = int(delta_latest["orders"]) - int(delta_earliest["orders"])

            # Rule 2: underspend
            if spent_ratio is not None:
                if (
                    Decimal(str(elapsed_ratio)) - spent_ratio >= Decimal("0.30")
                    and elapsed_ratio >= 0.50
                    and impr_delta < 10
                ):
                    severity = "INFO"
                    alerts.append(
                        ActiveAlert(
                            incident_type="pacing_underspend",
                            entity_type="campaign",
                            entity_id=entry["campaign_id"],
                            severity=severity,
                            title="Toc do pacing qua cham",
                            campaign_name=entry["campaign_name"],
                            shop_key=shop_key,
                            meta={
                                "spend_today": spend_today,
                                "daily_budget": budget,
                                "spent_ratio": spent_ratio,
                                "elapsed_ratio": Decimal(str(elapsed_ratio)),
                                "impr_delta": impr_delta,
                                "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                            },
                        )
                    )

            # Rule 3: no impressions
            if spend_delta >= Decimal("1000") and impr_delta <= 0 and click_delta == 0:
                alerts.append(
                    ActiveAlert(
                        incident_type="health_no_impressions",
                        entity_type="campaign",
                        entity_id=entry["campaign_id"],
                        severity="WARN",
                        title="Hieu qua hien thi thap trong 60 phut gan nhat",
                        campaign_name=entry["campaign_name"],
                        shop_key=shop_key,
                        meta={
                            "impr_delta": impr_delta,
                            "click_delta": click_delta,
                            "spend_delta": spend_delta,
                            "impressions_today": impressions_today,
                            "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                        },
                    )
                )

            # Rule 4: spend no orders
            spend_threshold = Decimal("20")
            if budget and budget > 0:
                spend_threshold = max(budget * Decimal("0.10"), Decimal("10"))
            if spend_delta >= spend_threshold and orders_delta == 0:
                severity = "WARN"
                if budget and budget > 0 and spend_delta >= budget * Decimal("0.30"):
                    severity = "CRITICAL"
                elif not budget and spend_delta >= Decimal("50"):
                    severity = "CRITICAL"
                alerts.append(
                    ActiveAlert(
                        incident_type="health_spend_no_orders",
                        entity_type="campaign",
                        entity_id=entry["campaign_id"],
                        severity=severity,
                        title="Chi tieu tang nhung khong co don hang",
                        campaign_name=entry["campaign_name"],
                        shop_key=shop_key,
                        meta={
                            "spend_delta": spend_delta,
                            "orders_delta": orders_delta,
                            "daily_budget": budget,
                                "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                            },
                        )
                    )

            # Rule 5: spend spike in last 60m
            spend_spike_warn = Decimal("30000")
            spend_spike_critical = Decimal("50000")
            if budget and budget > 0:
                spend_spike_warn = max(spend_spike_warn, budget * Decimal("0.18"))
                spend_spike_critical = max(spend_spike_critical, budget * Decimal("0.30"))
            if spend_delta >= spend_spike_warn:
                severity = "WARN"
                if spend_delta >= spend_spike_critical:
                    severity = "CRITICAL"
                alerts.append(
                    ActiveAlert(
                        incident_type="spend_spike_60m",
                        entity_type="campaign",
                        entity_id=entry["campaign_id"],
                        severity=severity,
                        title="Chi tieu tang dot bien trong 60 phut",
                        campaign_name=entry["campaign_name"],
                        shop_key=shop_key,
                        meta={
                            "spend_delta": spend_delta,
                            "warn_threshold": spend_spike_warn,
                            "critical_threshold": spend_spike_critical,
                            "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                        },
                    )
                )

            # Rule 6: CTR drop in last 60m (independent from no-impression incident)
            if impr_delta >= 500 and spend_delta >= Decimal("10000"):
                ctr_60m = Decimal("0")
                if impr_delta > 0:
                    ctr_60m = (Decimal(click_delta) / Decimal(impr_delta)) * Decimal("100")
                ctr_threshold = Decimal("0.80")
                if ctr_60m < ctr_threshold:
                    alerts.append(
                        ActiveAlert(
                            incident_type="ctr_drop_60m",
                            entity_type="campaign",
                            entity_id=entry["campaign_id"],
                            severity="WARN",
                            title="CTR giam bat thuong trong 60 phut",
                            campaign_name=entry["campaign_name"],
                            shop_key=shop_key,
                            meta={
                                "ctr_60m": ctr_60m,
                                "ctr_threshold": ctr_threshold,
                                "impr_delta": impr_delta,
                                "click_delta": click_delta,
                                "spend_delta": spend_delta,
                                "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                            },
                        )
                    )

            # Rule 7: CVR drop in last 60m (independent from spend-no-orders incident)
            if click_delta >= 30 and spend_delta >= Decimal("10000"):
                cvr_60m = Decimal("0")
                if click_delta > 0:
                    cvr_60m = (Decimal(orders_delta) / Decimal(click_delta)) * Decimal("100")
                cvr_threshold = Decimal("0.50")
                if cvr_60m < cvr_threshold:
                    alerts.append(
                        ActiveAlert(
                            incident_type="cvr_drop_60m",
                            entity_type="campaign",
                            entity_id=entry["campaign_id"],
                            severity="WARN",
                            title="CVR giam bat thuong trong 60 phut",
                            campaign_name=entry["campaign_name"],
                            shop_key=shop_key,
                            meta={
                                "cvr_60m": cvr_60m,
                                "cvr_threshold": cvr_threshold,
                                "click_delta": click_delta,
                                "orders_delta": orders_delta,
                                "spend_delta": spend_delta,
                                "totals_source": "ads_campaign_daily(shop_total_or_campaign_sum)",
                            },
                        )
                    )

    balance_row = (
        session.query(
            AdsAccountBalanceSnapshot.ts,
            AdsAccountBalanceSnapshot.total_balance,
        )
        .filter(
            AdsAccountBalanceSnapshot.shop_key == shop_key,
            AdsAccountBalanceSnapshot.ts <= now_db,
        )
        .order_by(AdsAccountBalanceSnapshot.ts.desc())
        .first()
    )
    if balance_row:
        balance_value = _to_decimal(balance_row[1])
        low_threshold = Decimal("50000")
        if balance_value <= low_threshold:
            alerts.append(
                ActiveAlert(
                    incident_type="account_balance_low",
                    entity_type="account",
                    entity_id="total_balance",
                    severity="WARN",
                    title="So du quang cao thap",
                    campaign_name=None,
                    shop_key=shop_key,
                    meta={
                        "current_balance": balance_value,
                        "low_threshold": low_threshold,
                        "balance_ts": _normalize_dt(balance_row[0], tz).isoformat(),
                    },
                )
            )
        if balance_value <= Decimal("0"):
            alerts.append(
                ActiveAlert(
                    incident_type="account_balance_zero",
                    entity_type="account",
                    entity_id="total_balance",
                    severity="CRITICAL",
                    title="So du quang cao da het",
                    campaign_name=None,
                    shop_key=shop_key,
                    meta={
                        "current_balance": balance_value,
                        "low_threshold": low_threshold,
                        "balance_ts": _normalize_dt(balance_row[0], tz).isoformat(),
                    },
                )
            )

    return DetectionResult(alerts=alerts)


def process_alerts(
    shop_key: str,
    now: datetime,
    session,
    shop_label: str,
    webhook_url: str | None,
    cooldown_minutes: int,
    send_discord: bool = True,
    notify_resolved: bool = True,
) -> dict[str, int]:
    settings = get_settings()
    tz = resolve_timezone(settings.timezone)
    now_local = _ensure_tz(now, tz)
    now_db = now_local.replace(tzinfo=None)

    result = detect_alerts(shop_key, now, session)
    alerts = result.alerts

    active_keys = {
        (alert.incident_type, alert.entity_type, alert.entity_id) for alert in alerts
    }

    opened = 0
    updated = 0
    resolved = 0
    notified = 0
    suppressed = 0
    overlap_suppressed = 0

    for alert in alerts:
        if _is_overlap_suppressed_by_open_incident(session, shop_key, alert):
            overlap_suppressed += 1
            continue
        meta_json = _safe_json(alert.meta)
        incident, created = open_or_update_incident(
            session,
            now,
            shop_key,
            alert.incident_type,
            alert.entity_type,
            alert.entity_id,
            alert.severity,
            alert.title,
            alert_message(alert),
            meta_json,
        )
        if created:
            opened += 1
            session.add(
                EventLog(
                    level="INFO",
                    message="incident_opened",
                    meta_json=_safe_json(
                        {
                            "incident_type": alert.incident_type,
                            "shop_key": shop_key,
                            "entity_id": alert.entity_id,
                            "severity": alert.severity,
                        }
                    ),
                )
            )
        else:
            updated += 1
        notify_on_create_only = alert.incident_type in {
            "account_balance_low",
            "account_balance_zero",
        }
        if notify_on_create_only:
            # Balance alerts are one-shot by design, but if an incident was created while
            # send_discord=0 we still need one first notification after re-enabling send.
            should_notify = created or incident.last_notified_at is None
        else:
            should_notify = created or _cooldown_elapsed(incident, now, cooldown_minutes)
        if should_notify:
            if send_discord:
                repeat_notify = (not created) and (incident.last_notified_at is not None)
                discord_send(
                    "alerts",
                    alert_message(alert, repeat=repeat_notify),
                    shop_label=shop_label,
                    webhook_url=webhook_url,
                )
                incident.last_notified_at = now
                notified += 1
                session.add(
                    EventLog(
                        level="INFO",
                        message="incident_notified",
                        meta_json=_safe_json(
                            {
                                "incident_type": alert.incident_type,
                                "shop_key": shop_key,
                                "entity_id": alert.entity_id,
                            }
                        ),
                    )
                )
        else:
            if send_discord:
                suppressed += 1

    open_incidents = (
        session.query(AdsIncident)
        .filter_by(shop_key=shop_key, status="OPEN")
        .all()
    )

    for incident in open_incidents:
        key = (incident.incident_type, incident.entity_type, incident.entity_id)
        if key in active_keys:
            continue
        if incident.incident_type in {"account_balance_low", "account_balance_zero"}:
            balance_meta = _load_latest_balance_meta(
                session=session,
                shop_key=shop_key,
                now_db=now_db,
                tz=tz,
            )
            if balance_meta is None:
                # Avoid false "resolved" messages when balance feed is temporarily unavailable.
                continue
            current_balance = _to_decimal(balance_meta.get("current_balance"))
            low_threshold = _to_decimal(balance_meta.get("low_threshold"))
            if incident.incident_type == "account_balance_low" and current_balance <= low_threshold:
                incident.last_seen_at = now
                incident.meta_json = _safe_json(balance_meta)
                continue
            if incident.incident_type == "account_balance_zero" and current_balance <= Decimal("0"):
                incident.last_seen_at = now
                incident.meta_json = _safe_json(balance_meta)
                continue
            # For real resolve cases, replace stale open-time meta with fresh balance snapshot.
            incident.meta_json = _safe_json(balance_meta)
        resolve_incident(session, incident, now)
        resolved += 1
        session.add(
            EventLog(
                level="INFO",
                message="incident_resolved",
                meta_json=_safe_json(
                    {
                        "incident_type": incident.incident_type,
                        "shop_key": shop_key,
                        "entity_id": incident.entity_id,
                    }
                ),
            )
        )
        if notify_resolved and send_discord:
            if _is_day_boundary_auto_resolve(incident, now):
                session.add(
                    EventLog(
                        level="INFO",
                        message="incident_resolved_notify_suppressed",
                        meta_json=_safe_json(
                            {
                                "incident_type": incident.incident_type,
                                "shop_key": shop_key,
                                "entity_id": incident.entity_id,
                                "reason": "day_boundary_auto_reset",
                            }
                        ),
                    )
                )
                continue
            discord_send(
                "alerts",
                alert_message(_incident_to_alert(incident), resolved=True),
                shop_label=shop_label,
                webhook_url=webhook_url,
            )
            incident.last_notified_at = now
            notified += 1

    return {
        "active": len(alerts),
        "opened": opened,
        "updated": updated,
        "resolved": resolved,
        "notified": notified,
        "suppressed": suppressed,
        "overlap_suppressed": overlap_suppressed,
    }


def load_alerts_source_totals(
    *,
    session,
    shop_key: str,
    target_date: date_type,
) -> dict[str, Any]:
    return load_report_totals_source(
        session,
        shop_key=shop_key,
        target_date=target_date,
    )


def alert_message(alert: ActiveAlert, resolved: bool = False, repeat: bool = False) -> str:
    title = alert.title
    if resolved:
        title = f"✅ RESOLVED / DA XU LY {title}"
    elif repeat:
        title = f"🔁 RE-ALERT / NHAC LAI {title}"
    lines = [title]
    lines.append(f"muc_do={str(alert.severity or 'INFO').upper()}")
    if alert.entity_type == "campaign":
        campaign_id = str(alert.entity_id or "").strip()
        label = resolve_campaign_display_name(
            shop_key=alert.shop_key,
            campaign_id=campaign_id,
            campaign_name=alert.campaign_name,
        )
        if campaign_id and label != campaign_id:
            lines.append(f"chien_dich: {label} ({campaign_id})")
        else:
            lines.append(f"chien_dich: {label or '-'}")

    meta = alert.meta
    if alert.incident_type.startswith("pacing_"):
        lines.append(
            "chi_tieu={}/ngan_sach={} ty_le_chi={} ty_le_thoi_gian={}".format(
                _fmt_money(meta.get("spend_today")),
                _fmt_money(meta.get("daily_budget")),
                _fmt_ratio(meta.get("spent_ratio")),
                _fmt_ratio(meta.get("elapsed_ratio")),
            )
        )
        lines.append("hanh_dong: kiem tra gia thau/ngan sach")
    elif alert.incident_type == "health_no_impressions":
        lines.append(
            "impr_delta={} click_delta={} chi_tieu_60p={}".format(
                meta.get("impr_delta"),
                meta.get("click_delta"),
                _fmt_money(meta.get("spend_delta")),
            )
        )
        lines.append("hanh_dong: kiem tra hieu_qua_quang_cao/target/gia_thau/trang_thai")
    elif alert.incident_type == "health_spend_no_orders":
        lines.append(
            "chi_tieu_tang={} don_hang_tang={}".format(
                _fmt_money(meta.get("spend_delta")),
                meta.get("orders_delta"),
            )
        )
        lines.append("hanh_dong: kiem tra creative/target")
    elif alert.incident_type == "spend_spike_60m":
        lines.append(
            "chi_tieu_60p={} nguong_warn={} nguong_critical={}".format(
                _fmt_money(meta.get("spend_delta")),
                _fmt_money(meta.get("warn_threshold")),
                _fmt_money(meta.get("critical_threshold")),
            )
        )
        lines.append("hanh_dong: kiem tra gia_thau/ngan_sach/gioi_han_chi_tieu")
    elif alert.incident_type == "ctr_drop_60m":
        lines.append(
            "ctr_60p={} nguong={} impr_delta={} click_delta={} chi_tieu_60p={}".format(
                _fmt_pct(meta.get("ctr_60m")),
                _fmt_pct(meta.get("ctr_threshold")),
                meta.get("impr_delta"),
                meta.get("click_delta"),
                _fmt_money(meta.get("spend_delta")),
            )
        )
        lines.append("hanh_dong: kiem tra noi_dung/doi_tuong/vi_tri_hien_thi")
    elif alert.incident_type == "cvr_drop_60m":
        lines.append(
            "cvr_60p={} nguong={} click_delta={} orders_delta={} chi_tieu_60p={}".format(
                _fmt_pct(meta.get("cvr_60m")),
                _fmt_pct(meta.get("cvr_threshold")),
                meta.get("click_delta"),
                meta.get("orders_delta"),
                _fmt_money(meta.get("spend_delta")),
            )
        )
        lines.append("hanh_dong: kiem tra trang_san_pham/gia/ton_kho/uu_dai")
    elif alert.incident_type in {"account_balance_low", "account_balance_zero"}:
        lines.append(
            "so_du_hien_tai={} nguong_canh_bao={}".format(
                _fmt_money(meta.get("current_balance")),
                _fmt_money(meta.get("low_threshold")),
            )
        )
        lines.append("thoi_diem_so_du={}".format(_fmt_local_ts(meta.get("balance_ts"))))
        if resolved:
            lines.append("so_du_sau_phuc_hoi={}".format(_fmt_money(meta.get("current_balance"))))
        lines.append("hanh_dong: nap tien vao tai khoan quang cao")
    else:
        lines.append("hanh_dong: kiem tra cau hinh chien dich")

    return "\n".join(lines)


def _cooldown_elapsed(incident: AdsIncident, now: datetime, cooldown_minutes: int) -> bool:
    if incident.last_notified_at is None:
        return True
    last = incident.last_notified_at
    now_dt = now
    if last.tzinfo is None and now_dt.tzinfo is not None:
        last = last.replace(tzinfo=now_dt.tzinfo)
    if last.tzinfo is not None and now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=last.tzinfo)
    delta = now_dt - last
    return delta.total_seconds() >= cooldown_minutes * 60


def _incident_to_alert(incident: AdsIncident) -> ActiveAlert:
    meta = {}
    if incident.meta_json:
        try:
            import json

            meta = json.loads(incident.meta_json)
        except Exception:
            meta = {}
    return ActiveAlert(
        incident_type=incident.incident_type,
        entity_type=incident.entity_type,
        entity_id=incident.entity_id,
        severity=incident.severity,
        title=incident.title,
        campaign_name=None,
        shop_key=incident.shop_key,
        meta=meta,
    )


def _row_to_metrics(row, ts: datetime) -> dict[str, Any]:
    return {
        "ts": ts,
        "spend": row[3],
        "impressions": row[4],
        "clicks": row[5],
        "orders": row[6],
        "gmv": row[7],
    }


def _ensure_tz(value: datetime, tz) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def _normalize_dt(value: datetime, tz) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "-"
    amount = Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP)
    return f"{int(amount):,}₫"


def _fmt_ratio(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{Decimal(str(value)).quantize(Decimal('0.01'))}"


def _fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{Decimal(str(value)).quantize(Decimal('0.01'))}%"


def _fmt_local_ts(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return "-"
        normalized = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return text
    settings = get_settings()
    tz = resolve_timezone(settings.timezone)
    local = _normalize_dt(dt, tz)
    offset = local.strftime("%z")
    offset_text = f"GMT{offset[:3]}:{offset[3:]}" if len(offset) == 5 else "GMT+07:00"
    return f"{local.strftime('%Y-%m-%d %H:%M:%S')} ({offset_text})"


def _is_day_boundary_auto_resolve(incident: AdsIncident, now: datetime) -> bool:
    if incident.incident_type not in _DAY_SCOPED_INCIDENT_TYPES:
        return False
    opened_at = getattr(incident, "first_seen_at", None)
    if opened_at is None:
        return False
    settings = get_settings()
    tz = resolve_timezone(settings.timezone)
    opened_local = _normalize_dt(opened_at, tz)
    now_local = _normalize_dt(now, tz)
    return opened_local.date() < now_local.date()


def _load_latest_balance_meta(
    *,
    session,
    shop_key: str,
    now_db: datetime,
    tz,
) -> dict[str, Any] | None:
    row = (
        session.query(
            AdsAccountBalanceSnapshot.ts,
            AdsAccountBalanceSnapshot.total_balance,
        )
        .filter(
            AdsAccountBalanceSnapshot.shop_key == shop_key,
            AdsAccountBalanceSnapshot.ts <= now_db,
        )
        .order_by(AdsAccountBalanceSnapshot.ts.desc())
        .first()
    )
    if not row:
        return None
    return {
        "current_balance": _to_decimal(row[1]),
        "low_threshold": Decimal("50000"),
        "balance_ts": _normalize_dt(row[0], tz).isoformat(),
    }


def _safe_json(meta: dict[str, Any]) -> str:
    import json

    return json.dumps(meta, ensure_ascii=True, default=str)


def _is_overlap_suppressed_by_open_incident(
    session,
    shop_key: str,
    alert: ActiveAlert,
) -> bool:
    overlap_map = {
        "spend_spike_60m": ["pacing_overspend"],
        "ctr_drop_60m": ["health_no_impressions"],
        "cvr_drop_60m": ["health_spend_no_orders"],
    }
    blockers = overlap_map.get(alert.incident_type) or []
    for incident_type in blockers:
        blocker = get_open_incident(
            session=session,
            shop_key=shop_key,
            incident_type=incident_type,
            entity_type=alert.entity_type,
            entity_id=alert.entity_id,
        )
        if blocker is not None:
            return True
    return False

from datetime import datetime, timedelta
from decimal import Decimal

from dotori_shopee_automation.ads.alerts import ActiveAlert, alert_message, process_alerts
from dotori_shopee_automation.ads.incidents import AdsIncident
from dotori_shopee_automation.ads.models import (
    AdsAccountBalanceSnapshot,
    AdsCampaign,
    AdsCampaignSnapshot,
)
from dotori_shopee_automation.config import get_settings, resolve_timezone
from dotori_shopee_automation.db import SessionLocal, init_db


def _setup_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    get_settings.cache_clear()
    init_db()


def _seed_campaign(session, shop_key: str, campaign_id: str, budget: Decimal | None) -> None:
    session.add(
        AdsCampaign(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name="Campaign One",
            status="ACTIVE",
            daily_budget=budget,
        )
    )


def _seed_snapshot(
    session,
    shop_key: str,
    campaign_id: str,
    ts: datetime,
    spend: Decimal,
    impressions: int,
    clicks: int,
    orders: int,
    gmv: Decimal,
) -> None:
    session.add(
        AdsCampaignSnapshot(
            shop_key=shop_key,
            campaign_id=campaign_id,
            ts=ts,
            spend_today=spend,
            impressions_today=impressions,
            clicks_today=clicks,
            orders_today=orders,
            gmv_today=gmv,
        )
    )


def _seed_balance_snapshot(
    session,
    shop_key: str,
    ts: datetime,
    total_balance: Decimal,
) -> None:
    session.add(
        AdsAccountBalanceSnapshot(
            shop_key=shop_key,
            ts=ts,
            total_balance=total_balance,
        )
    )


def test_alert_cooldown_and_resolve(monkeypatch, tmp_path) -> None:
    _setup_db(tmp_path, monkeypatch)
    tz = resolve_timezone("Asia/Ho_Chi_Minh")

    now = datetime(2026, 2, 1, 13, 15, tzinfo=tz)
    t1 = now - timedelta(minutes=60)

    with SessionLocal() as session:
        _seed_campaign(session, "shop_a", "cmp_1", Decimal("50000"))
        _seed_snapshot(session, "shop_a", "cmp_1", t1, Decimal("0"), 0, 0, 0, Decimal("0"))
        _seed_snapshot(session, "shop_a", "cmp_1", now, Decimal("2000"), 0, 0, 0, Decimal("0"))
        session.commit()

    send_calls = []

    def fake_send(*args, **kwargs) -> None:
        send_calls.append((args, kwargs))

    monkeypatch.setattr("dotori_shopee_automation.ads.alerts.discord_send", fake_send)

    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now,
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
        )
        session.commit()
    assert counts["opened"] >= 1
    assert counts["notified"] >= 1

    with SessionLocal() as session:
        incident = (
            session.query(AdsIncident)
            .filter_by(shop_key="shop_a", status="OPEN", incident_type="health_no_impressions")
            .one()
        )
        first_notified = incident.last_notified_at

    # Within cooldown - no notify
    with SessionLocal() as session:
        _seed_snapshot(
            session,
            "shop_a",
            "cmp_1",
            now + timedelta(minutes=30),
            Decimal("4000"),
            0,
            0,
            0,
            Decimal("0"),
        )
        session.commit()

    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now + timedelta(minutes=30),
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
        )
        session.commit()

    with SessionLocal() as session:
        incident = (
            session.query(AdsIncident)
            .filter_by(shop_key="shop_a", status="OPEN", incident_type="health_no_impressions")
            .one()
        )
        assert incident.last_notified_at == first_notified

    # Beyond cooldown - notify again
    with SessionLocal() as session:
        _seed_snapshot(
            session,
            "shop_a",
            "cmp_1",
            now + timedelta(minutes=130),
            Decimal("7000"),
            0,
            0,
            0,
            Decimal("0"),
        )
        session.commit()

    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now + timedelta(minutes=130),
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
        )
        session.commit()
    assert counts["notified"] >= 1

def test_account_balance_alert_low_then_zero_once(monkeypatch, tmp_path) -> None:
    _setup_db(tmp_path, monkeypatch)
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 1, 13, 0, tzinfo=tz)

    send_calls = []

    def fake_send(*args, **kwargs) -> None:
        send_calls.append((args, kwargs))

    monkeypatch.setattr("dotori_shopee_automation.ads.alerts.discord_send", fake_send)

    with SessionLocal() as session:
        _seed_balance_snapshot(session, "shop_a", now, Decimal("30000"))
        session.commit()

    # First low-balance alert should open + notify.
    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now,
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=False,
        )
        session.commit()
    assert counts["opened"] == 1
    assert counts["notified"] == 1

    # Same condition should not notify again (create-only policy for balance incidents).
    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now + timedelta(minutes=10),
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=False,
        )
        session.commit()
    assert counts["opened"] == 0
    assert counts["notified"] == 0

    with SessionLocal() as session:
        _seed_balance_snapshot(session, "shop_a", now + timedelta(minutes=20), Decimal("0"))
        session.commit()

    # Moving to zero should trigger one additional alert.
    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now + timedelta(minutes=20),
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=False,
        )
        session.commit()
    assert counts["opened"] == 1
    assert counts["notified"] == 1

    with SessionLocal() as session:
        open_types = {
            row[0]
            for row in session.query(AdsIncident.incident_type)
            .filter_by(shop_key="shop_a", status="OPEN")
            .all()
        }
    assert "account_balance_low" in open_types
    assert "account_balance_zero" in open_types


def test_alert_message_formats_vnd_without_decimals() -> None:
    alert = ActiveAlert(
        incident_type="account_balance_low",
        entity_type="account",
        entity_id="total_balance",
        severity="WARN",
        title="So du quang cao thap",
        campaign_name=None,
        shop_key="shop_a",
        meta={
            "current_balance": Decimal("30365.12"),
            "low_threshold": Decimal("50000.00"),
            "balance_ts": "2026-03-10T04:16:42+07:00",
        },
    )
    message = alert_message(alert)
    assert "so_du_hien_tai=30,365₫" in message
    assert "nguong_canh_bao=50,000₫" in message
    assert "thoi_diem_so_du=2026-03-10 04:16:42 (GMT+07:00)" in message


def test_alert_message_repeat_prefix() -> None:
    alert = ActiveAlert(
        incident_type="health_no_impressions",
        entity_type="campaign",
        entity_id="SHOP_TOTAL",
        severity="WARN",
        title="Khong co impression moi trong 60 phut gan nhat",
        campaign_name="SHOP_TOTAL",
        shop_key="shop_a",
        meta={"impr_delta": 0, "click_delta": 0},
    )
    message = alert_message(alert, repeat=True)
    assert message.startswith("🔁 RE-ALERT / NHAC LAI ")


def test_balance_resolved_message_uses_latest_snapshot(monkeypatch, tmp_path) -> None:
    _setup_db(tmp_path, monkeypatch)
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    start = datetime(2026, 2, 1, 13, 0, tzinfo=tz)

    send_calls = []

    def fake_send(*args, **kwargs) -> None:
        send_calls.append((args, kwargs))

    monkeypatch.setattr("dotori_shopee_automation.ads.alerts.discord_send", fake_send)

    with SessionLocal() as session:
        _seed_balance_snapshot(session, "shop_a", start, Decimal("30000"))
        session.commit()

    with SessionLocal() as session:
        process_alerts(
            shop_key="shop_a",
            now=start,
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=True,
        )
        session.commit()

    with SessionLocal() as session:
        _seed_balance_snapshot(session, "shop_a", start + timedelta(minutes=15), Decimal("70000"))
        session.commit()

    with SessionLocal() as session:
        process_alerts(
            shop_key="shop_a",
            now=start + timedelta(minutes=15),
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=True,
        )
        session.commit()

    resolved_messages = [args[1] for args, _ in send_calls if "RESOLVED" in str(args[1])]
    assert resolved_messages, "expected resolved notification message"
    assert "so_du_hien_tai=70,000₫" in resolved_messages[-1]
    assert "so_du_sau_phuc_hoi=70,000₫" in resolved_messages[-1]


def test_resolved_notification_suppressed_on_day_boundary_for_day_scoped_incident(
    monkeypatch, tmp_path
) -> None:
    _setup_db(tmp_path, monkeypatch)
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 2, 0, 7, tzinfo=tz)
    opened = datetime(2026, 2, 1, 23, 30, tzinfo=tz)

    send_calls = []

    def fake_send(*args, **kwargs) -> None:
        send_calls.append((args, kwargs))

    monkeypatch.setattr("dotori_shopee_automation.ads.alerts.discord_send", fake_send)

    with SessionLocal() as session:
        session.add(
            AdsIncident(
                shop_key="shop_a",
                incident_type="health_no_impressions",
                entity_type="campaign",
                entity_id="SHOP_TOTAL",
                severity="WARN",
                status="OPEN",
                title="No new impressions in last 60m",
                message="No new impressions in last 60m",
                first_seen_at=opened,
                last_seen_at=opened,
                last_notified_at=opened,
            )
        )
        session.commit()

    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now,
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=True,
            notify_resolved=True,
        )
        session.commit()

    assert counts["resolved"] == 1
    assert counts["notified"] == 0
    assert len(send_calls) == 0


def test_spend_spike_is_suppressed_when_overspend_incident_is_open(monkeypatch, tmp_path) -> None:
    _setup_db(tmp_path, monkeypatch)
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 2, 16, 0, tzinfo=tz)
    t1 = now - timedelta(minutes=60)

    with SessionLocal() as session:
        _seed_campaign(session, "shop_a", "cmp_1", Decimal("100000"))
        _seed_snapshot(session, "shop_a", "cmp_1", t1, Decimal("20000"), 1000, 30, 2, Decimal("200000"))
        _seed_snapshot(session, "shop_a", "cmp_1", now, Decimal("70000"), 1300, 42, 2, Decimal("200000"))
        session.add(
            AdsIncident(
                shop_key="shop_a",
                incident_type="pacing_overspend",
                entity_type="campaign",
                entity_id="cmp_1",
                severity="WARN",
                status="OPEN",
                title="Toc do pacing vuot nhanh",
                message="Toc do pacing vuot nhanh",
                first_seen_at=t1,
                last_seen_at=t1,
                last_notified_at=t1,
            )
        )
        session.commit()

    with SessionLocal() as session:
        counts = process_alerts(
            shop_key="shop_a",
            now=now,
            session=session,
            shop_label="SHOP_A",
            webhook_url=None,
            cooldown_minutes=120,
            send_discord=False,
            notify_resolved=False,
        )
        session.commit()

    assert counts["overlap_suppressed"] >= 1
    with SessionLocal() as session:
        spike_open = (
            session.query(AdsIncident)
            .filter_by(
                shop_key="shop_a",
                incident_type="spend_spike_60m",
                entity_type="campaign",
                entity_id="cmp_1",
                status="OPEN",
            )
            .count()
        )
    assert spike_open == 0

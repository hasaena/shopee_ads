from __future__ import annotations

from datetime import date, datetime, timedelta, time, timezone
from contextlib import redirect_stderr, redirect_stdout
from collections import Counter
import csv
from decimal import Decimal
import io
import os
import sys
import hashlib
import sqlite3
import json
import zipfile
import click
import re
import time as time_module
from pathlib import Path
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit
from typing import Any
import typer
import uvicorn
from sqlalchemy import func

from .ads.provider_mock_csv import MockCsvProvider
from .ads.alerts import process_alerts
from .ads.incidents import AdsIncident
from .ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from .ads.reporting import (
    build_discord_summary,
    render_daily_html,
    write_report_file,
    aggregate_daily_report,
)
from .ads.reconcile import run_report_reconcile
from .ads.weekly_report import (
    build_weekly_discord_message,
    build_weekly_payload,
    get_last_week_range,
    render_weekly_html,
    week_id as weekly_id,
    write_weekly_report_file,
)
from .ads.service import ingest_daily, ingest_snapshot, summary_daily
from .ads.campaign_probe import run_campaign_probe, run_gms_probe
from .ads.provider_live_plan import (
    ingest_ads_live,
    _call_ads_daily_with_fallback,
    _fetch_campaign_daily_breakdown_payload,
    _normalize_ads_daily_payload,
)
from .config import get_settings, load_shops, resolve_timezone
from .discord_notifier import build_report_url as _discord_build_report_url, send
from .db import EventLog, SessionLocal, init_db
from .scheduler import (
    build_scheduler,
    run_alerts_job,
    run_daily_report_job,
    run_weekly_report_job,
    _inspect_html_report,
    phase1_schedule_run_once as _phase1_schedule_run_once,
    phase1_alerts_run_once as _phase1_alerts_run_once,
)
from .shopee.auth import (
    build_auth_partner_url,
    exchange_code_for_token,
    refresh_access_token,
)
from .shopee.client import ShopeeClient
from .shopee.redact import redact_secrets, redact_text
from .shopee.token_store import get_token, needs_refresh, upsert_token
from .shopee.plan import (
    build_builtin_vars,
    interpolate_data,
    load_plan,
    safe_name,
    safe_path,
)
from .shopee.signing import build_sign_base, sign_hmac_sha256_hex
from .shopee.plan_runner import run_plan_for_shops
from .shopee.probe_analyzer import (
    analyze_artifacts,
    build_discord_summary,
    render_console_list,
    write_csv_summary,
    write_markdown_summary,
)
from .shopee.summary_links import build_summary_ref
from .shopee.probe_suite import run_probe_suite
from .token_preflight_gate import (
    emit_token_resolved_alerts_with_cooldown,
    emit_token_ttl_alerts_with_cooldown,
    evaluate_token_preflight_gate,
    load_token_preflight_gate_status_snapshot,
    write_token_preflight_gate_artifacts,
)
from .utils.envfile import load_env_file
from .webapp import app as web_app, build_phase1_status_payload
from .ops.doctor_notify import run_doctor_notify_cycle, parse_min_severity

app = typer.Typer(help="Dotori Shopee automation CLI")
shops_app = typer.Typer(help="Shop config commands")
app.add_typer(shops_app, name="shops")
ads_app = typer.Typer(help="Ads data commands")
app.add_typer(ads_app, name="ads")
ads_live_app = typer.Typer(help="Live ads ingest commands")
ads_app.add_typer(ads_live_app, name="live")
ops_app = typer.Typer(help="Ops scheduler commands")
app.add_typer(ops_app, name="ops")
ops_check_app = typer.Typer(help="Ops connectivity checks")
ops_app.add_typer(ops_check_app, name="check")
ops_readiness_app = typer.Typer(help="Ops readiness checks")
ops_app.add_typer(ops_readiness_app, name="readiness")
ops_phase1_app = typer.Typer(help="Phase 1 verification")
ops_app.add_typer(ops_phase1_app, name="phase1")
ops_phase1_ads_app = typer.Typer(help="Phase 1 ads probes")
ops_phase1_app.add_typer(ops_phase1_ads_app, name="ads")
ops_phase1_token_db_app = typer.Typer(help="Phase 1 token DB helpers")
ops_phase1_app.add_typer(ops_phase1_token_db_app, name="token-db")
ops_phase1_db_app = typer.Typer(help="Phase 1 DB helpers")
ops_phase1_app.add_typer(ops_phase1_db_app, name="db")
ops_phase1_ads_endpoint_app = typer.Typer(help="Phase 1 ads endpoint helpers")
ops_phase1_app.add_typer(ops_phase1_ads_endpoint_app, name="ads-endpoint")
ops_phase1_env_app = typer.Typer(help="Phase 1 env helpers")
ops_phase1_app.add_typer(ops_phase1_env_app, name="env")
ops_phase1_auth_app = typer.Typer(help="Phase 1 auth helpers")
ops_phase1_app.add_typer(ops_phase1_auth_app, name="auth")
ops_phase1_baseline_app = typer.Typer(help="Phase 1 baseline helpers")
ops_phase1_app.add_typer(ops_phase1_baseline_app, name="baseline")
ops_phase1_token_app = typer.Typer(help="Phase 1 token helpers")
ops_phase1_app.add_typer(ops_phase1_token_app, name="token")
ops_phase1_token_appsscript_app = typer.Typer(help="Apps Script token bridge")
ops_phase1_token_app.add_typer(ops_phase1_token_appsscript_app, name="appsscript")


def _print_captured_output(text: str) -> None:
    """Print captured subprocess output safely under non-UTF8 terminals."""
    if not text:
        return
    encoding = (getattr(sys.stdout, "encoding", None) or "utf-8").strip() or "utf-8"
    safe = str(text).encode(encoding, errors="backslashreplace").decode(encoding, errors="replace")
    print(safe, end="")
ops_phase1_artifacts_app = typer.Typer(help="Phase 1 artifacts helpers")
ops_phase1_app.add_typer(ops_phase1_artifacts_app, name="artifacts")
ops_phase1_evidence_app = typer.Typer(help="Phase 1 evidence runner")
ops_phase1_app.add_typer(ops_phase1_evidence_app, name="evidence")
ops_phase1_schedule_app = typer.Typer(help="Phase 1 schedule run-once harness")
ops_phase1_app.add_typer(ops_phase1_schedule_app, name="schedule")
ops_phase1_alerts_app = typer.Typer(help="Phase 1 alerts run-once harness")
ops_phase1_app.add_typer(ops_phase1_alerts_app, name="alerts")
ops_phase1_go_live_app = typer.Typer(help="Phase 1 go-live rehearsal helpers")
ops_phase1_app.add_typer(ops_phase1_go_live_app, name="go-live")
ops_phase1_reports_app = typer.Typer(help="Phase 1 reports helpers")
ops_phase1_app.add_typer(ops_phase1_reports_app, name="reports")
# Alias for shorter command path: ops phase1 report ...
ops_phase1_app.add_typer(ops_phase1_reports_app, name="report")
ops_phase1_export_app = typer.Typer(help="Phase 1 export helpers")
ops_phase1_app.add_typer(ops_phase1_export_app, name="export")
ops_phase1_status_app = typer.Typer(help="Phase 1 status tooling")
ops_phase1_app.add_typer(ops_phase1_status_app, name="status")
shopee_app = typer.Typer(help="Shopee auth commands")
app.add_typer(shopee_app, name="shopee")


@app.command()
def health() -> None:
    """Print a simple health check."""
    print("ok")


@app.command("discord-test")
def discord_test(
    channel: str = typer.Option(..., help="report | alerts | actions"),
    text: str = typer.Option(..., help="Message text"),
    shop: str | None = typer.Option(None, help="Shop key (optional)"),
) -> None:
    channel = channel.lower().strip()
    if channel not in {"report", "alerts", "actions"}:
        raise typer.BadParameter("channel must be one of: report, alerts, actions")
    shop_label = None
    webhook_url = None
    if shop:
        match = _get_shop_or_exit(shop)
        shop_label = match.label
        webhook_url = match.discord_webhook_url
    send(channel, text, shop_label=shop_label, webhook_url=webhook_url)


@app.command()
def web() -> None:
    """Run the FastAPI web server."""
    settings = get_settings()
    uvicorn.run(web_app, host=settings.web_host, port=settings.web_port)


def _load_shops_or_exit():
    try:
        return load_shops()
    except Exception as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


def _get_shop_or_exit(shop_key: str):
    shops = _load_shops_or_exit()
    match = next((item for item in shops if item.shop_key == shop_key), None)
    if not match:
        raise typer.BadParameter(f"unknown shop_key: {shop_key}")
    return match


@shops_app.command("validate")
def shops_validate() -> None:
    _load_shops_or_exit()
    print("ok")


@shops_app.command("list")
def shops_list() -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    enabled = [shop for shop in shops if shop.enabled]
    if not enabled:
        print("no enabled shops")
        return
    for shop in enabled:
        webhook_present = bool(shop.discord_webhook_url or settings.discord_webhook_report_url)
        webhook_text = "yes" if webhook_present else "no"
        print(f"{shop.shop_key}\t{shop.label}\t{shop.timezone}\twebhook={webhook_text}")


@ads_app.command("ingest-mock-daily")
def ads_ingest_mock_daily(
    shop: str = typer.Option(..., help="Shop key"),
    csv: str = typer.Option(..., help="CSV path"),
) -> None:
    shop_cfg = _get_shop_or_exit(shop)
    provider = MockCsvProvider(daily_csv=Path(csv), timezone=shop_cfg.timezone)
    summary = ingest_daily(shop, provider)
    print(
        f"ingested campaigns={summary['campaigns']} daily={summary['daily']} snapshots={summary['snapshots']}"
    )


@ads_app.command("ingest-mock-snapshot")
def ads_ingest_mock_snapshot(
    shop: str = typer.Option(..., help="Shop key"),
    csv: str = typer.Option(..., help="CSV path"),
) -> None:
    shop_cfg = _get_shop_or_exit(shop)
    provider = MockCsvProvider(snapshot_csv=Path(csv), timezone=shop_cfg.timezone)
    summary = ingest_snapshot(shop, provider)
    print(
        f"ingested campaigns={summary['campaigns']} daily={summary['daily']} snapshots={summary['snapshots']}"
    )


@ads_app.command("summary-daily")
def ads_summary_daily(
    shop: str = typer.Option(..., help="Shop key"),
    date_str: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
) -> None:
    _get_shop_or_exit(shop)
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError as exc:
        raise typer.BadParameter("date must be YYYY-MM-DD") from exc
    summary = summary_daily(shop, target_date)
    totals = summary["totals"]
    print(f"Totals for {shop} on {target_date.isoformat()}:")
    print(
        f"spend={totals['spend']} impressions={totals['impressions']} "
        f"clicks={totals['clicks']} orders={totals['orders']} gmv={totals['gmv']}"
    )
    print("Top 5 campaigns by spend:")
    if not summary["top_campaigns"]:
        print("(no data)")
        return
    for idx, row in enumerate(summary["top_campaigns"], start=1):
        print(f"{idx}. {row['campaign_id']} | {row['campaign_name']} | spend={row['spend']}")


@ads_live_app.command("ingest")
def ads_live_ingest(
    shop: str = typer.Option(..., help="Shop key"),
    target_date: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    plan: str = typer.Option(
        "collaboration/plans/ads_probe.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml", "--mapping", help="Mapping YAML path"
    ),
    transport: str = typer.Option("live", "--transport", help="live | fixtures"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    save_artifacts: bool = typer.Option(
        False, "--save-artifacts/--no-save-artifacts"
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
    strict_mapping: bool = typer.Option(False, "--strict-mapping"),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
) -> None:
    settings = get_settings()
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")
    date_value = _parse_date_or_today(target_date)
    transport_value = transport.lower().strip()
    if transport_value not in {"live", "fixtures"}:
        raise typer.BadParameter("transport must be live or fixtures")
    fixtures_path = Path(fixtures_dir) if transport_value == "fixtures" else None
    if fixtures_path and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")
    if not dry_run and transport_value == "live":
        _require_shopee_settings(settings)
    try:
        summary = ingest_ads_live(
            shop_cfg=shop_cfg,
            settings=settings,
            target_date=date_value,
            plan_path=Path(plan),
            mapping_path=Path(mapping),
            save_artifacts=save_artifacts,
            dry_run=dry_run,
            strict_mapping=strict_mapping,
            fixtures_dir=fixtures_path,
            client_factory=_build_shopee_client,
        )
    except ValueError as exc:
        print(f"error={exc}")
        raise typer.Exit(code=1)
    print(
        f"shop={shop_cfg.shop_key} date={date_value.isoformat()} "
        f"calls_ok={summary.calls_ok} calls_fail={summary.calls_fail}"
    )
    print(
        f"upserted campaigns={summary.campaigns} daily={summary.daily} "
        f"snapshots={summary.snapshots}"
    )


@ads_live_app.command("ingest-all")
def ads_live_ingest_all(
    target_date: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    plan: str = typer.Option(
        "collaboration/plans/ads_probe.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml", "--mapping", help="Mapping YAML path"
    ),
    transport: str = typer.Option("live", "--transport", help="live | fixtures"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    save_artifacts: bool = typer.Option(
        False, "--save-artifacts/--no-save-artifacts"
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
    strict_mapping: bool = typer.Option(False, "--strict-mapping"),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Comma-separated shop keys"
    ),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
) -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    target_shops = _select_shops(shops, only_shops)
    if not target_shops:
        print("no enabled shops")
        return
    _ensure_shop_ids(target_shops)
    date_value = _parse_date_or_today(target_date)
    transport_value = transport.lower().strip()
    if transport_value not in {"live", "fixtures"}:
        raise typer.BadParameter("transport must be live or fixtures")
    fixtures_path = Path(fixtures_dir) if transport_value == "fixtures" else None
    if fixtures_path and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")
    if not dry_run and transport_value == "live":
        _require_shopee_settings(settings)
    totals = {"calls_ok": 0, "calls_fail": 0, "campaigns": 0, "daily": 0, "snapshots": 0}
    for shop_cfg in target_shops:
        try:
            summary = ingest_ads_live(
                shop_cfg=shop_cfg,
                settings=settings,
                target_date=date_value,
                plan_path=Path(plan),
                mapping_path=Path(mapping),
                save_artifacts=save_artifacts,
                dry_run=dry_run,
                strict_mapping=strict_mapping,
                fixtures_dir=fixtures_path,
                client_factory=_build_shopee_client,
            )
        except ValueError as exc:
            print(f"error={exc}")
            raise typer.Exit(code=1)
        totals["calls_ok"] += summary.calls_ok
        totals["calls_fail"] += summary.calls_fail
        totals["campaigns"] += summary.campaigns
        totals["daily"] += summary.daily
        totals["snapshots"] += summary.snapshots
        print(
            f"shop={shop_cfg.shop_key} date={date_value.isoformat()} "
            f"calls_ok={summary.calls_ok} calls_fail={summary.calls_fail}"
        )
        print(
            f"upserted campaigns={summary.campaigns} daily={summary.daily} "
            f"snapshots={summary.snapshots}"
        )
    print(
        "total "
        f"calls_ok={totals['calls_ok']} calls_fail={totals['calls_fail']} "
        f"campaigns={totals['campaigns']} daily={totals['daily']} snapshots={totals['snapshots']}"
    )


@ads_app.command("report-daily")
def ads_report_daily(
    shop: str = typer.Option(..., help="Shop key"),
    kind: str = typer.Option(..., help="final | midday"),
    date_str: str | None = typer.Option(None, "--date", help="YYYY-MM-DD"),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
    write_html: bool = typer.Option(True, "--write-html/--no-write-html"),
    as_of: str | None = typer.Option(None, help="ISO datetime for midday cutoff"),
) -> None:
    kind = kind.lower().strip()
    if kind not in {"final", "midday"}:
        raise typer.BadParameter("kind must be one of: final, midday")

    settings = get_settings()
    shop_cfg = _get_shop_or_exit(shop)
    tz = resolve_timezone(shop_cfg.timezone or settings.timezone)
    now = datetime.now(tz)

    if date_str:
        try:
            target_date = date.fromisoformat(date_str)
        except ValueError as exc:
            raise typer.BadParameter("date must be YYYY-MM-DD") from exc
    else:
        target_date = now.date() - timedelta(days=1) if kind == "final" else now.date()

    as_of_dt = None
    if kind == "midday":
        if as_of:
            parsed = datetime.fromisoformat(as_of)
            as_of_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)
        else:
            as_of_dt = now

    init_db()
    output_path = None
    report_url = None
    session = SessionLocal()
    try:
        data = aggregate_daily_report(session, shop, target_date, as_of_dt)
        data.update(
            {
                "shop_label": shop_cfg.label,
                "kind": kind,
                "generated_at": now,
            }
        )
        html = render_daily_html(data)

        if write_html:
            output_path = write_report_file(shop, target_date, kind, html)
            report_url = _build_report_url(
                shop, target_date, kind, settings.report_access_token
            )

        if send_discord:
            summary = build_discord_summary(data, report_url)
            send(
                "report",
                summary,
                shop_label=shop_cfg.label,
                webhook_url=shop_cfg.discord_webhook_url,
            )

        meta = {
            "shop_key": shop,
            "kind": kind,
            "date": target_date.isoformat(),
            "as_of": as_of_dt.isoformat() if as_of_dt else None,
            "output_path": str(output_path) if output_path else None,
            "totals": data["totals"],
            "provider": "unknown",
        }
        session.add(
            EventLog(
                level="INFO",
                message="ads_daily_report_generated",
                meta_json=_safe_json(meta),
            )
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        session.add(
            EventLog(
                level="ERROR",
                message="ads_daily_report_failed",
                meta_json=_safe_json({"shop_key": shop, "error": str(exc)}),
            )
        )
        session.commit()
        raise
    finally:
        session.close()

    if output_path:
        print(f"report_written={output_path}")
    totals = data["totals"]
    print(
        f"totals spend={_fmt_decimal(totals['spend'])} impressions={totals['impressions']} "
        f"clicks={totals['clicks']} orders={totals['orders']} gmv={_fmt_decimal(totals['gmv'])}"
    )


@ads_app.command("detect-alerts")
def ads_detect_alerts(
    shop: str = typer.Option(..., help="Shop key"),
    now: str | None = typer.Option(None, help="ISO datetime, optional"),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
) -> None:
    shop_cfg = _get_shop_or_exit(shop)
    settings = get_settings()
    tz = resolve_timezone(shop_cfg.timezone or settings.timezone)
    now_dt = datetime.now(tz)
    if now:
        parsed = datetime.fromisoformat(now)
        now_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)

    init_db()
    session = SessionLocal()
    try:
        counts = process_alerts(
            shop_key=shop,
            now=now_dt,
            session=session,
            shop_label=shop_cfg.label,
            webhook_url=shop_cfg.discord_webhook_url,
            cooldown_minutes=settings.alert_cooldown_minutes,
            send_discord=send_discord,
            notify_resolved=True,
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    print(
        f"active={counts['active']} opened={counts['opened']} "
        f"resolved={counts['resolved']} notified={counts['notified']}"
    )


@ads_app.command("detect-alerts-all")
def ads_detect_alerts_all(
    now: str | None = typer.Option(None, help="ISO datetime, optional"),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
) -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    enabled = [shop for shop in shops if shop.enabled]
    if not enabled:
        print("no enabled shops")
        return

    total = {"active": 0, "opened": 0, "resolved": 0, "notified": 0}
    for shop in enabled:
        tz = resolve_timezone(shop.timezone or settings.timezone)
        now_dt = datetime.now(tz)
        if now:
            parsed = datetime.fromisoformat(now)
            now_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)

        init_db()
        session = SessionLocal()
        try:
            counts = process_alerts(
                shop_key=shop.shop_key,
                now=now_dt,
                session=session,
                shop_label=shop.label,
                webhook_url=shop.discord_webhook_url,
                cooldown_minutes=settings.alert_cooldown_minutes,
                send_discord=send_discord,
                notify_resolved=True,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        for key in total:
            total[key] += counts[key]

    print(
        f"active={total['active']} opened={total['opened']} "
        f"resolved={total['resolved']} notified={total['notified']}"
    )


@ads_app.command("report-weekly")
def ads_report_weekly(
    shop: str = typer.Option(..., help="Shop key"),
    now: str | None = typer.Option(None, help="ISO datetime, optional"),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
) -> None:
    settings = get_settings()
    shop_cfg = _get_shop_or_exit(shop)
    tz = resolve_timezone(shop_cfg.timezone or settings.timezone)
    now_dt = datetime.now(tz)
    if now:
        parsed = datetime.fromisoformat(now)
        now_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)

    start_date, end_date = get_last_week_range(now_dt, tz)
    week_id_value = weekly_id(start_date)

    init_db()
    session = SessionLocal()
    try:
        payload = build_weekly_payload(session, shop, start_date, end_date)
        html = render_weekly_html(shop_cfg.label, week_id_value, start_date, end_date, payload)
        output_path = write_weekly_report_file(shop, week_id_value, html)

        report_url = _build_weekly_report_url(
            shop, week_id_value, settings.report_access_token
        )

        if send_discord:
            message = build_weekly_discord_message(
                shop_cfg.label,
                week_id_value,
                payload["metrics"],
                payload.get("wow_delta"),
                report_url,
            )
            send(
                "report",
                message,
                shop_label=shop_cfg.label,
                webhook_url=shop_cfg.discord_webhook_url,
            )
        session.commit()
    finally:
        session.close()

    totals = payload["metrics"]["totals"]
    roas = payload["metrics"]["kpis"]["roas"]
    print(f"report_written={output_path}")
    print(
        f"totals spend={_fmt_decimal(totals['spend'])} orders={totals['orders']} "
        f"gmv={_fmt_decimal(totals['gmv'])} roas={_fmt_decimal(roas)}"
    )


@ads_app.command("report-weekly-all")
def ads_report_weekly_all(
    now: str | None = typer.Option(None, help="ISO datetime, optional"),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
) -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    enabled = [shop for shop in shops if shop.enabled]
    if not enabled:
        print("no enabled shops")
        return

    for shop_cfg in enabled:
        tz = resolve_timezone(shop_cfg.timezone or settings.timezone)
        now_dt = datetime.now(tz)
        if now:
            parsed = datetime.fromisoformat(now)
            now_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)

        start_date, end_date = get_last_week_range(now_dt, tz)
        week_id_value = weekly_id(start_date)

        init_db()
        session = SessionLocal()
        try:
            payload = build_weekly_payload(session, shop_cfg.shop_key, start_date, end_date)
            html = render_weekly_html(
                shop_cfg.label, week_id_value, start_date, end_date, payload
            )
            output_path = write_weekly_report_file(
                shop_cfg.shop_key, week_id_value, html
            )

            report_url = _build_weekly_report_url(
                shop_cfg.shop_key, week_id_value, settings.report_access_token
            )
            if send_discord:
                message = build_weekly_discord_message(
                    shop_cfg.label,
                    week_id_value,
                    payload["metrics"],
                    payload.get("wow_delta"),
                    report_url,
                )
                send(
                    "report",
                    message,
                    shop_label=shop_cfg.label,
                    webhook_url=shop_cfg.discord_webhook_url,
                )
            session.commit()
        finally:
            session.close()

        totals = payload["metrics"]["totals"]
        roas = payload["metrics"]["kpis"]["roas"]
        print(f"shop={shop_cfg.shop_key} report_written={output_path}")
        print(
            f"totals spend={_fmt_decimal(totals['spend'])} orders={totals['orders']} "
            f"gmv={_fmt_decimal(totals['gmv'])} roas={_fmt_decimal(roas)}"
        )


@ops_app.command("jobs")
def ops_jobs() -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    scheduler = build_scheduler(settings, shops, blocking=False)
    scheduler.start(paused=True)
    jobs = scheduler.get_jobs()
    if not jobs:
        print("no jobs registered")
        scheduler.shutdown(wait=False)
        return
    for job in jobs:
        next_run = job.next_run_time.isoformat() if job.next_run_time else "-"
        print(f"{job.id}\t{next_run}")
    scheduler.shutdown(wait=False)


@ops_app.command("scheduler")
def ops_scheduler() -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    scheduler = build_scheduler(settings, shops, blocking=True)
    scheduler.start()


@ops_app.command("run-once")
def ops_run_once(
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
    now: str | None = typer.Option(None, help="ISO datetime, optional"),
) -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    now_dt = datetime.now(tz)
    if now:
        parsed = datetime.fromisoformat(now)
        now_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)

    run_alerts_job(settings, shops, now=now_dt, send_discord=send_discord)
    run_daily_report_job(settings, shops, kind="final", now=now_dt, send_discord=send_discord)
    run_daily_report_job(settings, shops, kind="midday", now=now_dt, send_discord=send_discord)
    run_weekly_report_job(settings, shops, now=now_dt, send_discord=send_discord)
    print("run_once_complete")


@ops_app.command("run")
def ops_run() -> None:
    settings = get_settings()
    shops = _load_shops_or_exit()
    scheduler = build_scheduler(settings, shops, blocking=False)
    scheduler.start()
    uvicorn.run(web_app, host=settings.web_host, port=settings.web_port)


@ops_app.command("smoke")
def ops_smoke(
    mode: str | None = typer.Argument(
        None, help="probe | ads-live-fixtures"
    ),
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    date_value: str | None = typer.Option(None, "--date", help="YYYYMMDD"),
    plan: str | None = typer.Option(None, "--plan", help="Plan YAML path"),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml", "--mapping", help="Mapping YAML path"
    ),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    database: str = typer.Option(
        "sqlite:///./collaboration/task_017_smoke.db",
        "--database",
        help="Database URL override",
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    send_discord: bool = typer.Option(True, "--send-discord/--no-send-discord"),
    discord_channel: str = typer.Option(
        "report", "--discord-channel", help="report | alerts"
    ),
    live_http: bool = typer.Option(False, "--live-http/--no-live-http"),
) -> None:
    mode_value = (mode or "probe").strip().lower()
    if mode_value not in {"probe", "ads-live-fixtures"}:
        raise typer.BadParameter("mode must be probe or ads-live-fixtures")

    if mode_value == "ads-live-fixtures":
        _ops_smoke_ads_live_fixtures(
            only_shops=only_shops,
            date_value=date_value,
            plan=plan,
            mapping=mapping,
            transport=transport,
            fixtures_dir=fixtures_dir,
            database=database,
            reports_dir=reports_dir,
            send_discord=send_discord,
        )
        return

    settings = get_settings()
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now_dt = datetime.now(tz)
    date_value = date_value or now_dt.strftime("%Y%m%d")
    channel_value = discord_channel.lower().strip()
    if channel_value not in {"report", "alerts"}:
        raise typer.BadParameter("discord-channel must be report or alerts")
    plan_value = plan or "collaboration/plans/shop_info.yaml"

    try:
        shops = _load_shops_or_exit()
        target_shops = _select_shops(shops, only_shops)
        if not target_shops:
            print("no enabled shops")
            return
        _ensure_shop_ids(target_shops)

        print(
            "smoke_start "
            f"ts={now_dt.isoformat()} tz={tz} date={date_value} "
            f"plan_path={plan_value} live_http={live_http}"
        )
        print(f"target_shops={','.join([shop.shop_key for shop in target_shops])}")

        enabled = [shop for shop in shops if shop.enabled]
        if enabled:
            for shop in enabled:
                webhook_flag = 1 if shop.discord_webhook_url else 0
                print(f"enabled_shop={shop.shop_key} webhook_configured={webhook_flag}")
        else:
            print("enabled_shops=none")

        report_hook = settings.discord_webhook_report_url
        alerts_hook = settings.discord_webhook_alerts_url
        print(
            "discord_webhooks "
            f"report_configured={1 if report_hook else 0} "
            f"alerts_configured={1 if alerts_hook else 0}"
        )
        print(
            "shopee_credentials "
            f"partner_id_present={1 if settings.shopee_partner_id else 0} "
            f"partner_key_present={1 if settings.shopee_partner_key else 0}"
        )

        init_db()
        session = SessionLocal()
        try:
            for shop in target_shops:
                token_present = get_token(session, shop.shop_key) is not None
                print(f"token_present shop={shop.shop_key} value={1 if token_present else 0}")
        finally:
            session.close()

        if live_http:
            _require_shopee_settings(settings)

        if send_discord:
            _send_smoke_start(settings, channel_value, date_value, target_shops, live_http)

        result = run_probe_suite(
            settings=settings,
            shops=target_shops,
            plan_path=Path(plan_value),
            date_value=date_value,
            user_vars={},
            save_root=Path("collaboration") / "artifacts" / "shopee_api",
            out_dir=Path("collaboration")
            / "outputs"
            / "probe_summaries"
            / date_value,
            dry_run=not live_http,
            send_discord=False,
            channel=channel_value,
            client_factory=_build_shopee_client,
        )

        md_path = result.get("md_path", "-")
        csv_path = result.get("csv_path", "-")
        print(
            f"smoke_done ok={result.get('ok')} fail={result.get('fail')} "
            f"md={md_path} csv={csv_path}"
        )

        if send_discord:
            _send_smoke_done(settings, result, md_path, csv_path)
    except Exception as exc:  # noqa: BLE001
        error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
        print(f"smoke_error={error_text}")
        _send_smoke_error(settings, error_text)
        raise typer.Exit(code=1)


def _ops_smoke_ads_live_fixtures(
    *,
    only_shops: str,
    date_value: str | None,
    plan: str | None,
    mapping: str,
    transport: str,
    fixtures_dir: str,
    database: str,
    reports_dir: str,
    send_discord: bool,
) -> None:
    ctx = click.get_current_context(silent=True)
    target_date = _parse_required_date(date_value)
    transport_value = transport.lower().strip()
    if transport_value != "fixtures":
        raise typer.BadParameter("transport must be fixtures for this smoke")

    fixtures_path = Path(fixtures_dir)
    if not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    plan_value = plan or "collaboration/plans/ads_ingest_minimal.yaml"
    mapping_value = mapping or "collaboration/mappings/ads_mapping.yaml"

    db_url = _resolve_env_override(
        ctx, "database", "DATABASE_URL", database, database
    )
    reports_root = _resolve_env_override(
        ctx, "reports_dir", "REPORTS_DIR", reports_dir, reports_dir
    )
    os.environ["DATABASE_URL"] = db_url
    os.environ["REPORTS_DIR"] = reports_root
    get_settings.cache_clear()

    settings = get_settings()
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now_dt = datetime.now(tz)
    send_alerts = send_discord
    if _option_is_default(ctx, "send_discord"):
        send_alerts = False

    shops = _load_shops_or_exit()
    target_shops = _select_shops(shops, only_shops)
    if not target_shops:
        print("no enabled shops")
        return
    _ensure_shop_ids(target_shops)

    print(
        "smoke_ads_live_start "
        f"date={target_date.isoformat()} shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} fixtures_dir={fixtures_path} "
        f"plan_path={plan_value} mapping_path={mapping_value}"
    )

    totals = {"calls_ok": 0, "calls_fail": 0, "campaigns": 0, "daily": 0, "snapshots": 0}
    for shop_cfg in target_shops:
        summary = ingest_ads_live(
            shop_cfg=shop_cfg,
            settings=settings,
            target_date=target_date,
            plan_path=Path(plan_value),
            mapping_path=Path(mapping_value),
            save_artifacts=False,
            dry_run=False,
            strict_mapping=False,
            fixtures_dir=fixtures_path,
            client_factory=_build_shopee_client,
        )
        totals["calls_ok"] += summary.calls_ok
        totals["calls_fail"] += summary.calls_fail
        totals["campaigns"] += summary.campaigns
        totals["daily"] += summary.daily
        totals["snapshots"] += summary.snapshots
        print(
            f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
            f"calls_ok={summary.calls_ok} calls_fail={summary.calls_fail}"
        )
        print(
            f"upserted campaigns={summary.campaigns} daily={summary.daily} "
            f"snapshots={summary.snapshots}"
        )
    print(
        "total "
        f"calls_ok={totals['calls_ok']} calls_fail={totals['calls_fail']} "
        f"campaigns={totals['campaigns']} daily={totals['daily']} snapshots={totals['snapshots']}"
    )

    report_paths: list[Path] = []
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            data = aggregate_daily_report(
                session,
                shop_cfg.shop_key,
                target_date,
                as_of=None,
            )
            data.update(
                {
                    "shop_label": shop_cfg.label,
                    "kind": "final",
                    "generated_at": now_dt,
                }
            )
            html = render_daily_html(data)
            report_path = write_report_file(
                shop_cfg.shop_key, target_date, "final", html
            )
            report_paths.append(report_path)
            print(f"report_path shop={shop_cfg.shop_key} path={report_path}")
    finally:
        session.close()

    alert_now = datetime.combine(target_date, time(12, 0), tzinfo=tz)
    alerts_total = {"active": 0, "opened": 0, "resolved": 0, "notified": 0}
    for shop_cfg in target_shops:
        session = SessionLocal()
        try:
            counts = process_alerts(
                shop_key=shop_cfg.shop_key,
                now=alert_now,
                session=session,
                shop_label=shop_cfg.label,
                webhook_url=shop_cfg.discord_webhook_url,
                cooldown_minutes=settings.alert_cooldown_minutes,
                send_discord=send_alerts,
                notify_resolved=True,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        print(
            f"alerts shop={shop_cfg.shop_key} active={counts['active']} "
            f"opened={counts['opened']} resolved={counts['resolved']} notified={counts['notified']}"
        )
        for key in alerts_total:
            alerts_total[key] += counts[key]

    print(
        "alerts_total "
        f"active={alerts_total['active']} opened={alerts_total['opened']} "
        f"resolved={alerts_total['resolved']} notified={alerts_total['notified']}"
    )
    print("smoke_ok=1")


@ops_check_app.command("discord")
def ops_check_discord(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    dry_run: bool = typer.Option(True, "--dry-run/--send"),
    channel: str = typer.Option("both", "--channel", help="report | alerts | both"),
) -> None:
    settings = get_settings()
    channel_value = channel.lower().strip()
    if channel_value not in {"report", "alerts", "both"}:
        raise typer.BadParameter("channel must be report, alerts, or both")
    channels = ["report", "alerts"] if channel_value == "both" else [channel_value]

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        return

    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now_dt = datetime.now(tz)
    timestamp = now_dt.strftime("%Y-%m-%d %H:%M %Z")

    missing: list[tuple[str, str]] = []
    messages: list[tuple[str, str, str, str | None]] = []
    for shop_cfg in target_shops:
        message = (
            f"[{shop_cfg.label}][ACTION] Discord webhook check OK ({timestamp})"
        )
        for chan in channels:
            webhook = shop_cfg.discord_webhook_url
            if webhook is None:
                webhook = (
                    settings.discord_webhook_report_url
                    if chan == "report"
                    else settings.discord_webhook_alerts_url
                )
            configured = 1 if webhook else 0
            print(
                f"discord_webhook shop={shop_cfg.shop_key} channel={chan} "
                f"configured={configured}"
            )
            if not webhook:
                missing.append((shop_cfg.shop_key, chan))
            messages.append((shop_cfg.shop_key, shop_cfg.label, chan, webhook))

        if dry_run:
            if len(channels) > 1:
                channel_label = ",".join(channels)
                print(
                    f"discord_dry_run shop={shop_cfg.shop_key} "
                    f"channels={channel_label} message={message}"
                )
            else:
                print(
                    f"discord_dry_run shop={shop_cfg.shop_key} "
                    f"channel={channels[0]} message={message}"
                )

    if dry_run:
        return

    if missing:
        for shop_key, chan in missing:
            print(
                f"error=webhook_not_configured shop={shop_key} channel={chan}"
            )
        raise typer.Exit(code=2)

    had_error = False
    for shop_key, shop_label, chan, webhook in messages:
        if not webhook:
            continue
        message = (
            f"[{shop_label}][ACTION] Discord webhook check OK ({timestamp})"
        )
        try:
            send(
                chan,
                message,
                shop_label=shop_label,
                webhook_url=webhook,
            )
            print(f"discord_send_ok=1 shop={shop_key} channel={chan}")
        except Exception as exc:  # noqa: BLE001
            had_error = True
            error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
            print(
                f"discord_send_ok=0 shop={shop_key} channel={chan} error={error_text}"
            )
    if had_error:
        raise typer.Exit(code=1)


@ops_check_app.command("shopee-ping")
def ops_check_shopee_ping(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_mode: str = typer.Option(
        "passive", "--token-mode", help="default | passive"
    ),
    dry_run: bool = typer.Option(False, "--dry-run/--send"),
    allow_network: bool = typer.Option(False, "--allow-network"),
) -> bool:
    env_file = _coerce_option_value(env_file, None)
    token_file = _coerce_option_value(token_file, None)
    token_mode = _coerce_option_value(token_mode, "passive")
    _maybe_load_env_file(env_file)
    _ = _normalize_token_mode(token_mode)
    settings = get_settings()
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    fixtures_path = Path(fixtures_dir)
    if transport_value == "fixtures" and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if transport_value == "live" and not (allow_network or allow_network_env):
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        return
    _ensure_shop_ids(target_shops)

    if token_file:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        print(
            "token_sync_from_file_ok=1 "
            f"shops={','.join([shop.shop_key for shop in target_shops])}"
        )
        _print_db_token_fingerprints(target_shops)

    dry_run_effective = dry_run
    if transport_value == "live" and not dry_run_effective:
        if settings.shopee_partner_id is None or not settings.shopee_partner_key:
            print("ping_dry_run reason=missing_credentials")
            dry_run_effective = True

    if transport_value == "live" and not dry_run_effective:
        _require_shopee_settings(settings)

    if transport_value == "live":
        init_db()

    all_ok = True
    for shop_cfg in target_shops:
        if transport_value == "fixtures":
            if dry_run_effective:
                print(
                    f"shop={shop_cfg.shop_key} ping_ok=0 error=dry_run"
                )
                all_ok = False
                continue
            payload = _load_fixture_payload(fixtures_path, "shop_info")
            if payload is None:
                print(
                    f"shop={shop_cfg.shop_key} ping_ok=0 error=fixture_missing"
                )
                all_ok = False
                continue
            error_code = payload.get("error")
            if error_code not in (None, 0):
                print(
                    f"shop={shop_cfg.shop_key} ping_ok=0 error=shopee_error_{error_code}"
                )
                all_ok = False
                continue
            print(f"shop={shop_cfg.shop_key} ping_ok=1 error=-")
            continue

        if dry_run_effective:
            print(f"shop={shop_cfg.shop_key} ping_ok=0 error=dry_run")
            all_ok = False
            continue

        session = SessionLocal()
        try:
            token = get_token(session, shop_cfg.shop_key)
            if token is None:
                print(f"shop={shop_cfg.shop_key} ping_ok=0 error=no_token")
                all_ok = False
                continue
            if needs_refresh(token.access_token_expires_at):
                refreshed = refresh_access_token(
                    _build_shopee_client(settings),
                    settings.shopee_partner_id,
                    settings.shopee_partner_key,
                    shop_cfg.shopee_shop_id,
                    token.refresh_token,
                    int(datetime.now().timestamp()),
                )
                upsert_token(
                    session,
                    shop_cfg.shop_key,
                    refreshed.shop_id,
                    refreshed.access_token,
                    refreshed.refresh_token,
                    refreshed.access_expires_at,
                )
                session.commit()
                token = get_token(session, shop_cfg.shop_key)

            response = _build_shopee_client(settings).request(
                "GET",
                "/api/v2/shop/get_shop_info",
                shop_id=shop_cfg.shopee_shop_id,
                access_token=token.access_token,
            )
            if isinstance(response, dict):
                error_code = response.get("error")
                if error_code not in (None, 0):
                    print(
                        f"shop={shop_cfg.shop_key} ping_ok=0 error=shopee_error_{error_code}"
                    )
                    all_ok = False
                    continue
            print(f"shop={shop_cfg.shop_key} ping_ok=1 error=-")
        except Exception as exc:  # noqa: BLE001
            all_ok = False
            error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
            print(f"shop={shop_cfg.shop_key} ping_ok=0 error={error_text}")
        finally:
            session.close()

    if not all_ok:
        raise typer.Exit(code=1)
    return True


@ops_readiness_app.command("phase1")
def ops_readiness_phase1(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    lines, ready, missing = _compute_phase1_readiness(target_shops, settings)
    for line in lines:
        print(line)
    if not ready:
        raise typer.Exit(code=1)


def _compute_phase1_readiness(
    target_shops,
    settings,
    *,
    require_ads_endpoints: bool = False,
) -> tuple[list[str], bool, list[str]]:
    lines: list[str] = []
    missing: list[str] = []
    shop_list = ",".join([shop.shop_key for shop in target_shops])
    lines.append(
        f"readiness_phase1 shops={shop_list} timezone=Asia/Ho_Chi_Minh"
    )

    report_configured = bool(settings.discord_webhook_report_url)
    alerts_configured = bool(settings.discord_webhook_alerts_url)
    actions_configured = bool(settings.discord_webhook_actions_url)
    lines.append(
        "discord_webhooks "
        f"report={1 if report_configured else 0} "
        f"alerts={1 if alerts_configured else 0} "
        f"actions={1 if actions_configured else 0}"
    )

    partner_id_present = bool(settings.shopee_partner_id)
    partner_key_present = bool(settings.shopee_partner_key)
    lines.append(
        "shopee_credentials "
        f"partner_id={1 if partner_id_present else 0} "
        f"partner_key={1 if partner_key_present else 0}"
    )

    daily_ok, snapshot_ok, ads_missing = _ads_endpoint_status()
    lines.append(
        "ads_endpoints "
        f"daily_path={1 if daily_ok else 0} "
        f"snapshot_path={1 if snapshot_ok else 0}"
    )

    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            shop_id_configured = _shop_id_configured(shop_cfg)
            token = get_token(session, shop_cfg.shop_key)
            token_access = bool(token and token.access_token)
            token_refresh = bool(token and token.refresh_token)
            token_expires = bool(token and token.access_token_expires_at)
            lines.append(
                f"shop={shop_cfg.shop_key} "
                f"shop_id={1 if shop_id_configured else 0} "
                f"token_access={1 if token_access else 0} "
                f"token_refresh={1 if token_refresh else 0} "
                f"token_expires={1 if token_expires else 0}"
            )

            if not shop_id_configured:
                missing.append(f"{shop_cfg.shop_key}_shop_id")
            if not token_access:
                missing.append(f"{shop_cfg.shop_key}_token_access")
            if not token_refresh:
                missing.append(f"{shop_cfg.shop_key}_token_refresh")
    finally:
        session.close()

    if not report_configured:
        missing.append("discord_report_webhook")
    if not alerts_configured:
        missing.append("discord_alerts_webhook")
    if not partner_id_present:
        missing.append("shopee_partner_id")
    if not partner_key_present:
        missing.append("shopee_partner_key")
    if require_ads_endpoints:
        missing.extend(ads_missing)

    if missing:
        lines.append(f"ready=0 missing={','.join(missing)}")
        lines.append(
            "next_steps: set DISCORD_WEBHOOK_REPORT_URL/DISCORD_WEBHOOK_ALERTS_URL, "
            "set SHOPEE_PARTNER_ID/SHOPEE_PARTNER_KEY, run shopee exchange-code for samord/minmin, "
            "then ops check discord --send and ops check shopee-ping --transport live --allow-network"
        )
        return lines, False, missing

    lines.append("ready=1 missing=-")
    return lines, True, []


def _maybe_load_env_file(env_file: str | None) -> None:
    if not env_file:
        return
    try:
        loaded = load_env_file(env_file)
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    get_settings.cache_clear()
    print(f"env_file_loaded path={env_file} keys={len(loaded)}")


def _ads_endpoint_status() -> tuple[bool, bool, list[str]]:
    daily_path = os.environ.get("ADS_DAILY_PATH", "").strip()
    snapshot_path = os.environ.get("ADS_SNAPSHOT_PATH", "").strip()

    def is_configured(value: str) -> bool:
        if not value:
            return False
        return "TODO_REPLACE_ME" not in value.upper()

    daily_ok = is_configured(daily_path)
    snapshot_ok = is_configured(snapshot_path)
    missing: list[str] = []
    if not daily_ok:
        missing.append("ADS_DAILY_PATH")
    if not snapshot_ok:
        missing.append("ADS_SNAPSHOT_PATH")
    return daily_ok, snapshot_ok, missing


def _ads_snapshot_endpoint_status() -> tuple[bool, list[str]]:
    snapshot_path = os.environ.get("ADS_SNAPSHOT_PATH", "").strip()
    campaign_list_path = os.environ.get("ADS_CAMPAIGN_LIST_PATH", "").strip()

    def is_configured(value: str) -> bool:
        if not value:
            return False
        return "TODO_REPLACE_ME" not in value.upper()

    snapshot_ok = is_configured(snapshot_path)
    campaign_list_ok = is_configured(campaign_list_path)
    ok = snapshot_ok and campaign_list_ok
    missing: list[str] = []
    if not snapshot_ok:
        missing.append("ADS_SNAPSHOT_PATH")
    if not campaign_list_ok:
        missing.append("ADS_CAMPAIGN_LIST_PATH")
    return ok, missing


def _compute_budget_coverage(
    session,
    shop_key: str,
    shop_budget_override: Decimal | None = None,
) -> dict[str, object]:
    campaigns_total = (
        session.query(func.count(AdsCampaign.id))
        .filter(AdsCampaign.shop_key == shop_key)
        .scalar()
        or 0
    )
    with_budget = (
        session.query(func.count(AdsCampaign.id))
        .filter(AdsCampaign.shop_key == shop_key, AdsCampaign.daily_budget.isnot(None))
        .scalar()
        or 0
    )
    with_budget_pos = (
        session.query(func.count(AdsCampaign.id))
        .filter(
            AdsCampaign.shop_key == shop_key,
            AdsCampaign.daily_budget.isnot(None),
            AdsCampaign.daily_budget > 0,
        )
        .scalar()
        or 0
    )
    budget_min = (
        session.query(func.min(AdsCampaign.daily_budget))
        .filter(
            AdsCampaign.shop_key == shop_key,
            AdsCampaign.daily_budget.isnot(None),
            AdsCampaign.daily_budget > 0,
        )
        .scalar()
    )
    budget_max = (
        session.query(func.max(AdsCampaign.daily_budget))
        .filter(
            AdsCampaign.shop_key == shop_key,
            AdsCampaign.daily_budget.isnot(None),
            AdsCampaign.daily_budget > 0,
        )
        .scalar()
    )
    pct = 0.0
    if campaigns_total > 0:
        pct = float(with_budget_pos) / float(campaigns_total) * 100.0
    source = "campaign"
    effective_budget = _to_decimal(shop_budget_override)
    if with_budget_pos <= 0 and effective_budget is not None and effective_budget > 0:
        source = "override"
        with_budget = max(int(with_budget), 1)
        with_budget_pos = 1
        campaigns_total = max(int(campaigns_total), 1)
        budget_min = effective_budget
        budget_max = effective_budget
        pct = float(with_budget_pos) / float(campaigns_total) * 100.0
    return {
        "campaigns_total": int(campaigns_total),
        "with_budget": int(with_budget),
        "with_budget_pos": int(with_budget_pos),
        "pct": float(pct),
        "min": budget_min,
        "max": budget_max,
        "source": source,
    }


def _fmt_budget_value(value) -> str:
    if value is None:
        return "-"
    return str(value)


def _compute_snapshot_coverage(session, shop_key: str) -> dict[str, object]:
    snapshots_total = (
        session.query(func.count(AdsCampaignSnapshot.id))
        .filter(AdsCampaignSnapshot.shop_key == shop_key)
        .scalar()
        or 0
    )
    with_spend_pos = (
        session.query(func.count(AdsCampaignSnapshot.id))
        .filter(
            AdsCampaignSnapshot.shop_key == shop_key,
            AdsCampaignSnapshot.spend_today > 0,
        )
        .scalar()
        or 0
    )
    spend_min = (
        session.query(func.min(AdsCampaignSnapshot.spend_today))
        .filter(AdsCampaignSnapshot.shop_key == shop_key)
        .scalar()
    )
    spend_max = (
        session.query(func.max(AdsCampaignSnapshot.spend_today))
        .filter(AdsCampaignSnapshot.shop_key == shop_key)
        .scalar()
    )
    pct = 0.0
    if snapshots_total > 0:
        pct = float(with_spend_pos) / float(snapshots_total) * 100.0
    return {
        "snapshots_total": int(snapshots_total),
        "with_spend_pos": int(with_spend_pos),
        "pct_spend_pos": float(pct),
        "min": spend_min,
        "max": spend_max,
    }


def _normalize_token_mode(value: str) -> str:
    mode = value.lower().strip()
    if mode not in {"default", "passive"}:
        raise typer.BadParameter("token-mode must be default or passive")
    return mode


def _coerce_option_value(value, default):
    if isinstance(value, typer.models.OptionInfo):
        return default
    return value


def _resolve_shop_id(shop_cfg) -> int:
    env_key = f"SHOPEE_{shop_cfg.shop_key.upper()}_SHOP_ID"
    value = os.environ.get(env_key)
    if not value:
        if shop_cfg.shopee_shop_id is not None:
            return int(shop_cfg.shopee_shop_id)
        raise typer.BadParameter(
            f"shopee_shop_id missing in shops config and env for {shop_cfg.shop_key}"
        )
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise typer.BadParameter(f"invalid {env_key} value") from exc


def _check_token_passive(shop_cfg) -> tuple[bool, str]:
    init_db()
    session = SessionLocal()
    try:
        token = get_token(session, shop_cfg.shop_key)
        if token is None:
            print(
                f"token_missing shop={shop_cfg.shop_key} "
                f"shop_id={_resolve_shop_id(shop_cfg)}"
            )
            return False, "missing_token"
        if needs_refresh(token.access_token_expires_at):
            print(
                "token_expired_refresh_disabled "
                f"shop={shop_cfg.shop_key} shop_id={_resolve_shop_id(shop_cfg)}"
            )
            return False, "expired_access"
    finally:
        session.close()
    return True, "ok"


def _check_token_passive_or_exit(shop_cfg) -> None:
    ok, _reason = _check_token_passive(shop_cfg)
    if not ok:
        raise typer.Exit(code=1)


def _compute_expires_in(timestamp_value: int | None) -> int:
    if not timestamp_value:
        return -1
    now_ts = int(datetime.now(timezone.utc).timestamp())
    try:
        return max(-1, int(timestamp_value) - now_ts)
    except Exception:
        return -1


def _detect_env_file(explicit: str | None) -> str | None:
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise typer.BadParameter(f"env file not found: {explicit}")
        return str(path)
    candidates = [
        Path("collaboration") / "env" / ".env.phase1.local",
        Path(".env"),
        Path("collaboration") / "env" / ".env",
        Path("collaboration") / "env" / ".env.local",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _append_capture_section(lines: list[str], title: str, content: str) -> None:
    lines.append(f"## {title}")
    lines.append("```")
    if content:
        lines.append(content.rstrip())
    lines.append("```")
    lines.append("")


def _run_capture_step(func, *args, **kwargs) -> tuple[int, str]:
    buffer = io.StringIO()
    exit_code = 0
    with redirect_stdout(buffer), redirect_stderr(buffer):
        try:
            func(*args, **kwargs)
        except typer.Exit as exc:
            exit_code = exc.exit_code if exc.exit_code is not None else 0
        except Exception as exc:  # noqa: BLE001
            exit_code = 1
            error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
            print(f"error={error_text}")
    return exit_code, buffer.getvalue()


def _scan_token_dbs(
    *,
    only_shops: list[str],
    scan_root: str | None,
) -> tuple[list[str], str | None]:
    import sqlite3

    roots: list[Path]
    if scan_root:
        roots = [Path(scan_root)]
    else:
        roots = [Path.cwd(), Path.cwd() / "collaboration"]
    db_paths: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*.db"):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            db_paths.append(path)

    lines: list[str] = []
    candidates: list[tuple[Path, bool, dict[str, bool]]] = []
    for path in sorted(db_paths):
        token_store = False
        tokens: dict[str, bool] = {shop: False for shop in only_shops}
        error_text = None
        try:
            conn = sqlite3.connect(str(path))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                ("shopee_tokens",),
            )
            token_store = cursor.fetchone() is not None
            if token_store:
                for shop_key in only_shops:
                    cursor.execute(
                        "SELECT 1 FROM shopee_tokens WHERE shop_key=? LIMIT 1",
                        (shop_key,),
                    )
                    tokens[shop_key] = cursor.fetchone() is not None
        except Exception as exc:  # noqa: BLE001
            error_text = _scrub_sensitive_text(str(exc)) or "scan_failed"
        finally:
            try:
                conn.close()
            except Exception:
                pass

        parts = [
            f"token_db_candidate path={path}",
            f"token_store={1 if token_store else 0}",
        ]
        for shop_key in only_shops:
            parts.append(f"{shop_key}_token={1 if tokens[shop_key] else 0}")
        if error_text:
            parts.append(f"error={error_text}")
        line = " ".join(parts)
        lines.append(line)
        candidates.append((path, token_store, tokens))

    best_path = None
    best_score = 0
    for path, token_store, tokens in candidates:
        if not token_store:
            continue
        score = sum(1 for value in tokens.values() if value)
        if score > best_score:
            best_score = score
            best_path = path

    recommended = _format_sqlite_url(best_path) if best_path and best_score else None
    return lines, recommended


def _format_sqlite_url(path: Path | None) -> str | None:
    if path is None:
        return None
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(Path.cwd().resolve())
        return f"sqlite:///./{relative.as_posix()}"
    except ValueError:
        return f"sqlite:///{resolved.as_posix()}"


def _extract_appsscript_token_map(data: object) -> dict[str, dict]:
    if not isinstance(data, dict):
        return {}
    shops_section = data.get("shops")
    tokens_section = data.get("tokens")
    if isinstance(shops_section, dict):
        source = shops_section
    elif isinstance(tokens_section, dict):
        source = tokens_section
    else:
        source = data
    token_map: dict[str, dict] = {}
    for key, value in source.items():
        shop_id = None
        key_str = str(key)
        if key_str.isdigit():
            shop_id = key_str
        elif key_str.startswith("SHOPEE_TOKEN_DATA_"):
            suffix = key_str.replace("SHOPEE_TOKEN_DATA_", "", 1)
            if suffix.isdigit():
                shop_id = suffix
        payload = value
        if isinstance(payload, str):
            try:
                import json

                payload = json.loads(payload)
            except Exception:
                continue
        if not shop_id and isinstance(payload, dict):
            nested_shop_id = payload.get("shop_id")
            try:
                nested_shop_id_value = int(nested_shop_id)
            except Exception:
                nested_shop_id_value = 0
            if nested_shop_id_value > 0:
                shop_id = str(nested_shop_id_value)
        if not shop_id:
            continue
        if isinstance(payload, dict):
            token_map[shop_id] = payload
    return token_map


def _parse_appsscript_fingerprint(data: object) -> dict[str, object]:
    if not isinstance(data, dict):
        return {"partner_id": None, "partner_key_sha8": None, "shops": {}}
    partner_id = _coerce_int(data.get("partner_id"))
    partner_key_sha8 = data.get("partner_key_sha8")
    if partner_key_sha8 is not None and not isinstance(partner_key_sha8, str):
        partner_key_sha8 = str(partner_key_sha8)
    shops_raw = data.get("shops")
    shops: dict[str, dict[str, object | None]] = {}
    if isinstance(shops_raw, dict):
        for shop_key, payload in shops_raw.items():
            if not isinstance(payload, dict):
                continue
            shops[str(shop_key)] = {
                "shop_id": _coerce_int(payload.get("shop_id")),
                "token_len": _coerce_int(payload.get("token_len")),
                "token_sha8": (
                    str(payload.get("token_sha8"))
                    if payload.get("token_sha8") is not None
                    else None
                ),
            }
    return {
        "partner_id": partner_id,
        "partner_key_sha8": partner_key_sha8,
        "shops": shops,
    }


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_epoch_seconds(raw_value: object, *, present: bool) -> dict[str, object]:
    if not present:
        return {
            "timestamp": None,
            "present": 0,
            "unit_guess": "unknown",
            "src": "missing",
            "raw_type": "none",
        }
    raw_type = type(raw_value).__name__
    try:
        parsed = int(raw_value)
    except Exception:
        return {
            "timestamp": None,
            "present": 1,
            "unit_guess": "unknown",
            "src": "parse_error",
            "raw_type": raw_type,
        }
    if parsed <= 0:
        return {
            "timestamp": None,
            "present": 1,
            "unit_guess": "unknown",
            "src": "parse_error",
            "raw_type": raw_type,
        }
    unit_guess = "ms" if parsed >= 1_000_000_000_000 else "sec"
    if unit_guess == "ms":
        parsed = int(parsed / 1000)
    return {
        "timestamp": parsed,
        "present": 1,
        "unit_guess": unit_guess,
        "src": "expire_timestamp",
        "raw_type": raw_type,
    }


def _build_date_vars_probe(target_date: date) -> dict[str, str]:
    timestamp = int(
        datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=resolve_timezone("Asia/Ho_Chi_Minh"),
        ).timestamp()
    )
    date_str = target_date.isoformat()
    return {
        "date": date_str,
        "date_from": date_str,
        "date_to": date_str,
        "timestamp": str(timestamp),
    }


def _write_probe_artifact(
    root: Path,
    shop_key: str,
    target_date: date,
    call_name: str,
    payload: dict | None,
    ok: bool,
    error_text: str | None,
) -> Path | None:
    target_dir = root / shop_key / target_date.isoformat() / "ads_probe"
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{call_name}.json"
    meta = {
        "shop_key": shop_key,
        "call_name": call_name,
        "ok": ok,
        "error": error_text,
    }
    meta = redact_secrets(
        meta,
        extra_keys={
            "partner_key",
            "access_token",
            "refresh_token",
            "sign",
            "authorization",
            "cookie",
            "secret",
            "client_secret",
        },
    )
    data: dict[str, object] = {"__meta": meta}
    if payload is not None:
        if isinstance(payload, dict):
            data.update(
                redact_secrets(
                    payload,
                    extra_keys={
                        "partner_key",
                        "access_token",
                        "refresh_token",
                        "sign",
                        "authorization",
                        "cookie",
                        "secret",
                        "client_secret",
                    },
                )
            )
        else:
            data["payload"] = redact_text(str(payload))
    path.write_text(_dump_json(data, pretty=True), encoding="utf-8")
    return path


def _analyze_probe_payload(call_name: str, payload: dict) -> dict[str, object]:
    top_keys = sorted([str(key) for key in payload.keys()])
    response_keys: list[str] = []
    record_count: int | None = None
    response = payload.get("response")
    if isinstance(response, dict):
        response_keys = sorted([str(key) for key in response.keys()])
        for list_key in ["records", "data", "items", "list"]:
            value = response.get(list_key)
            if isinstance(value, list):
                record_count = len(value)
                break
    return {
        "call_name": call_name,
        "top_keys": top_keys,
        "response_keys": response_keys,
        "record_count": record_count,
    }


def _write_probe_analysis(
    root: Path,
    shop_key: str,
    target_date: date,
    records: list[dict[str, object]],
) -> Path:
    target_dir = root / shop_key / target_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / "ads_probe_summary.md"
    lines = [
        f"# Ads Probe Summary ({shop_key})",
        f"Date: {target_date.isoformat()}",
        "",
        "| call_name | top_keys | response_keys | record_count |",
        "| --- | --- | --- | --- |",
    ]
    for record in records:
        top_keys = ", ".join(record.get("top_keys") or [])
        response_keys = ", ".join(record.get("response_keys") or [])
        record_count = record.get("record_count")
        count_text = "-" if record_count is None else str(record_count)
        lines.append(
            f"| {record.get('call_name')} | {top_keys} | {response_keys} | {count_text} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _write_daily_truth_artifact(
    root: Path,
    shop_key: str,
    target_date: date,
    call_name: str,
    payload: dict | None,
    ok: bool,
    error_text: str | None,
) -> Path:
    target_dir = root / shop_key / target_date.isoformat() / "ads_daily_truth"
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{call_name}.json"
    meta = {
        "shop_key": shop_key,
        "call_name": call_name,
        "ok": ok,
        "error": error_text,
    }
    extra_keys = {
        "partner_key",
        "access_token",
        "refresh_token",
        "sign",
        "authorization",
        "cookie",
        "secret",
        "client_secret",
    }
    data: dict[str, object] = {"__meta": redact_secrets(meta, extra_keys=extra_keys)}
    if payload is not None:
        if isinstance(payload, dict):
            data.update(redact_secrets(payload, extra_keys=extra_keys))
        else:
            data["payload"] = redact_text(str(payload))
    path.write_text(_dump_json(data, pretty=True), encoding="utf-8")
    return path


def _extract_path_value(payload: object, path: str) -> object | None:
    if not path:
        return payload
    current = payload
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
            continue
        return None
    return current


def _find_first_list_path(payload: object) -> tuple[str, list[dict[str, object]]]:
    preferred = [
        "response.records",
        "response.data",
        "response.items",
        "response.list",
        "response.result",
        "response",
        "records",
        "data",
        "items",
        "list",
        "result",
    ]
    for path in preferred:
        value = _extract_path_value(payload, path)
        if isinstance(value, list):
            records = [row for row in value if isinstance(row, dict)]
            if records:
                return path, records
    queue: list[tuple[str, object]] = [("", payload)]
    seen = 0
    while queue and seen < 512:
        seen += 1
        current_path, node = queue.pop(0)
        if isinstance(node, list):
            records = [row for row in node if isinstance(row, dict)]
            if records:
                return (current_path or "response"), records
            continue
        if isinstance(node, dict):
            for key, value in node.items():
                key_str = str(key)
                child_path = f"{current_path}.{key_str}" if current_path else key_str
                queue.append((child_path, value))
    return "-", []


def _detect_metric_field_path(
    *,
    items_path: str,
    records: list[dict[str, object]],
    primary_keys: list[str],
    pair_keys: tuple[str, str] | None = None,
) -> str:
    if not records:
        return "-"
    if pair_keys:
        left, right = pair_keys
        has_left = any(left in row for row in records if isinstance(row, dict))
        has_right = any(right in row for row in records if isinstance(row, dict))
        if has_left or has_right:
            if has_left and has_right:
                return f"{items_path}[].{left}+{right}"
            return f"{items_path}[].{left if has_left else right}"
    fallback: str | None = None
    for key in primary_keys:
        has_key = False
        has_non_empty = False
        for row in records:
            if key in row:
                has_key = True
                if row.get(key) not in (None, ""):
                    has_non_empty = True
                    break
        if has_non_empty:
            return f"{items_path}[].{key}"
        if has_key and fallback is None:
            fallback = f"{items_path}[].{key}"
    return fallback or "-"


def _detect_ads_daily_truth(payload: dict) -> dict[str, object]:
    items_path, records = _find_first_list_path(payload)
    spend_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["spend", "expense", "cost", "spend_amt", "spend_today"],
    )
    clicks_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["clicks", "click", "clicks_today"],
    )
    impr_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["impressions", "impression", "views", "impressions_today"],
    )
    orders_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["orders", "order", "orders_cnt", "orders_today"],
        pair_keys=("direct_order", "broad_order"),
    )
    gmv_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["gmv", "revenue", "sales", "gmv_today"],
        pair_keys=("direct_gmv", "broad_gmv"),
    )
    campaign_id_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["campaign_id", "campaignId", "campaignID", "id"],
    )
    campaign_name_path = _detect_metric_field_path(
        items_path=items_path,
        records=records,
        primary_keys=["campaign_name", "campaignName", "name", "ad_name", "campaign"],
    )
    return {
        "items_path": items_path,
        "item_count": len(records),
        "spend_path": spend_path,
        "clicks_path": clicks_path,
        "impr_path": impr_path,
        "orders_path": orders_path,
        "gmv_path": gmv_path,
        "campaign_id_path": campaign_id_path,
        "campaign_name_path": campaign_name_path,
    }


def _write_ads_daily_truth_summary(
    *,
    analysis_root: Path,
    shop_key: str,
    target_date: date,
    summary: dict[str, object],
    saved_paths: list[Path],
) -> Path:
    target_dir = analysis_root / shop_key / target_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / "ads_daily_truth_summary.md"
    lines = [
        f"# Ads Daily Truth Summary ({shop_key})",
        f"Date: {target_date.isoformat()}",
        "",
        f"- items_path: {summary.get('items_path', '-')}",
        f"- item_count: {summary.get('item_count', 0)}",
        f"- spend_path: {summary.get('spend_path', '-')}",
        f"- clicks_path: {summary.get('clicks_path', '-')}",
        f"- impr_path: {summary.get('impr_path', '-')}",
        f"- orders_path: {summary.get('orders_path', '-')}",
        f"- gmv_path: {summary.get('gmv_path', '-')}",
        f"- campaign_id_path: {summary.get('campaign_id_path', '-')}",
        f"- campaign_name_path: {summary.get('campaign_name_path', '-')}",
        "",
        "Saved JSON:",
    ]
    for saved_path in saved_paths:
        lines.append(f"- {saved_path}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _save_redacted_json(path: Path, payload: object) -> None:
    extra_keys = {
        "partner_key",
        "access_token",
        "refresh_token",
        "sign",
        "authorization",
        "cookie",
        "secret",
        "client_secret",
    }
    if isinstance(payload, dict):
        safe_payload = redact_secrets(payload, extra_keys=extra_keys)
        path.write_text(_dump_json(safe_payload, pretty=True), encoding="utf-8")
    else:
        path.write_text(
            _dump_json({"payload": redact_text(str(payload))}, pretty=True),
            encoding="utf-8",
        )

def _campaign_daily_truth_extract_records(payload: dict | None) -> list[dict[str, object]]:
    normalized = _normalize_ads_daily_payload(payload if isinstance(payload, dict) else None)
    records = _extract_path_value(normalized, "response.records")
    if not isinstance(records, list):
        return []
    rows: list[dict[str, object]] = []
    for item in records:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        normalized = stripped.replace(",", "")
        try:
            return Decimal(normalized)
        except Exception:  # noqa: BLE001
            return None
    return None


def _campaign_daily_truth_metrics(records: list[dict[str, object]]) -> dict[str, object]:
    spend_total = Decimal("0")
    gmv_total = Decimal("0")
    impressions_total = 0
    clicks_total = 0
    orders_total = 0
    has_campaign_id_field = 0
    non_total_campaign_rows = 0
    for row in records:
        campaign_id = str(
            row.get("campaign_id") or row.get("campaignId") or row.get("id") or ""
        ).strip()
        if campaign_id:
            has_campaign_id_field = 1
            if campaign_id.upper() != "SHOP_TOTAL":
                non_total_campaign_rows += 1
        spend = _to_decimal(row.get("spend"))
        if spend is not None:
            spend_total += spend
        gmv = _to_decimal(row.get("gmv"))
        if gmv is not None:
            gmv_total += gmv
        try:
            impressions_total += int(row.get("impressions") or 0)
        except Exception:  # noqa: BLE001
            pass
        try:
            clicks_total += int(row.get("clicks") or 0)
        except Exception:  # noqa: BLE001
            pass
        try:
            orders_total += int(row.get("orders") or 0)
        except Exception:  # noqa: BLE001
            pass
    return {
        "items_total": len(records),
        "has_campaign_id_field": has_campaign_id_field,
        "non_total_campaign_rows": non_total_campaign_rows,
        "spend_total": spend_total,
        "impressions_total": impressions_total,
        "clicks_total": clicks_total,
        "orders_total": orders_total,
        "gmv_total": gmv_total,
    }


def _write_campaign_daily_truth_summary_files(
    *,
    root: Path,
    shop_key: str,
    target_date: date,
    summary: dict[str, object],
) -> tuple[Path, Path]:
    target_dir = root / shop_key / target_date.isoformat() / "ads_campaign_daily_truth"
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / "ads_campaign_daily_truth_summary.json"
    md_path = target_dir / "ads_campaign_daily_truth_summary.md"
    json_path.write_text(_dump_json(summary, pretty=True), encoding="utf-8")

    lines = [
        f"# Ads Campaign Daily Truth Summary ({shop_key})",
        f"Date: {target_date.isoformat()}",
        "",
        f"- verdict: {summary.get('verdict', '-')}",
        f"- reason: {summary.get('reason', '-')}",
        f"- selected_endpoint: {summary.get('selected_endpoint', '-')}",
        f"- blocked_403: {summary.get('blocked_403', 0)}",
        f"- try_alt_endpoints: {summary.get('try_alt_endpoints', 0)}",
        f"- items_total: {summary.get('items_total', 0)}",
        f"- has_campaign_id_field: {summary.get('has_campaign_id_field', 0)}",
        f"- non_total_campaign_rows: {summary.get('non_total_campaign_rows', 0)}",
        f"- spend_total: {summary.get('spend_total', '-')}",
        f"- impressions_total: {summary.get('impressions_total', 0)}",
        f"- clicks_total: {summary.get('clicks_total', 0)}",
        f"- orders_total: {summary.get('orders_total', 0)}",
        f"- gmv_total: {summary.get('gmv_total', '-')}",
        f"- ids_total: {summary.get('ids_total', 0)}",
        f"- chunks_total: {summary.get('chunks_total', 0)}",
        f"- api_error: {summary.get('api_error', '-')}",
        f"- api_message: {summary.get('api_message', '-')}",
        "",
        "Endpoint attempts:",
    ]
    endpoint_results = summary.get("endpoint_results", []) or []
    if endpoint_results:
        for row in endpoint_results:
            endpoint = str(row.get("endpoint") or "-")
            ok = int(row.get("ok") or 0)
            reason = str(row.get("reason") or "-")
            http_status = row.get("http_status")
            api_error = row.get("api_error")
            request_id = row.get("request_id")
            items_total = int(row.get("items_total") or 0)
            has_cid = int(row.get("has_campaign_id_field") or 0)
            lines.append(
                f"- endpoint={endpoint} ok={ok} reason={reason} "
                f"http_status={http_status if http_status is not None else '-'} "
                f"api_error={api_error if api_error not in (None, '') else '-'} "
                f"request_id={request_id if request_id not in (None, '') else '-'} "
                f"items_total={items_total} has_campaign_id_field={has_cid}"
            )
    else:
        lines.append("- (none)")
    lines.extend(
        [
            "",
        "Artifacts:",
        ]
    )
    for path in summary.get("saved_json_paths", []) or []:
        lines.append(f"- {path}")
    lines.append("")
    lines.append("Next actions:")
    verdict = str(summary.get("verdict") or "").upper()
    if verdict == "SUPPORTED":
        lines.append("- Use this endpoint in daily ingest fanout to populate campaign-level Top/Worst.")
    else:
        lines.append("- Attach these artifacts to Shopee support ticket for ads campaign-daily access review.")
        lines.append("- Verify app permission/region and endpoint whitelist for campaign daily performance.")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path, json_path


def _collect_campaign_daily_truth_artifacts(
    *,
    artifacts_root: Path,
    target_shops: list,
    target_date: date,
) -> tuple[list[Path], list[dict[str, object]]]:
    files: list[Path] = []
    summaries: list[dict[str, object]] = []
    for shop_cfg in target_shops:
        shop_dir = (
            artifacts_root
            / shop_cfg.shop_key
            / target_date.isoformat()
            / "ads_campaign_daily_truth"
        )
        if not shop_dir.exists():
            continue
        summary_md = shop_dir / "ads_campaign_daily_truth_summary.md"
        summary_json = shop_dir / "ads_campaign_daily_truth_summary.json"
        if summary_md.exists():
            files.append(summary_md)
        if summary_json.exists():
            files.append(summary_json)
            try:
                payload = _read_json(summary_json)
            except Exception:  # noqa: BLE001
                payload = {}
            summaries.append(
                {
                    "shop_key": shop_cfg.shop_key,
                    "shop_label": shop_cfg.label,
                    "shop_id": shop_cfg.shopee_shop_id,
                    "summary_path": str(summary_json),
                    "summary": payload if isinstance(payload, dict) else {},
                }
            )
        for pattern in (
            "endpoint_*.json",
            "campaign_id_list_page_*.json",
            "campaign_daily_chunk_*.json",
        ):
            for path in sorted(shop_dir.glob(pattern)):
                if path.exists():
                    files.append(path)
    dedup: dict[str, Path] = {}
    for path in files:
        dedup[str(path.resolve())] = path
    return list(dedup.values()), summaries


def _collect_request_ids(obj: object, out: set[str]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_lower = str(key).lower()
            if key_lower in {"request_id", "requestid"} and value not in (None, ""):
                out.add(str(value))
            _collect_request_ids(value, out)
        return
    if isinstance(obj, list):
        for item in obj:
            _collect_request_ids(item, out)


def _build_campaign_breakdown_ticket_template(
    *,
    target_date: date,
    transport_value: str,
    settings,
    target_shops: list,
    summaries: list[dict[str, object]],
    probe_exit_code: int,
    generated_at_utc: datetime,
    extra_request_ids: set[str] | None = None,
) -> str:
    summary_by_shop: dict[str, dict[str, object]] = {}
    for row in summaries:
        shop_key = str(row.get("shop_key") or "")
        payload = row.get("summary")
        if shop_key and isinstance(payload, dict):
            summary_by_shop[shop_key] = payload

    request_ids: set[str] = set()
    if extra_request_ids:
        request_ids.update(extra_request_ids)
    endpoint_lines: list[str] = []
    for shop_cfg in target_shops:
        payload = summary_by_shop.get(shop_cfg.shop_key, {})
        endpoint_rows = payload.get("endpoint_results") if isinstance(payload, dict) else []
        if not isinstance(endpoint_rows, list):
            endpoint_rows = []
        for row in endpoint_rows:
            if not isinstance(row, dict):
                continue
            endpoint = str(row.get("endpoint") or "-")
            reason = str(row.get("reason") or "-")
            http_status = row.get("http_status")
            api_error = row.get("api_error")
            request_id = row.get("request_id")
            if request_id not in (None, ""):
                request_ids.add(str(request_id))
            endpoint_lines.append(
                f"- shop={shop_cfg.shop_key} endpoint={endpoint} "
                f"http_status={http_status if http_status is not None else '-'} "
                f"api_error={api_error if api_error not in (None, '') else '-'} "
                f"reason={reason} request_id={request_id if request_id not in (None, '') else '-'}"
            )

    lines: list[str] = []
    lines.append("# Shopee Ads Campaign Breakdown Permission Request")
    lines.append("")
    lines.append(f"- generated_at_utc: {generated_at_utc.isoformat()}")
    lines.append(f"- environment: {settings.env}")
    lines.append(f"- timezone: {settings.timezone}")
    lines.append(f"- transport: {transport_value}")
    lines.append(f"- date: {target_date.isoformat()}")
    lines.append(f"- partner_id: {settings.shopee_partner_id if settings.shopee_partner_id is not None else '-'}")
    lines.append(f"- probe_exit_code: {probe_exit_code}")
    lines.append("")
    lines.append("## Shops affected")
    for shop_cfg in target_shops:
        payload = summary_by_shop.get(shop_cfg.shop_key, {})
        verdict = str(payload.get("verdict") or "-") if isinstance(payload, dict) else "-"
        reason = str(payload.get("reason") or "-") if isinstance(payload, dict) else "-"
        blocked_403 = int(payload.get("blocked_403") or 0) if isinstance(payload, dict) else 0
        lines.append(
            f"- shop_label={shop_cfg.label} shop_key={shop_cfg.shop_key} "
            f"shop_id={shop_cfg.shopee_shop_id} verdict={verdict} reason={reason} blocked_403={blocked_403}"
        )
    lines.append("")
    lines.append("## Endpoints attempted")
    if endpoint_lines:
        lines.extend(endpoint_lines)
    else:
        lines.append("- (no endpoint attempt rows captured)")
    lines.append("")
    lines.append("## Request IDs")
    if request_ids:
        for request_id in sorted(request_ids):
            lines.append(f"- {request_id}")
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Expected behavior")
    lines.append("- Campaign-level daily rows should be returned so Top/Worst campaign sections are populated.")
    lines.append("")
    lines.append("## Current behavior")
    lines.append("- Ads campaign breakdown endpoints return 403 or no campaign rows; shop totals ingest continues.")
    lines.append("")
    lines.append("## Non-Ads API health")
    lines.append("- Baseline shop-level ingest and daily report generation are working; issue is limited to campaign breakdown endpoints.")
    lines.append("")
    lines.append("## Security confirmation")
    lines.append("- This packet excludes access_token, refresh_token, and partner_key.")
    return "\n".join(lines) + "\n"


def _load_probe_fixture_payload(fixtures_dir: Path, call_name: str) -> dict | None:
    preferred_map = {
        "shop_info": "ads_probe_meta_with_fake_secrets.json",
        "ads_daily": "ads_daily_ok_with_fake_secrets.json",
        "ads_snapshot": "ads_snapshot_ok_with_fake_secrets.json",
    }
    preferred = preferred_map.get(call_name)
    if preferred:
        preferred_path = fixtures_dir / preferred
        if preferred_path.exists():
            return _read_json(preferred_path)
    return _load_fixture_payload(fixtures_dir, call_name)


@ops_phase1_app.command("verify")
def ops_phase1_verify(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    send_discord: bool = typer.Option(False, "--send-discord"),
    ping_live: bool = typer.Option(False, "--ping-live"),
    allow_network: bool = typer.Option(False, "--allow-network"),
    channel: str = typer.Option("both", "--channel", help="report | alerts | both"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    database_url: str | None = typer.Option(
        None, "--database-url", help="Override database URL"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    if database_url:
        os.environ["DATABASE_URL"] = database_url
        get_settings.cache_clear()

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    require_ads_endpoints = ping_live or transport.lower().strip() == "live"
    lines, ready, _missing = _compute_phase1_readiness(
        target_shops, settings, require_ads_endpoints=require_ads_endpoints
    )
    for line in lines:
        print(line)
    if not ready:
        print(
            "phase1_verify ready=0 discord_checked=0 discord_sent=0 ping=skipped ping_ok=0"
        )
        raise typer.Exit(code=1)

    # Always run discord dry-run
    ops_check_discord(shops=shops, dry_run=True, channel=channel)
    discord_sent = 0
    if send_discord:
        try:
            ops_check_discord(shops=shops, dry_run=False, channel=channel)
            discord_sent = 1
        except typer.Exit as exc:
            print(
                "phase1_verify ready=1 discord_checked=1 "
                f"discord_sent=0 ping=skipped ping_ok=0"
            )
            raise typer.Exit(code=1) from exc

    ping_transport = "live" if ping_live else transport
    ping_ok = 0
    try:
        ops_check_shopee_ping(
            shops=shops,
            transport=ping_transport,
            fixtures_dir=fixtures_dir,
            dry_run=False,
            allow_network=allow_network,
        )
        ping_ok = 1
    except typer.Exit as exc:
        print(
            "phase1_verify ready=1 discord_checked=1 "
            f"discord_sent={discord_sent} ping={ping_transport} ping_ok=0"
        )
        raise typer.Exit(code=1) from exc

    print(
        "phase1_verify ready=1 discord_checked=1 "
        f"discord_sent={discord_sent} ping={ping_transport} ping_ok={ping_ok}"
    )


@ops_phase1_app.command("preview")
def ops_phase1_preview(
    date_value: str | None = typer.Option(None, "--date", help="YYYY-MM-DD"),
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "default", "--token-mode", help="default | passive"
    ),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_sync: bool = typer.Option(
        True, "--token-sync/--no-token-sync", help="Sync tokens from file into DB"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    plan: str = typer.Option(
        "collaboration/plans/ads_ingest_minimal.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml",
        "--mapping",
        help="Mapping YAML path",
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    save_artifacts: bool = typer.Option(
        False,
        "--save-artifacts/--no-save-artifacts",
        help="Save full artifacts (success+fail) under artifacts-root",
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    save_failure_artifacts: bool = typer.Option(
        False, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    token_file = _coerce_option_value(token_file, None)
    token_sync = _coerce_option_value(token_sync, True)
    save_artifacts = _coerce_option_value(save_artifacts, False)
    artifacts_root = _coerce_option_value(
        artifacts_root, "collaboration/artifacts/shopee_api"
    )
    save_failure_artifacts = _coerce_option_value(save_failure_artifacts, False)
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    token_mode = _coerce_option_value(token_mode, "passive")
    token_mode_value = _normalize_token_mode(token_mode)

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if transport_value == "live" and not (allow_network or allow_network_env):
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    if transport_value == "live":
        daily_ok, snapshot_ok, missing = _ads_endpoint_status()
        if not (daily_ok and snapshot_ok):
            print(f"ads_endpoints_not_configured missing={','.join(missing)}")
            raise typer.Exit(code=1)

    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir
        get_settings.cache_clear()

    target_date = _parse_date_or_today(date_value)
    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, only_shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)
    shops_value = ",".join([shop.shop_key for shop in target_shops])
    if token_file and token_sync:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        print(
            "token_sync_from_file_ok=1 "
            f"shops={','.join([shop.shop_key for shop in target_shops])}"
        )
        _print_db_token_fingerprints(target_shops)
    elif not token_file:
        _phase1_db_preflight_or_exit(
            shops=shops_value,
            min_access_ttl_sec=120,
            command_name="phase1_preview",
        )

    fixtures_path = Path(fixtures_dir) if transport_value == "fixtures" else None
    if fixtures_path and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    print(
        "phase1_preview_start "
        f"date={target_date.isoformat()} "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} plan_path={plan} mapping_path={mapping} "
        f"reports_dir={reports_dir} save_artifacts={1 if save_artifacts else 0} "
        f"artifacts_root={artifacts_root}"
    )

    if transport_value == "live":
        _require_shopee_settings(settings)

    totals = {"calls_ok": 0, "calls_fail": 0, "campaigns": 0, "daily": 0, "snapshots": 0}
    failure_artifacts_dir = (
        str(_resolve_failure_artifacts_root()) if save_failure_artifacts else None
    )
    failure_artifacts_saved_total = 0
    failure_artifacts_counts: dict[str, int] = {}
    skipped_expired: list[str] = []
    skipped_missing: list[str] = []
    processed_shops: list[object] = []
    for shop_cfg in target_shops:
        if transport_value == "live" and token_mode_value == "passive":
            ok, reason = _check_token_passive(shop_cfg)
            if not ok:
                if reason == "expired_access":
                    skipped_expired.append(shop_cfg.shop_key)
                elif reason == "missing_token":
                    skipped_missing.append(shop_cfg.shop_key)
                continue
        processed_shops.append(shop_cfg)
        try:
            summary = ingest_ads_live(
                shop_cfg=shop_cfg,
                settings=settings,
                target_date=target_date,
                plan_path=Path(plan),
                mapping_path=Path(mapping),
                save_artifacts=save_artifacts,
                save_failure_artifacts=save_failure_artifacts,
                dry_run=False,
                strict_mapping=False,
                fixtures_dir=fixtures_path,
                save_root=Path(artifacts_root),
                token_mode=token_mode_value,
                client_factory=_build_shopee_client,
            )
        except ValueError as exc:
            print(f"error={exc}")
            raise typer.Exit(code=1)
        totals["calls_ok"] += summary.calls_ok
        totals["calls_fail"] += summary.calls_fail
        totals["campaigns"] += summary.campaigns
        totals["daily"] += summary.daily
        totals["snapshots"] += summary.snapshots
        print(
            f"shop={shop_cfg.shop_key} date={target_date.isoformat()} "
            f"calls_ok={summary.calls_ok} calls_fail={summary.calls_fail}"
        )
        print(
            f"upserted campaigns={summary.campaigns} daily={summary.daily} "
            f"snapshots={summary.snapshots}"
        )
        if save_failure_artifacts:
            failure_artifacts_saved_total += summary.failure_artifacts_saved
            failure_artifacts_counts[shop_cfg.shop_key] = summary.failure_artifacts_saved
        if summary.call_failures:
            for failure in summary.call_failures[:3]:
                http_text = (
                    str(failure.http_status)
                    if failure.http_status is not None
                    else "-"
                )
                api_error_text = _format_api_value(failure.api_error)
                api_message_text = _format_api_value(failure.api_message)
                request_id_text = _format_api_value(failure.request_id)
                print(
                    "call_fail "
                    f"shop={shop_cfg.shop_key} call={failure.call_name} "
                    f"http={http_text} api_error={api_error_text} "
                    f"api_message={api_message_text} request_id={request_id_text}"
                )

    if skipped_expired:
        print(f"preview_skipped_shops expired_access={','.join(skipped_expired)}")
    if skipped_missing:
        print(f"preview_skipped_shops missing_token={','.join(skipped_missing)}")
    if not processed_shops:
        print("preview_ok=0")
        raise typer.Exit(code=2)

    print(
        "total "
        f"calls_ok={totals['calls_ok']} calls_fail={totals['calls_fail']} "
        f"campaigns={totals['campaigns']} daily={totals['daily']} snapshots={totals['snapshots']}"
    )

    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in processed_shops:
            tz = resolve_timezone(shop_cfg.timezone or settings.timezone)
            now = datetime.now(tz)
            data = aggregate_daily_report(session, shop_cfg.shop_key, target_date, None)
            data.update(
                {
                    "shop_label": shop_cfg.label,
                    "kind": "final",
                    "generated_at": now,
                }
            )
            html = render_daily_html(data)
            output_path = write_report_file(
                shop_cfg.shop_key, target_date, "final", html
            )
            print(f"report_path shop={shop_cfg.shop_key} path={output_path}")
            if send_discord:
                report_url = _build_report_url(
                    shop_cfg.shop_key,
                    target_date,
                    "final",
                    settings.report_access_token,
                )
                message = (
                    f"[{shop_cfg.label}][ACTION] Phase1 preview report "
                    f"date={target_date.isoformat()}"
                )
                if report_url:
                    message = f"{message} | {report_url}"
                send(
                    "report",
                    message,
                    shop_label=shop_cfg.label,
                    webhook_url=shop_cfg.discord_webhook_url,
                )
    finally:
        session.close()

    if save_failure_artifacts:
        if failure_artifacts_dir:
            print(f"failure_artifacts_dir={failure_artifacts_dir}")
        print(f"failure_artifacts_saved={failure_artifacts_saved_total}")
        for shop_cfg in processed_shops:
            shop_key = shop_cfg.shop_key
            print(
                f"shop={shop_key} failure_artifacts_saved="
                f"{failure_artifacts_counts.get(shop_key, 0)}"
            )

    print("phase1_preview_ok=1")


def _default_phase1_schedule_plan(job_value: str, override: str | None) -> str:
    if override and override.strip():
        return override.strip()
    if job_value == "daily-final":
        return "collaboration/plans/ads_ingest_daily_final.yaml"
    return "collaboration/plans/ads_ingest_minimal.yaml"


@ops_phase1_schedule_app.command("run-once")
def ops_phase1_schedule_run_once(
    job: str = typer.Option(..., "--job", help="daily-final | daily-midday | weekly"),
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    token_mode: str = typer.Option("passive", "--token-mode", help="default | passive"),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_sync: bool = typer.Option(
        True, "--token-sync/--no-token-sync", help="Sync tokens from file into DB"
    ),
    plan: str | None = typer.Option(
        None, "--plan", help="Plan YAML path override"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml",
        "--mapping",
        help="Mapping YAML path",
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    save_failure_artifacts: bool = typer.Option(
        True, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    support_packet: bool = typer.Option(False, "--support-packet"),
    send_discord: bool = typer.Option(False, "--send-discord/--no-send-discord"),
    discord_attach_report_html: bool = typer.Option(
        False,
        "--discord-attach-report-html",
        help="Attach generated daily report HTML when sending Discord report messages",
    ),
    discord_attach_report_zip: bool = typer.Option(
        False,
        "--discord-attach-report-zip",
        help="Attach generated daily report ZIP when sending Discord report messages",
    ),
    discord_attach_report_md: bool = typer.Option(
        False,
        "--discord-attach-report-md",
        help="Attach generated daily report Markdown summary when sending Discord report messages",
    ),
) -> None:
    _maybe_load_env_file(env_file)
    job_value = job.strip().lower()
    if job_value not in {"daily-final", "daily-midday", "weekly"}:
        raise typer.BadParameter("job must be one of: daily-final, daily-midday, weekly")
    plan_path_value = _default_phase1_schedule_plan(job_value, plan)

    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = allow_network or allow_network_env
    if transport_value == "live" and not allow_network_effective:
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    if transport_value == "live":
        daily_ok, snapshot_ok, missing = _ads_endpoint_status()
        if not (daily_ok and snapshot_ok):
            print(f"ads_endpoints_not_configured missing={','.join(missing)}")
            raise typer.Exit(code=1)

    attach_report_html_env = os.environ.get("DISCORD_ATTACH_REPORT_HTML", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_html_effective = bool(discord_attach_report_html or attach_report_html_env)
    attach_report_zip_env = os.environ.get("DISCORD_ATTACH_REPORT_ZIP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_zip_effective = bool(discord_attach_report_zip or attach_report_zip_env)
    attach_report_md_env = os.environ.get("DISCORD_ATTACH_REPORT_MD", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_md_effective = bool(discord_attach_report_md or attach_report_md_env)
    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir
        get_settings.cache_clear()

    if artifacts_root:
        os.environ["FAILURE_ARTIFACTS_ROOT"] = artifacts_root

    token_mode_value = _normalize_token_mode(token_mode)
    anchor_date = _parse_required_date(date_value)
    settings = get_settings()

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)
    shops_value = ",".join([shop.shop_key for shop in target_shops])

    token_file = _coerce_option_value(token_file, None)
    token_sync = _coerce_option_value(token_sync, True)

    if token_file and token_sync:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        _print_db_token_fingerprints(target_shops)
    elif not token_file:
        _phase1_db_preflight_or_exit(
            shops=shops_value,
            min_access_ttl_sec=120,
            command_name="phase1_schedule_run_once",
        )

    fixtures_path = Path(fixtures_dir) if transport_value == "fixtures" else None
    if fixtures_path and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    anchor_dt = datetime.combine(anchor_date, time(13, 0), tzinfo=tz)
    ingest_date = anchor_date
    window_start = anchor_date
    window_end = anchor_date
    as_of = None
    week_id_value = None
    if job_value == "daily-final":
        report_date = anchor_date - timedelta(days=1)
        ingest_date = report_date
        window_start = report_date
        window_end = report_date
    elif job_value == "daily-midday":
        report_date = anchor_date
        ingest_date = report_date
        window_start = report_date
        window_end = report_date
        as_of = datetime.combine(report_date, time(13, 0), tzinfo=tz)
    else:
        window_start, window_end = get_last_week_range(anchor_dt, tz)
        ingest_date = window_end
        week_id_value = weekly_id(window_start)

    print(
        "phase1_schedule_run_once_start "
        f"job={job_value} date={anchor_date.isoformat()} "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} plan_path={plan_path_value} mapping_path={mapping} "
        f"reports_dir={reports_dir} artifacts_root={artifacts_root} token_mode={token_mode_value} "
        f"discord_attach_report_html={1 if attach_report_html_effective else 0} "
        f"discord_attach_report_zip={1 if attach_report_zip_effective else 0} "
        f"discord_attach_report_md={1 if attach_report_md_effective else 0}"
    )
    print(
        "computed_report_window "
        f"job={job_value} start={window_start.isoformat()} end={window_end.isoformat()} "
        f"ingest_date={ingest_date.isoformat()} "
        f"as_of={as_of.isoformat() if as_of else '-'} "
        f"week_id={week_id_value or '-'}"
    )

    if transport_value == "live":
        _require_shopee_settings(settings)
        if token_mode_value == "passive":
            for shop_cfg in target_shops:
                _check_token_passive_or_exit(shop_cfg)

    result = _phase1_schedule_run_once(
        settings=settings,
        shops=target_shops,
        job=job_value,
        anchor_date=anchor_date,
        transport=transport_value,
        allow_network=allow_network_effective,
        token_mode=token_mode_value,
        plan_path=Path(plan_path_value),
        mapping_path=Path(mapping),
        fixtures_dir=fixtures_path,
        save_failure_artifacts=save_failure_artifacts,
        send_discord=send_discord,
        discord_attach_report_html=attach_report_html_effective,
        discord_attach_report_zip=attach_report_zip_effective,
        discord_attach_report_md=attach_report_md_effective,
    )

    totals = result.get("totals") or {}
    per_shop = result.get("per_shop") or {}
    for shop_key in sorted(per_shop.keys()):
        row = per_shop.get(shop_key) or {}
        if "calls_ok" in row:
            print(
                f"shop={shop_key} ingest_date={result.get('ingest_date')} "
                f"calls_ok={row.get('calls_ok', 0)} calls_fail={row.get('calls_fail', 0)}"
            )
            print(
                f"upserted campaigns={row.get('campaigns', 0)} "
                f"daily={row.get('daily', 0)} snapshots={row.get('snapshots', 0)}"
            )
        report_path = row.get("report_path")
        if report_path:
            print(f"report_path shop={shop_key} path={report_path}")
        if row.get("error"):
            print(f"shop_error shop={shop_key} error={row.get('error')}")

    print(
        "total "
        f"calls_ok={totals.get('calls_ok', 0)} calls_fail={totals.get('calls_fail', 0)} "
        f"campaigns={totals.get('campaigns', 0)} daily={totals.get('daily', 0)} "
        f"snapshots={totals.get('snapshots', 0)}"
    )

    if support_packet:
        ingest_date_str = str(result.get("ingest_date") or "")
        if not ingest_date_str:
            print("support_packet_ok=0 error=missing_ingest_date")
            raise typer.Exit(code=1)
        packet = _build_phase1_schedule_support_packet(
            job=job_value,
            anchor_date=anchor_date,
            ingest_date=date.fromisoformat(ingest_date_str),
            artifacts_root=Path(artifacts_root),
            per_shop=per_shop,
        )
        if packet.get("ok"):
            print(f"support_packet_saved={packet.get('zip_path')} files={packet.get('files')}")
            print("support_packet_ok=1")
        else:
            print(f"support_packet_ok=0 error={packet.get('error')}")
            raise typer.Exit(code=1)

    ok = int(result.get("ok") or 0)
    print(f"phase1_schedule_run_once_ok={ok}")
    if not ok:
        failures = result.get("failures") or {}
        if failures:
            keys = ",".join(sorted([str(k) for k in failures.keys()]))
            print(f"failures shops={keys}")
        raise typer.Exit(code=1)


@ops_phase1_alerts_app.command("run-once")
def ops_phase1_alerts_run_once(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    db: str = typer.Option(..., "--db", help="SQLite DB path"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads_alerts/open", "--fixtures-dir", help="Fixtures directory"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    token_mode: str = typer.Option("passive", "--token-mode", help="default | passive"),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_sync: bool = typer.Option(
        True, "--token-sync/--no-token-sync", help="Sync tokens from file into DB"
    ),
    plan: str = typer.Option(
        "collaboration/plans/ads_ingest_alerts.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml",
        "--mapping",
        help="Mapping YAML path",
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    save_artifacts: bool = typer.Option(
        False,
        "--save-artifacts/--no-save-artifacts",
        help="Save full artifacts (success+fail) under artifacts-root",
    ),
    save_failure_artifacts: bool = typer.Option(
        True, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO datetime (default: now; fixtures default is fixed)"
    ),
    cooldown_minutes: int | None = typer.Option(
        None, "--cooldown-minutes", help="Override cooldown minutes"
    ),
    notify_resolved: bool = typer.Option(
        True, "--notify-resolved/--no-notify-resolved"
    ),
    send_discord: bool = typer.Option(
        True, "--send-discord/--no-send-discord"
    ),
) -> None:
    _maybe_load_env_file(env_file)

    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    db_path = Path(db)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    get_settings.cache_clear()

    if artifacts_root:
        os.environ["FAILURE_ARTIFACTS_ROOT"] = artifacts_root

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = allow_network or allow_network_env
    if transport_value == "live" and not allow_network_effective:
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    allow_send_in_fixtures = os.environ.get("DISCORD_ALLOW_SEND_IN_FIXTURES", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if (
        transport_value == "fixtures"
        and send_discord
        and "DISCORD_DRY_RUN" not in os.environ
        and not allow_send_in_fixtures
    ):
        # Fixtures run must never send network requests unless explicitly overridden
        # by go-live rehearsal tests.
        os.environ["DISCORD_DRY_RUN"] = "1"

    settings = get_settings()
    token_mode_value = _normalize_token_mode(token_mode)

    if transport_value == "live":
        snapshot_ok, missing = _ads_snapshot_endpoint_status()
        if not snapshot_ok:
            print(f"ads_endpoints_not_configured missing={','.join(missing)}")
            raise typer.Exit(code=1)
        _require_shopee_settings(settings)

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    token_file = _coerce_option_value(token_file, None)
    token_sync = _coerce_option_value(token_sync, True)
    if token_file and token_sync:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        _print_db_token_fingerprints(target_shops)

    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    if as_of:
        parsed = datetime.fromisoformat(as_of)
        as_of_dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)
    else:
        if transport_value == "fixtures":
            as_of_dt = datetime(2026, 2, 3, 14, 0, tzinfo=tz)
        else:
            as_of_dt = datetime.now(tz)

    fixtures_dir_value = fixtures_dir
    if transport_value == "fixtures":
        # QoL: if operator didn't provide `--fixtures-dir`, choose pacing fixtures automatically
        # based on DB filename so the task/runbook commands stay short.
        def _norm(p: str) -> str:
            return str(p).replace("\\", "/").strip().lower()

        if _norm(fixtures_dir_value) == _norm("tests/fixtures/shopee_ads_alerts/open"):
            if "pacing" in db_path.name.lower():
                fixtures_dir_value = "tests/fixtures/shopee_ads_alerts_pacing/open"

    fixtures_path = Path(fixtures_dir_value) if transport_value == "fixtures" else None
    if fixtures_path and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    cooldown_value = int(cooldown_minutes) if cooldown_minutes is not None else int(settings.alert_cooldown_minutes)

    print(
        "phase1_alerts_run_once_start "
        f"as_of={as_of_dt.isoformat()} shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} db={db_path} "
        f"plan_path={plan} mapping_path={mapping} token_mode={token_mode_value}"
    )
    print(f"computed_alert_window as_of={as_of_dt.isoformat()}")

    if transport_value == "live" and token_mode_value == "passive":
        for shop_cfg in target_shops:
            _check_token_passive_or_exit(shop_cfg)

    result = _phase1_alerts_run_once(
        settings=settings,
        shops=target_shops,
        as_of=as_of_dt,
        transport=transport_value,
        allow_network=allow_network_effective,
        token_mode=token_mode_value,
        plan_path=Path(plan),
        mapping_path=Path(mapping),
        fixtures_dir=fixtures_path,
        save_artifacts=save_artifacts,
        save_failure_artifacts=save_failure_artifacts,
        send_discord=send_discord,
        notify_resolved=notify_resolved,
        cooldown_minutes=cooldown_value,
    )

    per_shop = result.get("per_shop") or {}
    for shop_key in sorted(per_shop.keys()):
        row = per_shop.get(shop_key) or {}
        if "calls_ok" in row:
            print(
                f"shop={shop_key} calls_ok={row.get('calls_ok', 0)} calls_fail={row.get('calls_fail', 0)} "
                f"campaigns={row.get('campaigns', 0)} daily={row.get('daily', 0)} snapshots={row.get('snapshots', 0)}"
            )
        counts = row.get("alerts_counts") if isinstance(row, dict) else None
        if isinstance(counts, dict):
            print(
                "alerts "
                f"shop={shop_key} active={counts.get('active', 0)} opened={counts.get('opened', 0)} "
                f"updated={counts.get('updated', 0)} resolved={counts.get('resolved', 0)} "
                f"notified={counts.get('notified', 0)} suppressed={counts.get('suppressed', 0)}"
            )
        if row.get("error"):
            print(f"shop_error shop={shop_key} error={row.get('error')}")

    # Coverage metric: do we have budgets available for pacing alerts?
    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            cov = _compute_budget_coverage(
                session,
                shop_cfg.shop_key,
                shop_budget_override=getattr(shop_cfg, "daily_budget_est", None),
            )
            print(
                "budget_coverage "
                f"shop={shop_cfg.shop_key} campaigns_total={cov.get('campaigns_total', 0)} "
                f"with_budget={cov.get('with_budget', 0)} "
                f"with_budget_pos={cov.get('with_budget_pos', 0)} "
                f"pct_budget_pos={float(cov.get('pct', 0.0)):.1f} "
                f"min={_fmt_budget_value(cov.get('min'))} max={_fmt_budget_value(cov.get('max'))} "
                f"source={cov.get('source', '-')}"
            )
            if int(cov.get("campaigns_total", 0)) > 0 and int(cov.get("with_budget_pos", 0)) == 0:
                print(
                    "budget_coverage_warning "
                    f"shop={shop_cfg.shop_key} reason=no_positive_daily_budget"
                )

            scov = _compute_snapshot_coverage(session, shop_cfg.shop_key)
            print(
                "snapshot_coverage "
                f"shop={shop_cfg.shop_key} snapshots_total={scov.get('snapshots_total', 0)} "
                f"with_spend_pos={scov.get('with_spend_pos', 0)} "
                f"pct_spend_pos={float(scov.get('pct_spend_pos', 0.0)):.1f} "
                f"min_spend={_fmt_budget_value(scov.get('min'))} "
                f"max_spend={_fmt_budget_value(scov.get('max'))}"
            )
            if int(scov.get("snapshots_total", 0)) == 0:
                print(
                    "snapshot_coverage_warning "
                    f"shop={shop_cfg.shop_key} reason=no_snapshot_rows"
                )
    finally:
        session.close()

    totals = result.get("alerts_totals") or {}
    print(
        "detectors: "
        f"active={totals.get('active', 0)} opened={totals.get('opened', 0)} updated={totals.get('updated', 0)} "
        f"resolved={totals.get('resolved', 0)} notified={totals.get('notified', 0)} suppressed={totals.get('suppressed', 0)} "
        f"cooldown_sec={cooldown_value * 60}"
    )

    ok = int(result.get("ok") or 0)
    print(f"phase1_alerts_run_once_ok={ok}")
    if not ok:
        failures = result.get("failures") or {}
        if failures:
            keys = ",".join(sorted([str(k) for k in failures.keys()]))
            print(f"failures shops={keys}")
        raise typer.Exit(code=1)


@ops_phase1_alerts_app.command("resolve-fixtures")
def ops_phase1_alerts_resolve_fixtures(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    db: str = typer.Option(..., "--db", help="SQLite DB path"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures only"),
) -> None:
    transport_value = transport.lower().strip()
    if transport_value != "fixtures":
        raise typer.BadParameter("transport must be fixtures")

    db_path = Path(db)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()

    resolved_dir = "tests/fixtures/shopee_ads_alerts/resolved"
    if "pacing" in db_path.name.lower():
        resolved_dir = "tests/fixtures/shopee_ads_alerts_pacing/resolved"

    # Delegate to run-once with resolved fixtures.
    ops_phase1_alerts_run_once(
        shops=shops,
        db=str(db_path),
        env_file=env_file,
        transport="fixtures",
        fixtures_dir=resolved_dir,
        allow_network=False,
        token_mode="passive",
        token_file=None,
        token_sync=False,
        plan="collaboration/plans/ads_ingest_alerts.yaml",
        mapping="collaboration/mappings/ads_mapping.yaml",
        artifacts_root="collaboration/artifacts/shopee_api",
        save_artifacts=False,
        save_failure_artifacts=True,
        as_of=None,
        cooldown_minutes=None,
        notify_resolved=True,
        send_discord=True,
    )


@ops_phase1_alerts_app.command("live-smoke")
def ops_phase1_alerts_live_smoke(
    db: str = typer.Option(
        "collaboration/phase1_live.db", "--db", help="SQLite DB path"
    ),
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        help="Env file path (default: DOTORI_ENV_FILE if set)",
    ),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path (optional)"
    ),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    transport: str = typer.Option("live", "--transport", help="fixtures | live"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads_alerts/open", "--fixtures-dir", help="Fixtures directory"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    min_access_ttl_sec: int = typer.Option(
        900, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    emit_discord: bool = typer.Option(
        False,
        "--emit-discord",
        help="Send real Discord messages (disables DISCORD_DRY_RUN)",
    ),
    send_discord: bool = typer.Option(
        True,
        "--send-discord/--no-send-discord",
        help="Emit alerts messages (dry-run unless --emit-discord)",
    ),
    save_artifacts: bool = typer.Option(
        False,
        "--save-artifacts/--no-save-artifacts",
        help="Save full artifacts (success+fail) for this smoke run",
    ),
    skip_preflight: bool = typer.Option(
        False, "--skip-preflight", help="Skip token TTL preflight"
    ),
) -> None:
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    if transport_value == "live" and not allow_network:
        print("allow_network_required error=pass --allow-network")
        raise typer.Exit(code=1)

    effective_env_file = env_file or os.environ.get("DOTORI_ENV_FILE")
    if effective_env_file:
        _maybe_load_env_file(effective_env_file)

    emit_discord = _coerce_option_value(emit_discord, False)
    send_discord = _coerce_option_value(send_discord, True)
    if not emit_discord:
        # Default: never send real Discord messages in smoke commands.
        os.environ["DISCORD_DRY_RUN"] = "1"
    else:
        os.environ.pop("DISCORD_DRY_RUN", None)
    send_discord_effective = bool(send_discord or emit_discord)

    token_path = None
    token_file_value = _coerce_option_value(token_file, None)
    if token_file_value:
        token_path = Path(str(token_file_value))
        if not token_path.exists():
            print(f"token_file_missing warning=skip_token_sync path={token_path}")
            token_path = None

    skip_preflight = _coerce_option_value(skip_preflight, False)
    if not skip_preflight:
        pre_exit, pre_output = _run_capture_step(
            ops_phase1_token_appsscript_preflight,
            file=str(token_path) if token_path else None,
            env_file=None,
            shops=shops,
            min_access_ttl_sec=min_access_ttl_sec,
            allow_unknown_expiry=False,
        )
        if pre_output:
            _print_captured_output(pre_output)
        if pre_exit != 0:
            if token_path is None:
                print(
                    "next_steps: push/import tokens via POST /ops/phase1/token/import "
                    "or run ops phase1 token appsscript sync --token-file <path> then retry"
                )
            else:
                print(
                    "next_steps: run Apps Script refreshTok(...) and exportShopeeTokensToDrive_Normalized(), "
                    "overwrite token file, then retry"
                )
            raise typer.Exit(code=2 if pre_exit == 2 else pre_exit)

    # Delegate to run-once with live defaults.
    ops_phase1_alerts_run_once(
        shops=shops,
        db=db,
        env_file=effective_env_file,
        transport=transport_value,
        fixtures_dir=fixtures_dir,
        allow_network=(transport_value == "live"),
        token_mode="passive",
        token_file=str(token_path) if token_path else None,
        token_sync=True,
        plan="collaboration/plans/ads_ingest_alerts.yaml",
        mapping="collaboration/mappings/ads_mapping.yaml",
        artifacts_root="collaboration/artifacts/shopee_api",
        save_artifacts=save_artifacts,
        save_failure_artifacts=True,
        as_of=None,
        cooldown_minutes=None,
        notify_resolved=True,
        send_discord=send_discord_effective,
    )


def _parse_report_paths_from_output(output: str, *, job: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("report_path shop="):
            continue
        try:
            body = line.replace("report_path shop=", "", 1)
            shop_key, path_value = body.split(" path=", 1)
        except ValueError:
            continue
        path_text = path_value.strip()
        report_path = Path(path_text)
        exists = report_path.exists()
        size = report_path.stat().st_size if exists else 0
        rows.append(
            {
                "job": job,
                "shop": shop_key.strip(),
                "path": path_text,
                "exists": bool(exists),
                "size": int(size),
            }
        )
    return rows


def _parse_detectors_from_output(output: str) -> dict[str, int]:
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("detectors:"):
            continue
        parts = _parse_kv_tokens(line)
        return {
            "active": _coerce_int(parts.get("active")) or 0,
            "opened": _coerce_int(parts.get("opened")) or 0,
            "updated": _coerce_int(parts.get("updated")) or 0,
            "resolved": _coerce_int(parts.get("resolved")) or 0,
            "notified": _coerce_int(parts.get("notified")) or 0,
            "suppressed": _coerce_int(parts.get("suppressed")) or 0,
        }
    return {}


def _collect_phase1_incident_summary(*, shop_keys: list[str]) -> dict[str, object]:
    init_db()
    session = SessionLocal()
    summary: dict[str, object] = {
        "total": 0,
        "open": 0,
        "resolved": 0,
        "by_shop": {},
    }
    try:
        total = (
            session.query(func.count(AdsIncident.id))
            .filter(AdsIncident.shop_key.in_(shop_keys))
            .scalar()
            or 0
        )
        open_count = (
            session.query(func.count(AdsIncident.id))
            .filter(AdsIncident.shop_key.in_(shop_keys), AdsIncident.status == "OPEN")
            .scalar()
            or 0
        )
        resolved_count = (
            session.query(func.count(AdsIncident.id))
            .filter(AdsIncident.shop_key.in_(shop_keys), AdsIncident.status == "RESOLVED")
            .scalar()
            or 0
        )
        by_shop: dict[str, dict[str, int]] = {}
        for shop_key in shop_keys:
            shop_total = (
                session.query(func.count(AdsIncident.id))
                .filter(AdsIncident.shop_key == shop_key)
                .scalar()
                or 0
            )
            shop_open = (
                session.query(func.count(AdsIncident.id))
                .filter(AdsIncident.shop_key == shop_key, AdsIncident.status == "OPEN")
                .scalar()
                or 0
            )
            shop_resolved = (
                session.query(func.count(AdsIncident.id))
                .filter(
                    AdsIncident.shop_key == shop_key,
                    AdsIncident.status == "RESOLVED",
                )
                .scalar()
                or 0
            )
            by_shop[shop_key] = {
                "total": int(shop_total),
                "open": int(shop_open),
                "resolved": int(shop_resolved),
            }
        summary.update(
            {
                "total": int(total),
                "open": int(open_count),
                "resolved": int(resolved_count),
                "by_shop": by_shop,
            }
        )
    finally:
        session.close()
    return summary


def _collect_phase1_db_row_counts(*, shop_keys: list[str]) -> dict[str, int]:
    init_db()
    session = SessionLocal()
    try:
        return {
            "ads_campaign": int(
                session.query(func.count(AdsCampaign.id))
                .filter(AdsCampaign.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "ads_campaign_daily": int(
                session.query(func.count(AdsCampaignDaily.id))
                .filter(AdsCampaignDaily.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "ads_campaign_snapshot": int(
                session.query(func.count(AdsCampaignSnapshot.id))
                .filter(AdsCampaignSnapshot.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "ads_incident": int(
                session.query(func.count(AdsIncident.id))
                .filter(AdsIncident.shop_key.in_(shop_keys))
                .scalar()
                or 0
            ),
            "event_log": int(session.query(func.count(EventLog.id)).scalar() or 0),
        }
    finally:
        session.close()


def _write_go_live_rehearsal_summary(path: Path, summary: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, default=str),
        encoding="utf-8",
    )
    return path


def _resolve_go_live_bundle_path(
    *,
    bundle_out: str | None,
    target_date: date,
    transport: str,
) -> Path:
    if bundle_out:
        path = Path(bundle_out)
    else:
        path = (
            Path("collaboration")
            / "results"
            / f"phase1_go_live_rehearsal_{target_date.isoformat()}_{transport}.zip"
        )
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def _write_go_live_rehearsal_run_log(
    *,
    path: Path,
    date_value: str,
    transport: str,
    allow_network: bool,
    discord_mode: str,
    outputs: dict[str, str],
) -> Path:
    lines: list[str] = [
        f"phase1_go_live_rehearsal_run date={date_value} transport={transport} "
        f"allow_network={1 if allow_network else 0} discord_mode={discord_mode}",
        "",
    ]
    for section in ["preflight", "alerts", "schedule_midday", "schedule_final"]:
        lines.append(f"[{section}]")
        value = outputs.get(section, "").strip()
        lines.append(value if value else "(no output)")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _build_go_live_rehearsal_bundle(
    *,
    bundle_path: Path,
    summary_path: Path,
    run_log_path: Path,
    report_paths: list[dict[str, object]],
) -> dict[str, object]:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    files_added: list[str] = []
    seen_paths: set[str] = set()
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if summary_path.exists():
            zf.write(summary_path, arcname="summary.json")
            files_added.append("summary.json")
        if run_log_path.exists():
            zf.write(run_log_path, arcname="run.log")
            files_added.append("run.log")
        for row in report_paths:
            raw_path = str(row.get("path") or "").strip()
            if not raw_path:
                continue
            report_path = Path(raw_path)
            report_key = str(report_path.resolve()) if report_path.exists() else raw_path
            if not report_path.exists() or report_key in seen_paths:
                continue
            seen_paths.add(report_key)
            shop = str(row.get("shop") or "shop")
            job = str(row.get("job") or "job")
            arcname = f"reports/{job}/{shop}/{report_path.name}"
            zf.write(report_path, arcname=arcname)
            files_added.append(arcname)
    bundle_size = bundle_path.stat().st_size if bundle_path.exists() else 0
    return {
        "path": str(bundle_path),
        "files": int(len(files_added)),
        "size": int(bundle_size),
        "entries": files_added,
    }


def _phase1_go_live_readiness_gate(
    *,
    settings,
    target_shops: list,
    transport: str,
    allow_network: bool,
    discord_mode: str,
    min_access_ttl_sec: int,
) -> None:
    if transport != "live":
        return
    if not allow_network:
        print("live_transport_requires_allow_network=1")
        raise typer.Exit(code=2)
    if min_access_ttl_sec < 1800:
        print(
            f"live_token_ttl_recommended min_access_ttl_sec={min_access_ttl_sec} "
            "recommended_min=1800"
        )

    daily_ok, snapshot_ok, missing_daily = _ads_endpoint_status()
    snapshot_ok_alerts, missing_alerts = _ads_snapshot_endpoint_status()
    missing = list(dict.fromkeys([*missing_daily, *missing_alerts]))
    if not (daily_ok and snapshot_ok and snapshot_ok_alerts):
        print(f"live_readiness_failed missing={','.join(missing)}")
        raise typer.Exit(code=1)
    if settings.shopee_partner_id is None or not settings.shopee_partner_key:
        print("live_readiness_failed missing=SHOPEE_PARTNER_ID,SHOPEE_PARTNER_KEY")
        raise typer.Exit(code=1)

    if discord_mode == "send":
        missing_webhook_shops: list[str] = []
        for shop_cfg in target_shops:
            report_hook_ok = bool(
                shop_cfg.discord_webhook_url or settings.discord_webhook_report_url
            )
            alerts_hook_ok = bool(
                shop_cfg.discord_webhook_url
                or settings.discord_webhook_alerts_url
                or settings.discord_webhook_report_url
            )
            if not (report_hook_ok and alerts_hook_ok):
                missing_webhook_shops.append(shop_cfg.shop_key)
        if missing_webhook_shops:
            print(
                "live_readiness_failed missing_discord_webhook_shops="
                f"{','.join(missing_webhook_shops)}"
            )
            raise typer.Exit(code=1)
    print("live_readiness_ok=1")


@ops_phase1_go_live_app.command("rehearsal")
def ops_phase1_go_live_rehearsal(
    ctx: typer.Context,
    date_value: str | None = typer.Option(None, "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Phase1 shop keys only"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    allow_network: bool = typer.Option(False, "--allow-network"),
    token_mode: str = typer.Option("passive", "--token-mode", help="passive only"),
    db: str = typer.Option(
        "collaboration/phase1_live.db", "--db", help="SQLite DB path"
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api", "--artifacts-root", help="Artifacts root directory"
    ),
    min_access_ttl_sec: int = typer.Option(
        900, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    strict_preflight: bool = typer.Option(
        False,
        "--strict-preflight/--no-strict-preflight",
        help="Enable token preflight gate auto-pause before runner steps",
    ),
    token_alert_cooldown_sec: int = typer.Option(
        21600,
        "--token-alert-cooldown-sec",
        help="Cooldown seconds for token TTL alerts",
    ),
    token_resolved_cooldown_sec: int = typer.Option(
        21600,
        "--token-resolved-cooldown-sec",
        help="Cooldown seconds for TOKEN_TTL_OK resolved alerts",
    ),
    discord_mode: str = typer.Option(
        "off", "--discord-mode", help="off | dry-run | send"
    ),
    confirm_discord_send: bool = typer.Option(
        False,
        "--confirm-discord-send",
        help="Required safety flag when --discord-mode send",
    ),
    discord_attach_report_html: bool = typer.Option(
        False,
        "--discord-attach-report-html",
        help="Attach generated daily report HTML when sending Discord report messages",
    ),
    discord_attach_report_zip: bool = typer.Option(
        False,
        "--discord-attach-report-zip",
        help="Attach generated daily report ZIP when sending Discord report messages",
    ),
    discord_attach_report_md: bool = typer.Option(
        False,
        "--discord-attach-report-md",
        help="Attach generated daily report Markdown summary when sending Discord report messages",
    ),
    schedule_fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--schedule-fixtures-dir", help="Schedule fixtures directory"
    ),
    alerts_fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads_alerts/open",
        "--alerts-fixtures-dir",
        help="Alerts fixtures directory",
    ),
    summary_out: str = typer.Option(
        "collaboration/tmp/task_078/summary.json", "--summary-out", help="Summary JSON output path"
    ),
    bundle_out: str | None = typer.Option(
        None, "--bundle-out", help="Bundle zip output path"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    strict_preflight_effective = _resolve_bool_option_with_env(
        ctx,
        "strict_preflight",
        current=strict_preflight,
        env_key="DOTORI_STRICT_PREFLIGHT",
        default=False,
    )
    min_access_ttl_sec_effective = _resolve_int_option_with_env(
        ctx,
        "min_access_ttl_sec",
        current=min_access_ttl_sec,
        env_key="DOTORI_MIN_ACCESS_TTL_SEC",
        default=1200,
        minimum=1,
    )
    token_alert_cooldown_sec_effective = _resolve_int_option_with_env(
        ctx,
        "token_alert_cooldown_sec",
        current=token_alert_cooldown_sec,
        env_key="DOTORI_TOKEN_ALERT_COOLDOWN_SEC",
        default=21600,
        minimum=0,
    )
    token_resolved_cooldown_sec_effective = _resolve_int_option_with_env(
        ctx,
        "token_resolved_cooldown_sec",
        current=token_resolved_cooldown_sec,
        env_key="DOTORI_TOKEN_RESOLVED_COOLDOWN_SEC",
        default=21600,
        minimum=0,
    )
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    token_mode_value = _normalize_token_mode(token_mode)
    if token_mode_value != "passive":
        print("go_live_rehearsal_requires_token_mode_passive=1")
        raise typer.Exit(code=2)

    discord_mode_value = discord_mode.lower().strip()
    if discord_mode_value not in {"off", "dry-run", "send"}:
        raise typer.BadParameter("discord-mode must be off, dry-run, or send")
    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = bool(allow_network or allow_network_env)
    attach_report_html_env = os.environ.get("DISCORD_ATTACH_REPORT_HTML", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_html_effective = bool(discord_attach_report_html or attach_report_html_env)
    attach_report_zip_env = os.environ.get("DISCORD_ATTACH_REPORT_ZIP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_zip_effective = bool(discord_attach_report_zip or attach_report_zip_env)
    attach_report_md_env = os.environ.get("DISCORD_ATTACH_REPORT_MD", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_md_effective = bool(discord_attach_report_md or attach_report_md_env)

    if discord_mode_value == "send":
        if not allow_network_effective:
            print("discord_send_requires_allow_network=1")
            raise typer.Exit(code=2)
        if not confirm_discord_send:
            print("discord_send_requires_confirm=1")
            raise typer.Exit(code=2)

    if transport_value == "live" and not allow_network_effective:
        print("live_transport_requires_allow_network=1")
        raise typer.Exit(code=2)

    db_path = Path(db)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir
    if artifacts_root:
        os.environ["FAILURE_ARTIFACTS_ROOT"] = artifacts_root
    if discord_mode_value == "send":
        os.environ.pop("DISCORD_DRY_RUN", None)
        if transport_value == "fixtures":
            os.environ["DISCORD_ALLOW_SEND_IN_FIXTURES"] = "1"
        else:
            os.environ.pop("DISCORD_ALLOW_SEND_IN_FIXTURES", None)
    else:
        os.environ["DISCORD_DRY_RUN"] = "1"
        os.environ.pop("DISCORD_ALLOW_SEND_IN_FIXTURES", None)
    get_settings.cache_clear()
    settings = get_settings()

    target_date = _parse_date_or_today(date_value)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_phase1_shops_only(shops, target_shops)
    _ensure_shop_ids(target_shops)
    _phase1_go_live_readiness_gate(
        settings=settings,
        target_shops=target_shops,
        transport=transport_value,
        allow_network=allow_network_effective,
        discord_mode=discord_mode_value,
        min_access_ttl_sec=min_access_ttl_sec_effective,
    )
    shop_keys = [shop.shop_key for shop in target_shops]
    shops_value = ",".join(shop_keys)

    summary_path = Path(summary_out)
    if not summary_path.is_absolute():
        summary_path = (Path.cwd() / summary_path).resolve()

    token_status_rows, token_status_ok = _collect_db_token_status_rows(
        target_shops=target_shops,
        min_access_ttl_sec=min_access_ttl_sec_effective,
    )
    summary: dict[str, object] = {
        "date": target_date.isoformat(),
        "shops": shop_keys,
        "transport": transport_value,
        "allow_network": int(allow_network_effective),
        "token_mode": token_mode_value,
        "discord_mode": discord_mode_value,
        "discord_attach_report_html": int(attach_report_html_effective),
        "discord_attach_report_zip": int(attach_report_zip_effective),
        "discord_attach_report_md": int(attach_report_md_effective),
        "db": str(db_path),
        "reports_dir": reports_dir,
        "artifacts_root": artifacts_root,
        "token_source": "db",
        "strict_preflight": int(strict_preflight_effective),
        "min_access_ttl_sec": int(min_access_ttl_sec_effective),
        "token_alert_cooldown_sec": int(token_alert_cooldown_sec_effective),
        "token_resolved_cooldown_sec": int(token_resolved_cooldown_sec_effective),
        "token_status_ok": bool(token_status_ok),
        "token_status_rows": token_status_rows,
        "preflight": {},
        "token_alerts": {},
        "token_resolved": {},
        "alerts": {},
        "reports": {
            "midday_ok": False,
            "final_ok": False,
            "report_paths": [],
        },
        "report_paths": [],
        "incidents": {},
        "db_row_counts": {},
        "bundle": {},
        "skipped_due_to_token": False,
        "ok": False,
    }

    print(
        "phase1_go_live_rehearsal_start "
        f"date={target_date.isoformat()} shops={shops_value} "
        f"transport={transport_value} allow_network={1 if allow_network_effective else 0} "
        f"token_mode={token_mode_value} discord_mode={discord_mode_value} "
        f"strict_preflight={1 if strict_preflight_effective else 0} "
        f"min_access_ttl_sec={min_access_ttl_sec_effective} "
        f"token_alert_cooldown_sec={token_alert_cooldown_sec_effective} "
        f"token_resolved_cooldown_sec={token_resolved_cooldown_sec_effective} "
        f"discord_attach_report_html={1 if attach_report_html_effective else 0} "
        f"discord_attach_report_zip={1 if attach_report_zip_effective else 0} "
        f"discord_attach_report_md={1 if attach_report_md_effective else 0}"
    )

    run_outputs: dict[str, str] = {}
    send_discord_enabled = discord_mode_value in {"dry-run", "send"}

    if strict_preflight_effective:
        gate_result = evaluate_token_preflight_gate(
            shops=target_shops,
            min_access_ttl_sec=min_access_ttl_sec_effective,
        )
        for row in gate_result.get("rows") or []:
            if not isinstance(row, dict):
                continue
            print(
                "preflight_gate_row "
                f"shop={row.get('shop_key')} "
                f"shop_id={row.get('shop_id')} "
                f"token_verdict={row.get('token_verdict')} "
                f"access_expires_in_sec={row.get('access_expires_in_sec')} "
                f"min_access_ttl_sec={row.get('min_access_ttl_sec')}"
            )
        summary["preflight"] = gate_result
        preflight_gate_ok = bool(gate_result.get("ok"))
        print(
            "preflight_gate "
            f"ok={1 if preflight_gate_ok else 0} "
            f"reason={gate_result.get('reason', '-')}"
        )
        print(f"preflight_gate_ok={1 if preflight_gate_ok else 0}")
        resolved_alerts: dict[str, object] = {}
        if not preflight_gate_ok:
            token_alerts = emit_token_ttl_alerts_with_cooldown(
                shops=target_shops,
                gate_result=gate_result,
                cooldown_sec=token_alert_cooldown_sec_effective,
                send_discord=send_discord_enabled,
            )
            summary["token_alerts"] = token_alerts
            for row in token_alerts.get("rows") or []:
                if not isinstance(row, dict):
                    continue
                shop_key = str(row.get("shop_key") or "-")
                cooldown_until = str(row.get("cooldown_until_utc") or "-")
                if int(row.get("suppressed") or 0) == 1:
                    print(
                        "discord_token_alert_cooldown_skip=1 "
                        f"shop={shop_key} cooldown_until_utc={cooldown_until}"
                    )
                    continue
                if int(row.get("dry_run") or 0) == 1:
                    print(
                        "discord_token_alert_dry_run=1 "
                        f"shop={shop_key} cooldown_until_utc={cooldown_until}"
                    )
                elif send_discord_enabled:
                    print(
                        "discord_token_alert_sent=1 "
                        f"shop={shop_key} cooldown_until_utc={cooldown_until}"
                    )
                else:
                    print(
                        "discord_token_alert_skipped=1 "
                        f"shop={shop_key} reason=send_disabled"
                    )
            artifact_paths = write_token_preflight_gate_artifacts(
                base_dir=summary_path.parent,
                gate_result=gate_result,
                alert_result=token_alerts,
                resolved_result=resolved_alerts,
            )
            summary["preflight_gate_artifacts"] = artifact_paths
            summary["skipped_due_to_token"] = True
            summary["ok"] = False
            summary["reason"] = "skipped_due_to_token"
            _write_go_live_rehearsal_summary(summary_path, summary)
            print(f"preflight_gate_summary_json={artifact_paths.get('json_path')}")
            print(f"preflight_gate_summary_md={artifact_paths.get('md_path')}")
            print("skipped_due_to_token=1")
            print("planned_calls_in_fail=0")
            print(f"summary_path={summary_path}")
            print("phase1_go_live_rehearsal_ok=0 reason=skipped_due_to_token")
            return
        resolved_alerts = emit_token_resolved_alerts_with_cooldown(
            shops=target_shops,
            gate_result=gate_result,
            cooldown_sec=token_resolved_cooldown_sec_effective,
            send_discord=send_discord_enabled,
        )
        summary["token_resolved"] = resolved_alerts
        for row in resolved_alerts.get("rows") or []:
            if not isinstance(row, dict):
                continue
            if int(row.get("transitioned_from_blocked") or 0) != 1:
                continue
            shop_key = str(row.get("shop_key") or "-")
            cooldown_until = str(row.get("resolved_cooldown_until_utc") or "-")
            if int(row.get("suppressed") or 0) == 1:
                print(
                    "discord_token_resolved_cooldown_skip=1 "
                    f"shop={shop_key} resolved_cooldown_until_utc={cooldown_until}"
                )
                continue
            if int(row.get("dry_run") or 0) == 1:
                print(
                    "discord_token_resolved_dry_run=1 "
                    f"shop={shop_key} resolved_cooldown_until_utc={cooldown_until}"
                )
            elif send_discord_enabled:
                http_status = int(row.get("http_status") or -1)
                print(
                    "discord_token_resolved_send_ok=1 "
                    f"shop={shop_key} http_status={http_status} "
                    f"resolved_cooldown_until_utc={cooldown_until}"
                )
            else:
                print(
                    "discord_token_resolved_skipped=1 "
                    f"shop={shop_key} reason=send_disabled"
                )
        artifact_paths = write_token_preflight_gate_artifacts(
            base_dir=summary_path.parent,
            gate_result=gate_result,
            alert_result={},
            resolved_result=resolved_alerts,
        )
        summary["preflight_gate_artifacts"] = artifact_paths
        print(f"preflight_gate_summary_json={artifact_paths.get('json_path')}")
        print(f"preflight_gate_summary_md={artifact_paths.get('md_path')}")
        summary["skipped_due_to_token"] = False
        print("skipped_due_to_token=0")
    else:
        pre_exit, pre_output = _run_capture_step(
            ops_phase1_token_appsscript_preflight,
            file=None,
            env_file=None,
            shops=shops_value,
            min_access_ttl_sec=min_access_ttl_sec_effective,
            allow_unknown_expiry=False,
        )
        run_outputs["preflight"] = pre_output
        if pre_output:
            _print_captured_output(pre_output)
        preflight_info = _parse_preflight_output(pre_output)
        summary["preflight"] = {
            "exit_code": int(pre_exit),
            "ok": bool(preflight_info.get("ok")),
            "rows": preflight_info.get("rows") or {},
        }
        if pre_exit != 0:
            summary["ok"] = False
            summary["reason"] = "preflight_failed"
            _write_go_live_rehearsal_summary(summary_path, summary)
            print(f"summary_path={summary_path}")
            print("phase1_go_live_rehearsal_ok=0 reason=preflight_failed")
            raise typer.Exit(code=2)

    alerts_exit, alerts_output = _run_capture_step(
        ops_phase1_alerts_run_once,
        shops=shops_value,
        db=str(db_path),
        env_file=None,
        transport=transport_value,
        fixtures_dir=alerts_fixtures_dir,
        allow_network=allow_network_effective,
        token_mode=token_mode_value,
        token_file=None,
        token_sync=False,
        plan="collaboration/plans/ads_ingest_alerts.yaml",
        mapping="collaboration/mappings/ads_mapping.yaml",
        artifacts_root=artifacts_root,
        save_artifacts=False,
        save_failure_artifacts=True,
        as_of=None,
        cooldown_minutes=None,
        notify_resolved=True,
        send_discord=send_discord_enabled,
    )
    run_outputs["alerts"] = alerts_output
    if alerts_output:
        _print_captured_output(alerts_output)
    alerts_ok = alerts_exit == 0 and "phase1_alerts_run_once_ok=1" in alerts_output
    summary["alerts"] = {
        "exit_code": int(alerts_exit),
        "ok": bool(alerts_ok),
        "detectors": _parse_detectors_from_output(alerts_output),
    }
    if not alerts_ok:
        summary["ok"] = False
        summary["reason"] = "alerts_failed"
        summary["incidents"] = _collect_phase1_incident_summary(shop_keys=shop_keys)
        summary["db_row_counts"] = _collect_phase1_db_row_counts(shop_keys=shop_keys)
        _write_go_live_rehearsal_summary(summary_path, summary)
        print(f"summary_path={summary_path}")
        print("phase1_go_live_rehearsal_ok=0 reason=alerts_failed")
        raise typer.Exit(code=1)

    midday_exit, midday_output = _run_capture_step(
        ops_phase1_schedule_run_once,
        job="daily-midday",
        date_value=target_date.isoformat(),
        shops=shops_value,
        env_file=None,
        transport=transport_value,
        fixtures_dir=schedule_fixtures_dir,
        allow_network=allow_network_effective,
        token_mode=token_mode_value,
        token_file=None,
        token_sync=False,
        plan=None,
        mapping="collaboration/mappings/ads_mapping.yaml",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
        save_failure_artifacts=True,
        support_packet=False,
        send_discord=send_discord_enabled,
        discord_attach_report_html=attach_report_html_effective,
        discord_attach_report_zip=attach_report_zip_effective,
        discord_attach_report_md=attach_report_md_effective,
    )
    run_outputs["schedule_midday"] = midday_output
    if midday_output:
        _print_captured_output(midday_output)
    final_exit, final_output = _run_capture_step(
        ops_phase1_schedule_run_once,
        job="daily-final",
        date_value=target_date.isoformat(),
        shops=shops_value,
        env_file=None,
        transport=transport_value,
        fixtures_dir=schedule_fixtures_dir,
        allow_network=allow_network_effective,
        token_mode=token_mode_value,
        token_file=None,
        token_sync=False,
        plan=None,
        mapping="collaboration/mappings/ads_mapping.yaml",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
        save_failure_artifacts=True,
        support_packet=False,
        send_discord=send_discord_enabled,
        discord_attach_report_html=attach_report_html_effective,
        discord_attach_report_zip=attach_report_zip_effective,
        discord_attach_report_md=attach_report_md_effective,
    )
    run_outputs["schedule_final"] = final_output
    if final_output:
        _print_captured_output(final_output)

    midday_ok = midday_exit == 0 and "phase1_schedule_run_once_ok=1" in midday_output
    final_ok = final_exit == 0 and "phase1_schedule_run_once_ok=1" in final_output
    report_paths = _parse_report_paths_from_output(midday_output, job="daily-midday")
    report_paths.extend(_parse_report_paths_from_output(final_output, job="daily-final"))
    summary["reports"] = {
        "midday_exit_code": int(midday_exit),
        "final_exit_code": int(final_exit),
        "midday_ok": bool(midday_ok),
        "final_ok": bool(final_ok),
        "report_paths": report_paths,
    }
    summary["report_paths"] = report_paths
    summary["incidents"] = _collect_phase1_incident_summary(shop_keys=shop_keys)
    summary["db_row_counts"] = _collect_phase1_db_row_counts(shop_keys=shop_keys)

    overall_ok = bool(midday_ok and final_ok)
    summary["ok"] = overall_ok
    if not overall_ok:
        summary["reason"] = "reports_failed"

    if overall_ok:
        run_log_path = summary_path.parent / "run.log"
        _write_go_live_rehearsal_run_log(
            path=run_log_path,
            date_value=target_date.isoformat(),
            transport=transport_value,
            allow_network=allow_network_effective,
            discord_mode=discord_mode_value,
            outputs=run_outputs,
        )
        _write_go_live_rehearsal_summary(summary_path, summary)
        bundle_path = _resolve_go_live_bundle_path(
            bundle_out=bundle_out, target_date=target_date, transport=transport_value
        )
        bundle_info = _build_go_live_rehearsal_bundle(
            bundle_path=bundle_path,
            summary_path=summary_path,
            run_log_path=run_log_path,
            report_paths=report_paths,
        )
        summary["bundle"] = bundle_info
        _write_go_live_rehearsal_summary(summary_path, summary)
        print(f"zip_path={bundle_info.get('path')}")
        print(f"bundle_path={bundle_info.get('path')}")
        print(f"bundle_files={bundle_info.get('files')}")
        print(f"bundle_size={bundle_info.get('size')}")
    else:
        _write_go_live_rehearsal_summary(summary_path, summary)

    print(f"summary_path={summary_path}")
    print(f"phase1_go_live_rehearsal_ok={1 if overall_ok else 0}")
    if not overall_ok:
        raise typer.Exit(code=1)


@ops_phase1_app.command("run-all")
def ops_phase1_run_all(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "passive", "--token-mode", help="default | passive"
    ),
    min_access_ttl_sec: int = typer.Option(
        600, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    allow_unknown_expiry: bool = typer.Option(
        False, "--allow-unknown-expiry", help="Allow unknown access expiry"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    token_sync: bool = typer.Option(
        True, "--token-sync/--no-token-sync", help="Sync tokens from file into DB"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    candidates: str = typer.Option(
        "collaboration/endpoints/ads_candidates.yaml",
        "--candidates",
        help="Candidates YAML path (evidence sweep)",
    ),
    plan: str = typer.Option(
        "collaboration/plans/ads_ingest_minimal.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml",
        "--mapping",
        help="Mapping YAML path",
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    save_failure_artifacts: bool = typer.Option(
        True, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    evidence_out: str | None = typer.Option(
        None, "--evidence-out", help="Evidence report markdown path"
    ),
    failures_out: str | None = typer.Option(
        None, "--failures-out", help="Failures summary markdown path"
    ),
    support_packet: bool = typer.Option(
        True, "--support-packet/--no-support-packet"
    ),
    support_zip: str | None = typer.Option(
        None, "--support-zip", help="Support packet zip path"
    ),
    support_md: str | None = typer.Option(
        None, "--support-md", help="Support request markdown path"
    ),
    support_max_request_ids: int = typer.Option(
        50, "--support-max-request-ids", help="Max request_id entries"
    ),
    support_no_scan: bool = typer.Option(False, "--support-no-scan"),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
) -> None:
    token_file = _coerce_option_value(token_file, None)
    env_file = _coerce_option_value(env_file, None)
    token_sync = _coerce_option_value(token_sync, True)
    allow_unknown_expiry = _coerce_option_value(allow_unknown_expiry, False)
    allow_network = _coerce_option_value(allow_network, False)
    save_failure_artifacts = _coerce_option_value(save_failure_artifacts, True)
    support_packet = _coerce_option_value(support_packet, True)
    support_zip = _coerce_option_value(support_zip, None)
    support_md = _coerce_option_value(support_md, None)
    evidence_out = _coerce_option_value(evidence_out, None)
    failures_out = _coerce_option_value(failures_out, None)
    support_max_request_ids = _coerce_option_value(support_max_request_ids, 50)
    support_no_scan = _coerce_option_value(support_no_scan, False)
    send_discord = _coerce_option_value(send_discord, False)

    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    token_mode_value = _normalize_token_mode(token_mode)
    target_date = _parse_required_date(date_value)
    _maybe_load_env_file(env_file)

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = allow_network or allow_network_env
    if transport_value == "live" and not allow_network_effective:
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    if transport_value == "fixtures" and not token_file:
        default_token = Path("tests") / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"
        token_file = str(default_token)

    if not token_file:
        raise typer.BadParameter("--token-file is required (or use --transport fixtures)")

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    print(
        "phase1_run_all_start "
        f"date={target_date.isoformat()} shops={shops} transport={transport_value} "
        f"token_mode={token_mode_value} min_access_ttl_sec={min_access_ttl_sec}"
    )

    if token_file and token_sync:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        _print_db_token_fingerprints(target_shops)

    pre_exit, pre_output = _run_capture_step(
        ops_phase1_token_appsscript_preflight,
        file=token_file,
        env_file=None,
        shops=shops,
        min_access_ttl_sec=min_access_ttl_sec,
        allow_unknown_expiry=allow_unknown_expiry,
    )
    preflight_info = _parse_preflight_output(pre_output)
    if pre_output:
        _print_captured_output(pre_output)
    if pre_exit != 0:
        print("phase1_run_all_ok=0 reason=preflight_failed")
        print(
            "next_steps: run Apps Script forceRefreshAndExport() then "
            "exportShopeeTokensToDrive_Normalized(), download shopee_tokens_export.json, "
            "overwrite locally, and re-run"
        )
        raise typer.Exit(code=1)

    run_started_ms = int(time_module.time() * 1000)
    print(f"run_started_ms={run_started_ms}")

    evidence_out_path = (
        Path(evidence_out)
        if evidence_out
        else Path("collaboration")
        / "results"
        / f"phase1_evidence_{target_date.isoformat()}.md"
    )
    failures_out_path = (
        Path(failures_out)
        if failures_out
        else Path("collaboration")
        / "results"
        / f"phase1_failures_{target_date.isoformat()}.md"
    )
    support_zip_path = (
        Path(support_zip)
        if support_zip
        else Path("collaboration")
        / "results"
        / f"phase1_support_packet_{target_date.isoformat()}.zip"
    )
    support_md_path = (
        Path(support_md)
        if support_md
        else Path("collaboration")
        / "results"
        / f"phase1_support_request_{target_date.isoformat()}.md"
    )
    evidence_out_path.parent.mkdir(parents=True, exist_ok=True)

    # Evidence: sweep -> summarize -> report -> (optional) support packet
    prev_failure_root = os.environ.get("FAILURE_ARTIFACTS_ROOT")
    sweep_exit = 0
    sweep_output = ""
    try:
        os.environ["FAILURE_ARTIFACTS_ROOT"] = artifacts_root
        sweep_exit, sweep_output = _run_capture_step(
            ops_phase1_ads_endpoint_sweep,
            date_value=target_date.isoformat(),
            shops=shops,
            only_shops=None,
            transport=transport_value,
            token_mode=token_mode_value,
            allow_network=allow_network_effective,
            env_file=None,
            token_file=token_file,
            token_file_format="auto",
            token_import=not token_sync,
            candidates=candidates,
            fixtures_dir=fixtures_dir,
            out_md=None,
            send_discord=False,
            baseline_shop_info=True,
            save_failure_artifacts=save_failure_artifacts,
            auth_debug=False,
        )
    finally:
        if prev_failure_root is None:
            os.environ.pop("FAILURE_ARTIFACTS_ROOT", None)
        else:
            os.environ["FAILURE_ARTIFACTS_ROOT"] = prev_failure_root
    if sweep_output:
        _print_captured_output(sweep_output)
    sweep_ok = (sweep_exit == 0) or ("sweep_ok=1" in (sweep_output or ""))

    summarize_exit, summarize_output = _run_capture_step(
        ops_phase1_artifacts_summarize_failures,
        date_value=target_date.isoformat(),
        shops=shops,
        artifacts_root=artifacts_root,
        out=str(failures_out_path),
        only_prefix=None,
        since_ms=run_started_ms,
    )
    if summarize_output:
        _print_captured_output(summarize_output)
    if summarize_exit != 0:
        print("evidence_ok=0 reason=summarize_failed")
        raise typer.Exit(code=1)

    records = _parse_failure_summary_markdown(failures_out_path)
    verdict_entries = _build_verdict_entries(
        records,
        _parse_only_shops(shops) or [],
        preflight_rows=preflight_info.get("rows") if preflight_info else None,
    )
    summarize_status = _parse_summarize_output(
        summarize_output, failures_out_path
    )
    _write_evidence_report(
        evidence_out_path,
        header={
            "date": target_date.isoformat(),
            "shops": shops,
            "transport": transport_value,
            "token_mode": token_mode_value,
            "min_access_ttl_sec": str(min_access_ttl_sec),
            "allow_network": "1" if allow_network_effective else "0",
            "skip_sweep": "0",
            "skip_preview": "1",
        },
        preflight=preflight_info,
        sweep_status=_parse_sweep_output(sweep_output, skipped=False),
        summary_status=summarize_status,
        verdicts=verdict_entries,
    )
    print(f"evidence_report_saved={evidence_out_path}")

    if not sweep_ok:
        print("evidence_ok=0 reason=sweep_failed")
        raise typer.Exit(code=1)

    if support_packet:
        support_exit, support_output = _run_capture_step(
            ops_phase1_evidence_support_packet,
            date_value=target_date.isoformat(),
            shops=shops,
            artifacts_root=artifacts_root,
            evidence_file=str(evidence_out_path),
            failures_file=str(failures_out_path),
            out_zip=str(support_zip_path),
            out_md=str(support_md_path),
            max_request_ids=support_max_request_ids,
            no_scan=support_no_scan,
        )
        if support_output:
            _print_captured_output(support_output)
        if support_exit != 0:
            print("evidence_ok=0 reason=support_packet_failed")
            raise typer.Exit(code=1)

    print("evidence_ok=1")

    # Preview (HTML)
    prev_exit, prev_output = _run_capture_step(
        ops_phase1_preview,
        date_value=target_date.isoformat(),
        only_shops=shops,
        env_file=None,
        transport=transport_value,
        token_mode=token_mode_value,
        token_file=token_file,
        token_sync=False,
        fixtures_dir=fixtures_dir,
        plan=plan,
        mapping=mapping,
        reports_dir=reports_dir,
        send_discord=send_discord,
        allow_network=allow_network_effective,
        save_failure_artifacts=save_failure_artifacts,
    )
    if prev_output:
        _print_captured_output(prev_output)
    if prev_exit != 0:
        print("preview_ok=0")
        raise typer.Exit(code=1)

    print("preview_ok=1")
    print("phase1_run_all_ok=1")


@ops_phase1_app.command("capture")
def ops_phase1_capture(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "default", "--token-mode", help="default | passive"
    ),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_file_format: str = typer.Option(
        "auto", "--token-file-format", help="auto"
    ),
    token_import: bool = typer.Option(
        True, "--token-import/--no-token-import"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    plan: str = typer.Option(
        "collaboration/plans/ads_ingest_minimal.yaml", "--plan", help="Plan YAML path"
    ),
    mapping: str = typer.Option(
        "collaboration/mappings/ads_mapping.yaml",
        "--mapping",
        help="Mapping YAML path",
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-dir",
        help="Artifacts root directory",
    ),
    out_md: str | None = typer.Option(
        None, "--out-md", help="Capture markdown path"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
    probe_on_failure: bool = typer.Option(
        True, "--probe-on-failure/--no-probe-on-failure"
    ),
    analyze: bool = typer.Option(True, "--analyze/--no-analyze"),
    token_db_auto: bool = typer.Option(
        True, "--token-db-auto/--no-token-db-auto"
    ),
) -> None:
    target_date = _parse_required_date(date_value)
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    token_mode = _coerce_option_value(token_mode, "passive")
    token_mode_value = _normalize_token_mode(token_mode)
    if token_file_format.lower().strip() != "auto":
        raise typer.BadParameter("token-file-format must be auto")

    detected_env = _detect_env_file(env_file)
    loaded_keys: list[str] = []
    if detected_env:
        loaded = load_env_file(detected_env)
        loaded_keys = sorted(loaded.keys())
        get_settings.cache_clear()
        print(f"env_file_loaded path={detected_env} keys={len(loaded)}")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, only_shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    fixtures_path = Path(fixtures_dir)
    if transport_value == "fixtures" and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    capture_path = (
        Path(out_md)
        if out_md
        else Path("collaboration")
        / "results"
        / f"phase1_capture_{target_date.isoformat()}_{transport_value}.md"
    )
    capture_path.parent.mkdir(parents=True, exist_ok=True)

    capture_lines: list[str] = []
    capture_lines.append("# Phase 1 Capture")
    capture_lines.append("")
    capture_lines.append(f"timestamp: {datetime.now().isoformat()}")
    capture_lines.append(f"date: {target_date.isoformat()}")
    capture_lines.append(f"shops: {','.join([shop.shop_key for shop in target_shops])}")
    capture_lines.append(f"transport: {transport_value}")
    capture_lines.append(f"token_mode: {token_mode_value}")
    if token_file:
        capture_lines.append(f"token_file: {token_file}")
        capture_lines.append(f"token_file_format: {token_file_format}")
    capture_lines.append(f"env_file_detected: {detected_env or '-'}")
    if loaded_keys:
        capture_lines.append(f"loaded_keys: {', '.join(loaded_keys)}")
    capture_lines.append(f"allow_network: {1 if allow_network else 0}")
    capture_lines.append(f"send_discord: {1 if send_discord else 0}")
    capture_lines.append(f"probe_on_failure: {1 if probe_on_failure else 0}")
    capture_lines.append(f"reports_dir: {reports_dir}")
    capture_lines.append(f"plan_path: {plan}")
    capture_lines.append(f"mapping_path: {mapping}")
    capture_lines.append("")

    overall_ok = True

    if token_file and token_import:
        import_exit, import_output = _run_capture_step(
            ops_phase1_token_appsscript_import,
            file=token_file,
            env_file=None,
            shops=only_shops,
        )
        if import_output:
            _print_captured_output(import_output)
        if import_exit != 0:
            overall_ok = False
        _append_capture_section(capture_lines, "token_import", import_output)

    verify_exit, verify_output = _run_capture_step(
        ops_phase1_verify,
        shops=only_shops,
        send_discord=False,
        ping_live=False,
        allow_network=False,
        channel="both",
        transport="fixtures",
        fixtures_dir=fixtures_dir,
        database_url=None,
        env_file=None,
    )
    if verify_exit != 0:
        overall_ok = False
    _append_capture_section(capture_lines, "verify", verify_output)

    _, _ready, missing = _compute_phase1_readiness(
        target_shops, settings, require_ads_endpoints=False
    )
    token_missing = any(
        item.endswith("_token_access") or item.endswith("_token_refresh")
        for item in missing
    )
    if token_db_auto and token_missing:
        token_lines, recommended = _scan_token_dbs(
            only_shops=_parse_only_shops(only_shops) or [],
            scan_root=None,
        )
        for line in token_lines:
            print(line)
        if recommended:
            print(f"recommended_database_url={recommended}")
        else:
            print("recommended_database_url=-")
        token_output = "\n".join(token_lines + [
            f"recommended_database_url={recommended or '-'}"
        ])
        _append_capture_section(capture_lines, "token_db_find", token_output)

    preview_exit, preview_output = _run_capture_step(
        ops_phase1_preview,
        date_value=target_date.isoformat(),
        only_shops=only_shops,
        env_file=None,
        transport=transport_value,
        token_mode=token_mode_value,
        fixtures_dir=fixtures_dir,
        plan=plan,
        mapping=mapping,
        reports_dir=reports_dir,
        send_discord=send_discord,
        allow_network=allow_network,
    )
    if preview_exit != 0:
        overall_ok = False
    _append_capture_section(capture_lines, "preview", preview_output)

    ran_probe = False
    if (
        transport_value == "live"
        and allow_network
        and preview_exit != 0
        and probe_on_failure
        and "network_disabled" not in preview_output
        and "ads_endpoints_not_configured" not in preview_output
    ):
        probe_exit, probe_output = _run_capture_step(
            ops_phase1_ads_probe,
            date_value=target_date.isoformat(),
            shops=only_shops,
            only_shops=None,
            env_file=None,
            transport=transport_value,
            token_mode=token_mode_value,
            allow_network=allow_network,
            plan="collaboration/plans/ads_probe_phase1.yaml",
            fixtures_dir=fixtures_dir,
            artifacts_dir=artifacts_dir,
            analyze=analyze,
            analysis_dir="collaboration/probes",
            send_discord=False,
        )
        if probe_exit != 0:
            overall_ok = False
        ran_probe = True
        _append_capture_section(capture_lines, "probe", probe_output)

    capture_lines.append("## conclusion")
    capture_lines.append("SUCCESS" if overall_ok else "FAIL")
    if ran_probe:
        capture_lines.append("probe_on_failure: 1")
    capture_lines.append("")
    capture_path.write_text("\n".join(capture_lines), encoding="utf-8")

    print(f"capture_md path={capture_path}")
    if not overall_ok:
        raise typer.Exit(code=1)


@ops_phase1_token_db_app.command("find")
def ops_phase1_token_db_find(
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    scan_root: str | None = typer.Option(
        None, "--scan-root", help="Override scan root directory"
    ),
) -> None:
    shop_list = _parse_only_shops(only_shops) or []
    lines, recommended = _scan_token_dbs(
        only_shops=shop_list,
        scan_root=scan_root,
    )
    for line in lines:
        print(line)
    if recommended:
        print(f"recommended_database_url={recommended}")
    else:
        print("recommended_database_url=-")


def _resolve_phase1_sqlite_db_path() -> Path:
    settings = get_settings()
    db_url = settings.database_url
    if not db_url.startswith("sqlite:///"):
        raise RuntimeError("DATABASE_URL must be sqlite:///... for this command")
    db_path = db_url[len("sqlite:///") :]
    if db_path in {"", ":memory:"}:
        raise RuntimeError("DATABASE_URL must point to a file-backed sqlite database")
    return Path(db_path)


def _phase1_shop_total_variant_counts(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.cursor()

    def _count(table: str, where_sql: str) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}")
        row = cur.fetchone()
        return int(row[0] if row else 0)

    lower_where = "lower(trim(campaign_id))='shop_total' AND trim(campaign_id)<>'SHOP_TOTAL'"
    upper_where = "trim(campaign_id)='SHOP_TOTAL'"
    return {
        "ads_campaign_shop_total_lower": _count("ads_campaign", lower_where),
        "ads_campaign_shop_total_upper": _count("ads_campaign", upper_where),
        "daily_shop_total_lower": _count("ads_campaign_daily", lower_where),
        "daily_shop_total_upper": _count("ads_campaign_daily", upper_where),
        "snapshot_shop_total_lower": _count("ads_campaign_snapshot", lower_where),
        "snapshot_shop_total_upper": _count("ads_campaign_snapshot", upper_where),
    }


def _format_phase1_shop_total_counts(prefix: str, counts: dict[str, int]) -> str:
    return (
        f"{prefix} "
        f"ads_campaign_shop_total_lower={counts.get('ads_campaign_shop_total_lower', 0)} "
        f"ads_campaign_shop_total_upper={counts.get('ads_campaign_shop_total_upper', 0)} "
        f"daily_shop_total_lower={counts.get('daily_shop_total_lower', 0)} "
        f"daily_shop_total_upper={counts.get('daily_shop_total_upper', 0)} "
        f"snapshot_shop_total_lower={counts.get('snapshot_shop_total_lower', 0)} "
        f"snapshot_shop_total_upper={counts.get('snapshot_shop_total_upper', 0)}"
    )


def _normalize_phase1_shop_total_sqlite(conn: sqlite3.Connection) -> tuple[int, int]:
    cur = conn.cursor()
    rows_updated_total = 0
    rows_deleted_total = 0

    cur.execute(
        """
        DELETE FROM ads_campaign
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
          AND EXISTS (
            SELECT 1
            FROM ads_campaign AS keep
            WHERE keep.shop_key = ads_campaign.shop_key
              AND trim(keep.campaign_id)='SHOP_TOTAL'
          )
        """
    )
    rows_deleted_total += max(cur.rowcount, 0)

    cur.execute(
        """
        UPDATE ads_campaign
        SET campaign_id='SHOP_TOTAL', campaign_name='SHOP_TOTAL'
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
        """
    )
    rows_updated_total += max(cur.rowcount, 0)

    cur.execute(
        """
        UPDATE ads_campaign
        SET campaign_name='SHOP_TOTAL'
        WHERE trim(campaign_id)='SHOP_TOTAL'
          AND coalesce(campaign_name, '')<>'SHOP_TOTAL'
        """
    )
    rows_updated_total += max(cur.rowcount, 0)

    cur.execute(
        """
        DELETE FROM ads_campaign_daily
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
          AND EXISTS (
            SELECT 1
            FROM ads_campaign_daily AS keep
            WHERE keep.shop_key = ads_campaign_daily.shop_key
              AND keep.date = ads_campaign_daily.date
              AND trim(keep.campaign_id)='SHOP_TOTAL'
          )
        """
    )
    rows_deleted_total += max(cur.rowcount, 0)

    cur.execute(
        """
        UPDATE ads_campaign_daily
        SET campaign_id='SHOP_TOTAL'
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
        """
    )
    rows_updated_total += max(cur.rowcount, 0)

    cur.execute(
        """
        DELETE FROM ads_campaign_snapshot
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
          AND EXISTS (
            SELECT 1
            FROM ads_campaign_snapshot AS keep
            WHERE keep.shop_key = ads_campaign_snapshot.shop_key
              AND keep.ts = ads_campaign_snapshot.ts
              AND trim(keep.campaign_id)='SHOP_TOTAL'
          )
        """
    )
    rows_deleted_total += max(cur.rowcount, 0)

    cur.execute(
        """
        UPDATE ads_campaign_snapshot
        SET campaign_id='SHOP_TOTAL'
        WHERE lower(trim(campaign_id))='shop_total'
          AND trim(campaign_id)<>'SHOP_TOTAL'
        """
    )
    rows_updated_total += max(cur.rowcount, 0)

    return rows_updated_total, rows_deleted_total


@ops_phase1_db_app.command("normalize-shop-total")
def ops_phase1_db_normalize_shop_total(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    get_settings.cache_clear()
    try:
        db_path = _resolve_phase1_sqlite_db_path()
    except RuntimeError as exc:
        print(f"normalize_shop_total_ok=0 error={_scrub_sensitive_text(str(exc))}")
        raise typer.Exit(code=1)

    if not db_path.exists():
        print(f"normalize_shop_total_ok=0 error=db_not_found path={db_path}")
        raise typer.Exit(code=1)

    conn = sqlite3.connect(str(db_path))
    try:
        before = _phase1_shop_total_variant_counts(conn)
        conn.execute("BEGIN")
        rows_updated_total, rows_deleted_total = _normalize_phase1_shop_total_sqlite(conn)
        conn.commit()
        after = _phase1_shop_total_variant_counts(conn)
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        err = _scrub_sensitive_text(str(exc)) or "normalize_failed"
        print(f"normalize_shop_total_ok=0 error={err}")
        raise typer.Exit(code=1)
    finally:
        conn.close()

    print(f"normalize_shop_total_db path={db_path}")
    print(_format_phase1_shop_total_counts("before", before))
    print(_format_phase1_shop_total_counts("after", after))
    print(f"rows_updated_total={rows_updated_total}")
    print(f"rows_deleted_total={rows_deleted_total}")
    print("normalize_shop_total_ok=1")


@ops_phase1_token_appsscript_app.command("import")
def ops_phase1_token_appsscript_import(
    file: str = typer.Option(..., "--file", help="Apps Script export JSON path"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    path = Path(file)
    if not path.exists():
        raise typer.BadParameter(f"file not found: {path}")

    data = _read_json(path)
    token_map = _extract_appsscript_token_map(data)

    print(
        "token_appsscript_import_start "
        f"shops={','.join([shop.shop_key for shop in target_shops])} file={path}"
    )

    init_db()
    session = SessionLocal()
    imported_total = 0
    overall_ok = True
    try:
        for shop_cfg in target_shops:
            shop_id = _resolve_shop_id(shop_cfg)
            token_data = token_map.get(str(shop_id))
            if not token_data:
                print(
                    f"shop={shop_cfg.shop_key} shop_id={shop_id} imported=0 "
                    "access=0 refresh=0 access_expires_in_sec=-1 refresh_expires_in_sec=-1"
                )
                overall_ok = False
                continue
            access = token_data.get("access_token")
            refresh = token_data.get("refresh_token")
            access_raw, access_present = _extract_access_expiry_epoch(token_data)
            refresh_present = "refresh_token_expire_timestamp" in token_data
            access_info = _parse_epoch_seconds(
                access_raw if access_present else None,
                present=access_present,
            )
            refresh_info = _parse_epoch_seconds(
                token_data.get("refresh_token_expire_timestamp")
                if refresh_present
                else None,
                present=refresh_present,
            )
            expire_ts = access_info["timestamp"]
            refresh_ts = refresh_info["timestamp"]
            access_expires_in = _compute_expires_in(expire_ts)
            refresh_expires_in = _compute_expires_in(refresh_ts)

            access_expires_at = (
                datetime.fromtimestamp(expire_ts, tz=timezone.utc)
                if isinstance(expire_ts, int)
                else None
            )
            refresh_expires_at = (
                datetime.fromtimestamp(refresh_ts, tz=timezone.utc)
                if isinstance(refresh_ts, int)
                else None
            )
            if not access:
                print(
                    f"shop={shop_cfg.shop_key} shop_id={shop_id} imported=0 "
                    f"access={1 if bool(access) else 0} refresh={1 if bool(refresh) else 0} "
                    f"access_expires_in_sec={access_expires_in} refresh_expires_in_sec={refresh_expires_in}"
                )
                overall_ok = False
                continue

            upsert_token(
                session,
                shop_cfg.shop_key,
                shop_id,
                str(access),
                str(refresh) if refresh else "",
                access_expires_at,
                refresh_expires_at if refresh else None,
            )
            session.commit()
            imported_total += 1
            print(
                f"shop={shop_cfg.shop_key} shop_id={shop_id} imported=1 "
                "access=1 refresh=1 "
                f"access_expires_in_sec={access_expires_in} refresh_expires_in_sec={refresh_expires_in}"
            )
    finally:
        session.close()

    if overall_ok:
        print(f"token_appsscript_import_ok=1 imported_total={imported_total}")
    else:
        print(f"token_appsscript_import_ok=0 imported_total={imported_total}")
        raise typer.Exit(code=1)


@ops_phase1_token_appsscript_app.command("status")
def ops_phase1_token_appsscript_status(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            shop_id = _resolve_shop_id(shop_cfg)
            token = get_token(session, shop_cfg.shop_key)
            if token is None:
                print(
                    f"shop={shop_cfg.shop_key} shop_id={shop_id} "
                    "token_access=0 token_refresh=0 access_expires_in_sec=-1 refresh_expires_in_sec=-1"
                )
                continue
            access_expires_in = -1
            if token.access_token_expires_at:
                expires_at = token.access_token_expires_at
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                access_expires_in = max(
                    -1,
                    int(expires_at.timestamp())
                    - int(datetime.now(timezone.utc).timestamp()),
                )
            refresh_expires_in = -1
            if token.refresh_token_expires_at:
                refresh_at = token.refresh_token_expires_at
                if refresh_at.tzinfo is None:
                    refresh_at = refresh_at.replace(tzinfo=timezone.utc)
                refresh_expires_in = max(
                    -1,
                    int(refresh_at.timestamp())
                    - int(datetime.now(timezone.utc).timestamp()),
                )
            print(
                f"shop={shop_cfg.shop_key} shop_id={shop_id} "
                "token_access=1 token_refresh=1 "
                f"access_expires_in_sec={access_expires_in} refresh_expires_in_sec={refresh_expires_in}"
            )
    finally:
        session.close()


def _ensure_phase1_shops_only(shops_value: str, target_shops: list) -> None:
    allowed = {"samord", "minmin"}
    requested = _parse_only_shops(shops_value) or []
    invalid_requested = [shop_key for shop_key in requested if shop_key not in allowed]
    if invalid_requested:
        raise typer.BadParameter(
            f"shops must be within phase1 scope (samord,minmin), got: {','.join(invalid_requested)}"
        )
    invalid_selected = [shop.shop_key for shop in target_shops if shop.shop_key not in allowed]
    if invalid_selected:
        raise typer.BadParameter(
            f"phase1 scope only supports samord,minmin, got: {','.join(invalid_selected)}"
        )


def _phase1_select_shops(shops_value: str) -> list:
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops_value)
    if not target_shops:
        raise typer.BadParameter("no enabled phase1 shops selected")
    _ensure_phase1_shops_only(shops_value, target_shops)
    return target_shops


def _phase1_shop_keys_from_option(shops_value: str) -> list[str]:
    target_shops = _phase1_select_shops(shops_value)
    return sorted({shop.shop_key for shop in target_shops})


def _filter_phase1_status_payload_by_shops(
    payload: dict[str, Any],
    *,
    shop_keys: list[str],
) -> dict[str, Any]:
    allowed = set(shop_keys)
    filtered_shops = sorted([key for key in payload.get("shops", []) if key in allowed])
    out: dict[str, Any] = dict(payload)
    out["shops"] = filtered_shops

    token_section = payload.get("token", {})
    if isinstance(token_section, dict):
        out["token"] = {
            shop_key: token_section.get(shop_key, {}) for shop_key in filtered_shops
        }

    db_section = payload.get("db", {})
    if isinstance(db_section, dict):
        db_latest = db_section.get("latest_ingest", {})
        db_out = dict(db_section)
        if isinstance(db_latest, dict):
            db_out["latest_ingest"] = {
                shop_key: db_latest.get(shop_key, {}) for shop_key in filtered_shops
            }
        out["db"] = db_out

    reports_section = payload.get("reports", {})
    if isinstance(reports_section, dict):
        reports_latest = reports_section.get("latest", {})
        reports_out = dict(reports_section)
        if isinstance(reports_latest, dict):
            reports_out["latest"] = {
                shop_key: reports_latest.get(shop_key, {}) for shop_key in filtered_shops
            }
        out["reports"] = reports_out

    freshness_section = payload.get("freshness", {})
    if isinstance(freshness_section, dict):
        per_shop = freshness_section.get("per_shop", {})
        freshness_out = dict(freshness_section)
        if isinstance(per_shop, dict):
            freshness_out["per_shop"] = {
                shop_key: per_shop.get(shop_key, {}) for shop_key in filtered_shops
            }
        out["freshness"] = freshness_out

    ads_rate_limit_section = payload.get("ads_rate_limit", {})
    if isinstance(ads_rate_limit_section, dict):
        out["ads_rate_limit"] = {
            shop_key: ads_rate_limit_section.get(shop_key, {})
            for shop_key in filtered_shops
        }

    issues = payload.get("issues", [])
    if isinstance(issues, list):
        out["issues"] = [
            row
            for row in issues
            if isinstance(row, dict) and str(row.get("shop") or "") in allowed
        ]

    return out


def _build_phase1_status_payload_for_cli(
    *,
    shops_value: str,
    reports_dir: str | None = None,
) -> dict[str, Any]:
    shop_keys = _phase1_shop_keys_from_option(shops_value)
    prev_reports_dir = os.environ.get("REPORTS_DIR")
    try:
        if reports_dir:
            os.environ["REPORTS_DIR"] = reports_dir
        get_settings.cache_clear()
        payload = build_phase1_status_payload()
    finally:
        if reports_dir:
            if prev_reports_dir is None:
                os.environ.pop("REPORTS_DIR", None)
            else:
                os.environ["REPORTS_DIR"] = prev_reports_dir
        get_settings.cache_clear()

    return _filter_phase1_status_payload_by_shops(payload, shop_keys=shop_keys)


def _phase1_doctor_exit_code(issues: list[dict[str, Any]]) -> int:
    severities = [str(row.get("severity") or "").lower() for row in issues if isinstance(row, dict)]
    if any(level == "error" for level in severities):
        return 2
    if any(level == "warn" for level in severities):
        return 1
    return 0


def _phase1_doctor_verdict(exit_code: int) -> str:
    if exit_code >= 2:
        return "FAIL"
    if exit_code == 1:
        return "WARN"
    return "PASS"


def _phase1_report_pointer_compact(pointer: object) -> str:
    if not isinstance(pointer, dict):
        return "missing"
    age_hours = pointer.get("age_hours")
    stale_flag = 1 if bool(pointer.get("is_stale")) else 0
    if isinstance(age_hours, (int, float)):
        return f"age_h={float(age_hours):.2f} stale={stale_flag}"
    return f"age_h=- stale={stale_flag}"


def _build_phase1_doctor_summary_lines(
    *,
    payload: dict[str, Any],
    max_issues: int,
) -> list[str]:
    shops = [str(value) for value in payload.get("shops", [])]
    token_payload = payload.get("token", {}) if isinstance(payload.get("token"), dict) else {}
    db_payload = payload.get("db", {}) if isinstance(payload.get("db"), dict) else {}
    db_latest = (
        db_payload.get("latest_ingest", {})
        if isinstance(db_payload.get("latest_ingest"), dict)
        else {}
    )
    reports_payload = (
        payload.get("reports", {}) if isinstance(payload.get("reports"), dict) else {}
    )
    reports_latest = (
        reports_payload.get("latest", {})
        if isinstance(reports_payload.get("latest"), dict)
        else {}
    )
    ads_rate_limit_config = (
        payload.get("ads_rate_limit_config", {})
        if isinstance(payload.get("ads_rate_limit_config"), dict)
        else {}
    )
    issues = [
        row for row in payload.get("issues", []) if isinstance(row, dict)
    ]
    exit_code = _phase1_doctor_exit_code(issues)
    verdict = _phase1_doctor_verdict(exit_code)

    severity_counts: Counter[str] = Counter()
    issue_code_counts: Counter[tuple[str, str]] = Counter()
    for row in issues:
        severity = str(row.get("severity") or "info").lower()
        code = str(row.get("code") or "-")
        severity_counts[severity] += 1
        issue_code_counts[(code, severity)] += 1

    lines: list[str] = []
    lines.append(
        "phase1_doctor "
        f"verdict={verdict} exit_code={exit_code} shops={','.join(shops)} issues_total={len(issues)}"
    )
    lines.append(
        "issues_by_severity "
        f"error={severity_counts.get('error', 0)} "
        f"warn={severity_counts.get('warn', 0)} "
        f"info={severity_counts.get('info', 0)}"
    )
    lines.append(
        "ads_rate_limit_state "
        f"ads_rate_limit_state_path={str(ads_rate_limit_config.get('state_path_effective') or '-')} "
        f"ads_rate_limit_state_path_exists={1 if bool(ads_rate_limit_config.get('parent_dir_exists')) else 0} "
        f"ads_rate_limit_state_path_writable={1 if bool(ads_rate_limit_config.get('parent_dir_writable')) else 0}"
    )

    for shop_key in shops:
        token_row = token_payload.get(shop_key, {}) if isinstance(token_payload, dict) else {}
        db_row = db_latest.get(shop_key, {}) if isinstance(db_latest, dict) else {}
        report_row = (
            reports_latest.get(shop_key, {}) if isinstance(reports_latest, dict) else {}
        )
        lines.append(
            "shop_token "
            f"shop={shop_key} "
            f"access_ttl_sec={int(token_row.get('access_expires_in_sec', -1) or -1)} "
            f"gate_state={str(token_row.get('gate_state') or 'unknown')}"
        )
        lines.append(
            "shop_ingest "
            f"shop={shop_key} "
            f"daily_latest={str(db_row.get('daily_latest_date') or '-')} "
            f"snapshot_latest={str(db_row.get('snapshot_latest_at') or '-')}"
        )
        lines.append(
            "shop_reports "
            f"shop={shop_key} "
            f"daily_midday={_phase1_report_pointer_compact(report_row.get('daily_midday'))} "
            f"daily_final={_phase1_report_pointer_compact(report_row.get('daily_final'))} "
            f"weekly={_phase1_report_pointer_compact(report_row.get('weekly'))}"
        )

    ranked_codes = sorted(
        issue_code_counts.items(),
        key=lambda item: (-item[1], item[0][1], item[0][0]),
    )[: max(0, int(max_issues))]
    for idx, ((code, severity), count) in enumerate(ranked_codes, start=1):
        lines.append(
            f"top_issue_{idx} code={code} severity={severity} count={int(count)}"
        )

    return lines


def _build_phase1_doctor_summary_markdown(
    *,
    payload: dict[str, Any],
    summary_lines: list[str],
) -> str:
    issues = [row for row in payload.get("issues", []) if isinstance(row, dict)]
    exit_code = _phase1_doctor_exit_code(issues)
    verdict = _phase1_doctor_verdict(exit_code)
    shops = [str(value) for value in payload.get("shops", [])]
    rows = [
        "# Phase1 Doctor Summary",
        "",
        f"- verdict: {verdict}",
        f"- exit_code: {exit_code}",
        f"- shops: {','.join(shops)}",
        f"- issues_total: {len(issues)}",
        "",
        "## Output",
        "",
        "```text",
        *summary_lines,
        "```",
        "",
    ]
    return "\n".join(rows)


def _write_phase1_doctor_artifacts(
    *,
    artifacts_dir: str | None,
    payload: dict[str, Any],
    summary_lines: list[str],
) -> tuple[Path | None, Path | None]:
    if not artifacts_dir:
        return None, None
    out_dir = Path(artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    status_path = out_dir / "doctor_status.json"
    summary_path = out_dir / "doctor_summary.md"
    status_path.write_text(_dump_json(payload, pretty=True), encoding="utf-8")
    summary_path.write_text(
        _build_phase1_doctor_summary_markdown(
            payload=payload,
            summary_lines=summary_lines,
        ),
        encoding="utf-8",
    )
    print(f"doctor_status_path={status_path}")
    print(f"doctor_summary_path={summary_path}")
    return status_path, summary_path


def _phase1_doctor_report_value(pointer: object) -> str:
    if not isinstance(pointer, dict):
        return "missing"
    url_value = str(pointer.get("url") or "").strip()
    if url_value:
        return url_value
    if bool(pointer.get("is_stale")):
        return "stale"
    return "ok"


def _build_phase1_doctor_alert_message(
    *,
    shop_label: str,
    shop_key: str,
    level: str,
    issue_codes: list[str],
    issue_count: int,
    payload: dict[str, Any],
    reports_dir: str | None,
) -> str:
    code_text = ",".join(issue_codes) if issue_codes else "NONE"
    reports_root = (
        payload.get("reports", {}) if isinstance(payload.get("reports"), dict) else {}
    )
    reports_latest = (
        reports_root.get("latest", {})
        if isinstance(reports_root.get("latest"), dict)
        else {}
    )
    report_row = reports_latest.get(shop_key, {}) if isinstance(reports_latest, dict) else {}
    midday_value = _phase1_doctor_report_value(
        report_row.get("daily_midday") if isinstance(report_row, dict) else None
    )
    final_value = _phase1_doctor_report_value(
        report_row.get("daily_final") if isinstance(report_row, dict) else None
    )
    weekly_value = _phase1_doctor_report_value(
        report_row.get("weekly") if isinstance(report_row, dict) else None
    )
    reports_dir_value = reports_dir or str(Path(get_settings().reports_dir))

    return "\n".join(
        [
            f"[{shop_label}][ALERT] OPS_DOCTOR {level.upper()} issues={issue_count} codes={code_text}",
            f"Reports: midday={midday_value} final={final_value} weekly={weekly_value}",
            f"Next: ops phase1 doctor --shops {shop_key} --reports-dir {reports_dir_value}",
        ]
    )


def _build_phase1_doctor_resolved_message(
    *,
    shop_label: str,
    shop_key: str,
    previous_level: str,
    reports_dir: str | None,
) -> str:
    reports_dir_value = reports_dir or str(Path(get_settings().reports_dir))
    return "\n".join(
        [
            f"[{shop_label}][ALERT] OPS_DOCTOR RESOLVED previous={previous_level.upper()}",
            f"Next: ops phase1 doctor --shops {shop_key} --reports-dir {reports_dir_value}",
        ]
    )


def _phase1_doctor_notify_run(
    *,
    payload: dict[str, Any],
    target_shops: list,
    min_severity: str,
    discord_mode: str,
    confirm_discord_send: bool,
    persist_state: bool,
    reports_dir: str | None,
    aggregate: bool,
    cooldown_sec: int,
    resolved_cooldown_sec: int,
    max_issues: int,
    summary_path: Path | None,
) -> None:
    mode_value = str(discord_mode or "").strip().lower()
    if mode_value not in {"dry-run", "send"}:
        raise typer.BadParameter("discord-mode must be one of: dry-run, send")
    if mode_value == "send" and not confirm_discord_send:
        print("doctor_notify_send_blocked=1 reason=missing_confirm_discord_send")
    effective_send = mode_value == "send" and bool(confirm_discord_send)
    persist_state_effective = bool(effective_send) or (
        mode_value == "dry-run" and bool(persist_state)
    )
    dry_run = 0 if effective_send else 1

    now_utc = datetime.now(timezone.utc)
    try:
        normalized_min = parse_min_severity(min_severity)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    shop_rows = [
        {
            "shop_key": str(shop.shop_key),
            "shop_label": str(shop.label or shop.shop_key.upper()),
        }
        for shop in target_shops
    ]
    init_db()
    session = SessionLocal()
    try:
        decisions = run_doctor_notify_cycle(
            session=session,
            shops=shop_rows,
            payload=payload,
            min_severity=normalized_min,
            cooldown_sec=cooldown_sec,
            resolved_cooldown_sec=resolved_cooldown_sec,
            persist_state=persist_state_effective,
            record_sent_state=effective_send,
            now_utc=now_utc,
            max_issues=max_issues,
        )
        if persist_state_effective:
            session.commit()
        else:
            session.rollback()
    finally:
        session.close()

    emitted = 0
    suppressed = 0
    queued_messages: list[dict[str, str]] = []
    for row in decisions:
        shop_key = str(row.get("shop_key") or "")
        shop_label = str(row.get("shop_label") or shop_key.upper())
        action = str(row.get("action") or "none")
        level = str(row.get("level") or "-")
        would_send = int(row.get("would_send") or 0)
        cooldown_skip = int(row.get("cooldown_skip") or 0)
        resolved_cooldown_skip = int(row.get("resolved_cooldown_skip") or 0)
        message = str(row.get("message") or "")

        if cooldown_skip == 1:
            suppressed += 1
            print(
                "doctor_notify "
                f"shop={shop_key} label={shop_label} action={action} level={level} "
                f"would_send=0 cooldown_skip=1 cooldown_until={row.get('cooldown_until_utc', '-')}"
            )
            continue
        if resolved_cooldown_skip == 1:
            suppressed += 1
            print(
                "doctor_notify "
                f"shop={shop_key} label={shop_label} action={action} level={level} "
                f"would_send=0 resolved_cooldown_skip=1 "
                f"resolved_cooldown_until={row.get('resolved_cooldown_until_utc', '-')}"
            )
            continue
        if would_send != 1:
            print(
                "doctor_notify "
                f"shop={shop_key} label={shop_label} action={action} level={level} "
                "would_send=0 reason=no_matching_issues"
            )
            continue

        if action == "alert":
            message = _build_phase1_doctor_alert_message(
                shop_label=shop_label,
                shop_key=shop_key,
                level=level,
                issue_codes=[str(value) for value in (row.get("issue_codes") or [])],
                issue_count=int(row.get("issue_count") or 0),
                payload=payload,
                reports_dir=reports_dir,
            )
        elif action == "resolved":
            message = _build_phase1_doctor_resolved_message(
                shop_label=shop_label,
                shop_key=shop_key,
                previous_level=str(row.get("level") or "-"),
                reports_dir=reports_dir,
            )

        emitted += 1
        print(
            "doctor_notify "
            f"shop={shop_key} label={shop_label} action={action} level={level} "
            f"would_send=1 sent={1 if effective_send else 0} dry_run={dry_run}"
        )
        message_preview = _scrub_sensitive_text(message).replace("\n", "\\n").strip()
        print(
            "doctor_notify_preview "
            f"shop={shop_key} label={shop_label} text={message_preview}"
        )
        queued_messages.append(
            {
                "shop_key": shop_key,
                "shop_label": shop_label,
                "message": message,
            }
        )

    if effective_send and queued_messages:
        if aggregate:
            joined = "\n\n".join([row["message"] for row in queued_messages])
            send(
                "alerts",
                joined,
                md_attachment_path=summary_path,
                md_attachment_filename=(
                    summary_path.name if isinstance(summary_path, Path) else None
                ),
            )
        else:
            for row in queued_messages:
                send(
                    "alerts",
                    row["message"],
                    md_attachment_path=summary_path,
                    md_attachment_filename=(
                        summary_path.name if isinstance(summary_path, Path) else None
                    ),
                )

    print(
        "doctor_notify_summary "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"min_severity={normalized_min} discord_mode={mode_value} aggregate={1 if aggregate else 0} "
        f"send_enabled={1 if effective_send else 0} "
        f"persist_state={1 if persist_state_effective else 0} "
        f"emitted={emitted} suppressed={suppressed}"
    )


def _compute_token_verdict(
    *,
    access: object,
    refresh: object,
    access_info: dict[str, object],
    min_access_ttl_sec: int,
) -> tuple[str, str, int]:
    access_ts = access_info.get("timestamp")
    access_expires_in = _compute_expires_in(access_ts if isinstance(access_ts, int) else None)
    access_expiry_kind = "unknown"
    if access_ts is not None:
        if access_expires_in <= 0:
            access_expiry_kind = "expired"
        elif access_expires_in < min_access_ttl_sec:
            access_expiry_kind = "short_ttl"
        else:
            access_expiry_kind = "ok"
    verdict = "ok"
    if not access:
        verdict = "missing"
    elif access_expiry_kind == "unknown":
        verdict = "unknown"
    elif access_expiry_kind == "expired":
        verdict = "expired"
    elif access_expiry_kind == "short_ttl":
        verdict = "short_ttl"
    return verdict, access_expiry_kind, access_expires_in


def _format_token_updated_at(value: object) -> str:
    if not isinstance(value, datetime):
        return "-"
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _collect_db_token_status_rows(
    *,
    target_shops: list,
    min_access_ttl_sec: int,
) -> tuple[list[dict[str, object]], bool]:
    token_rows = _collect_preflight_rows_from_db(target_shops=target_shops)
    rows: list[dict[str, object]] = []
    overall_ok = True
    for idx, raw in enumerate(token_rows):
        shop_cfg = target_shops[idx]
        access = raw.get("access_token")
        refresh = raw.get("refresh_token")
        access_info = raw.get("access_info") or _parse_epoch_seconds(None, present=False)
        refresh_info = raw.get("refresh_info") or _parse_epoch_seconds(None, present=False)
        verdict, access_expiry_kind, access_expires_in = _compute_token_verdict(
            access=access,
            refresh=refresh,
            access_info=access_info,
            min_access_ttl_sec=min_access_ttl_sec,
        )
        refresh_ts = refresh_info.get("timestamp")
        refresh_expires_in = _compute_expires_in(
            refresh_ts if isinstance(refresh_ts, int) else None
        )
        if verdict in {"missing", "expired", "short_ttl", "unknown"}:
            overall_ok = False
        rows.append(
            {
                "shop": shop_cfg.shop_key,
                "shop_label": shop_cfg.label,
                "shop_id": int(raw.get("shop_id", 0)),
                "token_source": "db",
                "token_len": int(raw.get("token_len", 0)),
                "token_sha8": str(raw.get("token_sha8", "-")),
                "access_expires_in_sec": access_expires_in,
                "refresh_expires_in_sec": refresh_expires_in,
                "updated_at": _format_token_updated_at(raw.get("updated_at")),
                "token_verdict": verdict,
                "access_expiry_kind": access_expiry_kind,
            }
        )
    return rows, overall_ok


@ops_phase1_token_app.command("status")
def ops_phase1_token_status(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Phase1 shop keys only"
    ),
    db: str | None = typer.Option(
        None, "--db", help="SQLite DB path override (Phase1 DB token mode)"
    ),
    min_access_ttl_sec: int = typer.Option(
        900, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    previous_db_url = os.environ.get("DATABASE_URL")
    db_path: Path | None = None
    if db:
        db_path = Path(db)
        if not db_path.is_absolute():
            db_path = (Path.cwd() / db_path).resolve()
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
        get_settings.cache_clear()
    try:
        shops_cfg = _load_shops_or_exit()
        target_shops = _select_shops(shops_cfg, shops)
        if not target_shops:
            print("no enabled shops")
            raise typer.Exit(code=1)
        _ensure_phase1_shops_only(shops, target_shops)
        _ensure_shop_ids(target_shops)

        rows, overall_ok = _collect_db_token_status_rows(
            target_shops=target_shops,
            min_access_ttl_sec=min_access_ttl_sec,
        )
        gate_status_map = load_token_preflight_gate_status_snapshot(shops=target_shops)
        db_text = str(db_path) if db_path else os.environ.get("DATABASE_URL", "-")
        print(
            "token_status_start "
            f"shops={','.join([shop.shop_key for shop in target_shops])} "
            f"token_source=db min_access_ttl_sec={min_access_ttl_sec} db={db_text}"
        )
        for row in rows:
            shop_key = str(row["shop"])
            gate_status = gate_status_map.get(shop_key, {})
            gate_state = str(gate_status.get("gate_state") or "unknown")
            cooldown_until = int(gate_status.get("cooldown_until", -1))
            resolved_cooldown_until = int(gate_status.get("resolved_cooldown_until", -1))
            last_alert_at = int(gate_status.get("last_alert_at", -1))
            last_resolved_at = int(gate_status.get("last_resolved_at", -1))
            print(
                "token_status "
                f"shop={row['shop']} shop_label={row['shop_label']} shop_id={row['shop_id']} "
                f"token_len={row['token_len']} token_sha8={row['token_sha8']} "
                f"access_ttl_sec={row['access_expires_in_sec']} "
                f"verdict={row['token_verdict']} token_verdict={row['token_verdict']} "
                f"min_required={min_access_ttl_sec} "
                f"gate_state={gate_state} cooldown_until={cooldown_until} "
                f"resolved_cooldown_until={resolved_cooldown_until} "
                f"last_alert_at={last_alert_at} last_resolved_at={last_resolved_at}"
            )
        print(f"token_status_ok={1 if overall_ok else 0}")
    finally:
        if db:
            if previous_db_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous_db_url
            get_settings.cache_clear()


@ops_phase1_token_app.command("preflight")
def ops_phase1_token_preflight(
    file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path (optional)"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    min_access_ttl_sec: int = typer.Option(
        300, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    allow_unknown_expiry: bool = typer.Option(
        False, "--allow-unknown-expiry", help="Allow unknown access expiry"
    ),
) -> None:
    # Backward-compatible alias for operator UX.
    ops_phase1_token_appsscript_preflight(
        file=file,
        env_file=env_file,
        shops=shops,
        min_access_ttl_sec=min_access_ttl_sec,
        allow_unknown_expiry=allow_unknown_expiry,
    )


@ops_phase1_token_app.command("seed-expired")
def ops_phase1_token_seed_expired(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    db: str = typer.Option(..., "--db", help="SQLite DB path"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    db_path = Path(db)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    previous_db_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    get_settings.cache_clear()

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_phase1_shops_only(shops, target_shops)
    _ensure_shop_ids(target_shops)

    now = datetime.now(timezone.utc)
    access_expired_at = now - timedelta(hours=2)
    refresh_expires_at = now + timedelta(days=30)

    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            shop_id = _resolve_shop_id(shop_cfg)
            access = f"EXPIRED_ACCESS_{shop_cfg.shop_key}_{int(now.timestamp())}"
            refresh = f"REFRESH_{shop_cfg.shop_key}_{int(now.timestamp())}"
            upsert_token(
                session,
                shop_cfg.shop_key,
                shop_id,
                access,
                refresh,
                access_expired_at,
                refresh_expires_at,
            )
            print(
                "token_seed_expired "
                f"shop={shop_cfg.shop_key} shop_id={shop_id} "
                f"access_expires_at={access_expired_at.isoformat()} "
                f"refresh_expires_at={refresh_expires_at.isoformat()}"
            )
        session.commit()
    finally:
        session.close()
        if previous_db_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_db_url
        get_settings.cache_clear()
    print(
        "token_seed_expired_ok=1 "
        f"shops={','.join([shop.shop_key for shop in target_shops])} db={db_path}"
    )


@ops_phase1_token_appsscript_app.command("preflight")
def ops_phase1_token_appsscript_preflight(
    file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path (optional)"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    min_access_ttl_sec: int = typer.Option(
        300, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    allow_unknown_expiry: bool = typer.Option(
        False, "--allow-unknown-expiry", help="Allow unknown access expiry"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    token_source = "file"
    file_value = "-"
    token_rows: list[dict[str, object]] = []
    if file:
        token_source = "file"
        file_value = str(Path(file))
        token_rows = _collect_preflight_rows_from_file(
            target_shops=target_shops, token_file=file
        )
    else:
        token_source = "db"
        file_value = "-"
        token_rows = _collect_preflight_rows_from_db(target_shops=target_shops)

    print(
        "token_preflight_start "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"file={file_value} token_source={token_source} "
        f"min_access_ttl_sec={min_access_ttl_sec}"
    )

    overall_ok = True
    unknown_allowed = False
    for row in token_rows:
        shop_key = str(row.get("shop_key", "-"))
        shop_id = int(row.get("shop_id", 0))
        access = row.get("access_token")
        refresh = row.get("refresh_token")
        access_info = row.get("access_info") or _parse_epoch_seconds(
            None, present=False
        )
        refresh_info = row.get("refresh_info") or _parse_epoch_seconds(
            None, present=False
        )
        token_len = int(row.get("token_len", 0))
        token_sha8 = str(row.get("token_sha8", "-"))

        verdict, access_expiry_kind, access_expires_in = _compute_token_verdict(
            access=access,
            refresh=refresh,
            access_info=access_info,
            min_access_ttl_sec=min_access_ttl_sec,
        )
        refresh_expires_in = _compute_expires_in(refresh_info["timestamp"])
        access_expiry_src = str(access_info["src"])
        if access_info["timestamp"] is not None and access_expires_in <= 0:
            access_expiry_src = "expired_in_past"
        access_expiry_present = access_info["present"]
        access_expiry_unit_guess = access_info["unit_guess"]
        access_expiry_type = access_info["raw_type"]

        if verdict in {"missing", "expired", "short_ttl"}:
            overall_ok = False
        elif verdict == "unknown":
            if allow_unknown_expiry:
                unknown_allowed = True
            else:
                overall_ok = False
        print(
            f"shop={shop_key} shop_id={shop_id} token_source={token_source} "
            f"access={1 if bool(access) else 0} refresh={1 if bool(refresh) else 0} "
            f"access_expires_in_sec={access_expires_in} min_access_ttl_sec={min_access_ttl_sec} "
            f"refresh_expires_in_sec={refresh_expires_in} token_verdict={verdict} "
            f"access_expiry_kind={access_expiry_kind} "
            f"access_expiry_src={access_expiry_src} "
            f"access_expiry_present={access_expiry_present} "
            f"access_expiry_unit_guess={access_expiry_unit_guess} "
            f"access_expiry_type={access_expiry_type} "
            f"token_len={token_len} token_sha8={token_sha8}"
        )
        if verdict in {"short_ttl", "expired"}:
            print(
                f"next_steps: run Apps Script refreshTok({shop_id}) then "
                "exportShopeeTokensToDrive_Normalized()"
            )

    if allow_unknown_expiry and unknown_allowed:
        print("warning=unknown_access_expiry_allowed")
    if overall_ok:
        print("preflight_ok=1")
    else:
        print("preflight_ok=0")
        raise typer.Exit(code=2)


def _parse_datetime_seconds(value: object, *, present: bool, src: str) -> dict[str, object]:
    if not present:
        return {
            "timestamp": None,
            "present": 0,
            "unit_guess": "unknown",
            "src": "missing",
            "raw_type": "none",
        }
    if value is None:
        return {
            "timestamp": None,
            "present": 1,
            "unit_guess": "unknown",
            "src": f"{src}_missing",
            "raw_type": "none",
        }
    if isinstance(value, datetime):
        dt_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return {
            "timestamp": int(dt_value.timestamp()),
            "present": 1,
            "unit_guess": "sec",
            "src": src,
            "raw_type": "datetime",
        }
    return {
        "timestamp": None,
        "present": 1,
        "unit_guess": "unknown",
        "src": f"{src}_parse_error",
        "raw_type": type(value).__name__,
    }


def _extract_access_expiry_epoch(token_data: dict[str, object] | None) -> tuple[object, bool]:
    if not isinstance(token_data, dict):
        return None, False
    for key in (
        "expire_timestamp",
        "access_expire_timestamp",
        "access_token_expire_timestamp",
        "access_expires_at",
    ):
        if key in token_data:
            return token_data.get(key), True
    if "expires_in" in token_data:
        raw = token_data.get("expires_in")
        try:
            expires_in = int(raw)
        except Exception:
            return raw, True
        if expires_in < 0:
            return raw, True
        return int(datetime.now(timezone.utc).timestamp()) + expires_in, True
    return None, False


def _collect_preflight_rows_from_file(*, target_shops: list, token_file: str) -> list[dict[str, object]]:
    path = Path(token_file)
    if not path.exists():
        raise typer.BadParameter(f"file not found: {path}")

    data = _read_json(path)
    token_map = _extract_appsscript_token_map(data)
    rows: list[dict[str, object]] = []
    for shop_cfg in target_shops:
        shop_id = _resolve_shop_id(shop_cfg)
        token_data = token_map.get(str(shop_id))
        access = token_data.get("access_token") if token_data else None
        refresh = token_data.get("refresh_token") if token_data else None
        access_raw, access_present = _extract_access_expiry_epoch(token_data)
        refresh_present = bool(
            token_data and "refresh_token_expire_timestamp" in token_data
        )
        access_info = _parse_epoch_seconds(
            access_raw if access_present else None,
            present=access_present,
        )
        refresh_info = _parse_epoch_seconds(
            token_data.get("refresh_token_expire_timestamp")
            if refresh_present
            else None,
            present=refresh_present,
        )
        access_text = str(access) if access else ""
        rows.append(
            {
                "shop_key": shop_cfg.shop_key,
                "shop_id": shop_id,
                "access_token": access,
                "refresh_token": refresh,
                "access_info": access_info,
                "refresh_info": refresh_info,
                "token_len": len(access_text),
                "token_sha8": _sha256_8(access_text),
            }
        )
    return rows


def _collect_preflight_rows_from_db(*, target_shops: list) -> list[dict[str, object]]:
    init_db()
    session = SessionLocal()
    rows: list[dict[str, object]] = []
    try:
        for shop_cfg in target_shops:
            shop_id = _resolve_shop_id(shop_cfg)
            token = get_token(session, shop_cfg.shop_key)
            access = token.access_token if token else None
            refresh = token.refresh_token if token else None
            access_info = _parse_datetime_seconds(
                token.access_token_expires_at if token else None,
                present=bool(token),
                src="token_store_access_expires_at",
            )
            refresh_info = _parse_datetime_seconds(
                token.refresh_token_expires_at if token else None,
                present=bool(token),
                src="token_store_refresh_expires_at",
            )
            access_text = str(access) if access else ""
            rows.append(
                {
                    "shop_key": shop_cfg.shop_key,
                    "shop_id": shop_id,
                    "access_token": access,
                    "refresh_token": refresh,
                    "access_info": access_info,
                    "refresh_info": refresh_info,
                    "token_len": len(access_text),
                    "token_sha8": _sha256_8(access_text),
                    "updated_at": token.updated_at if token else None,
                }
            )
    finally:
        session.close()
    return rows


@ops_phase1_token_appsscript_app.command("print-force-refresh-snippet")
def ops_phase1_token_appsscript_print_force_refresh_snippet(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    function_name: str = typer.Option(
        "forceRefreshAndExport",
        "--function-name",
        help="Apps Script function name (snippet output)",
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    print("```javascript")
    print(f"function {function_name}() {{")
    print("  // Paste into Apps Script, run, then download the exported JSON locally.")
    for shop_cfg in target_shops:
        shop_id = _resolve_shop_id(shop_cfg)
        print(f"  refreshTok({shop_id});")
    print("  exportShopeeTokensToDrive_Normalized();")
    print("}")
    print("```")


def _sync_tokens_from_file(
    *,
    token_file: str,
    target_shops: list,
) -> dict[str, object]:
    token_path = Path(token_file)
    if not token_path.exists():
        raise typer.BadParameter(f"file not found: {token_path}")

    token_map = _extract_appsscript_token_map(_read_json(token_path))
    settings = get_settings()
    target_db = settings.database_url
    init_db()
    session = SessionLocal()

    imported_total = 0
    noop_total = 0
    missing_total = 0
    token_rows: dict[str, dict[str, object]] = {}
    try:
        for shop_cfg in target_shops:
            shop_id = _resolve_shop_id(shop_cfg)
            token_data = token_map.get(str(shop_id))
            if not token_data:
                missing_total += 1
                token_rows[shop_cfg.shop_key] = {
                    "token_len": 0,
                    "token_sha8": "-",
                    "missing": True,
                }
                continue
            access = token_data.get("access_token")
            refresh = token_data.get("refresh_token")
            if not access:
                missing_total += 1
                token_rows[shop_cfg.shop_key] = {
                    "token_len": 0,
                    "token_sha8": "-",
                    "missing": True,
                }
                continue

            access_raw, access_present = _extract_access_expiry_epoch(token_data)
            refresh_present = "refresh_token_expire_timestamp" in token_data
            access_info = _parse_epoch_seconds(
                access_raw if access_present else None,
                present=access_present,
            )
            refresh_info = _parse_epoch_seconds(
                token_data.get("refresh_token_expire_timestamp")
                if refresh_present
                else None,
                present=refresh_present,
            )
            access_expires_at = (
                datetime.fromtimestamp(access_info["timestamp"], tz=timezone.utc)
                if isinstance(access_info.get("timestamp"), int)
                else None
            )
            refresh_expires_at = (
                datetime.fromtimestamp(refresh_info["timestamp"], tz=timezone.utc)
                if isinstance(refresh_info.get("timestamp"), int)
                else None
            )

            existing = get_token(session, shop_cfg.shop_key)
            existing_sha8 = _sha256_8(existing.access_token) if existing else None
            access_text = str(access)
            access_sha8 = _sha256_8(access_text)
            token_len = len(access_text)
            if existing_sha8 == access_sha8:
                noop_total += 1
            else:
                imported_total += 1

            upsert_token(
                session,
                shop_cfg.shop_key,
                shop_id,
                access_text,
                str(refresh) if refresh else "",
                access_expires_at,
                refresh_expires_at,
            )
            session.commit()
            token_rows[shop_cfg.shop_key] = {
                "token_len": token_len,
                "token_sha8": access_sha8,
                "missing": False,
            }
    finally:
        session.close()

    ok = missing_total == 0
    print(
        "token_sync_from_file_ok="
        f"{1 if ok else 0} imported_total={imported_total} "
        f"noop_total={noop_total} target_db={target_db}"
    )
    if not ok:
        raise typer.Exit(code=1)
    return {
        "ok": ok,
        "imported_total": imported_total,
        "noop_total": noop_total,
        "target_db": target_db,
        "rows": token_rows,
    }


def _print_db_token_fingerprints(target_shops: list) -> None:
    init_db()
    session = SessionLocal()
    try:
        for shop_cfg in target_shops:
            token = get_token(session, shop_cfg.shop_key)
            if token is None or not token.access_token:
                print(
                    f"token_source=db shop={shop_cfg.shop_key} "
                    "token_len=0 token_sha8=- missing=1"
                )
                continue
            token_len = len(token.access_token)
            token_sha8 = _sha256_8(token.access_token)
            print(
                f"token_source=db shop={shop_cfg.shop_key} "
                f"token_len={token_len} token_sha8={token_sha8}"
            )
    finally:
        session.close()


def _phase1_db_preflight_or_exit(
    *,
    shops: str,
    min_access_ttl_sec: int,
    command_name: str,
) -> None:
    pre_exit, pre_output = _run_capture_step(
        ops_phase1_token_appsscript_preflight,
        file=None,
        env_file=None,
        shops=shops,
        min_access_ttl_sec=min_access_ttl_sec,
        allow_unknown_expiry=False,
    )
    if pre_output:
        _print_captured_output(pre_output)
    if pre_exit == 0:
        return
    exit_code = pre_exit if pre_exit else 2
    print(
        f"{command_name}_token_preflight_ok=0 token_source=db exit_code={exit_code}"
    )
    print(
        "next_steps: push/import tokens via POST /ops/phase1/token/import "
        "or run ops phase1 token appsscript sync --token-file <path> then retry"
    )
    raise typer.Exit(code=2 if exit_code == 2 else exit_code)


@ops_phase1_token_appsscript_app.command("sync")
def ops_phase1_token_appsscript_sync(
    file: str = typer.Option(..., "--token-file", help="Apps Script export JSON path"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    _sync_tokens_from_file(token_file=file, target_shops=target_shops)
    _print_db_token_fingerprints(target_shops)


@ops_phase1_auth_app.command("fingerprint")
def ops_phase1_auth_fingerprint(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    token_file: str = typer.Option(..., "--token-file", help="Apps Script export JSON path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    compare_to: str | None = typer.Option(
        None, "--compare-to", help="Apps Script fingerprint JSON path"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    token_path = Path(token_file)
    if not token_path.exists():
        raise typer.BadParameter(f"file not found: {token_path}")
    token_map = _extract_appsscript_token_map(_read_json(token_path))

    compare_data = None
    if compare_to:
        compare_path = Path(compare_to)
        if not compare_path.exists():
            raise typer.BadParameter(f"compare file not found: {compare_path}")
        compare_data = _parse_appsscript_fingerprint(_read_json(compare_path))

    shop_list = ",".join([shop.shop_key for shop in target_shops])
    print(f"auth_fingerprint_start shops={shop_list}")

    settings = get_settings()
    partner_id = settings.shopee_partner_id
    partner_id_text = str(partner_id) if partner_id is not None else "-"
    partner_key_sha8 = _sha256_8(settings.shopee_partner_key)
    print(f"partner_id={partner_id_text} partner_key_sha8={partner_key_sha8}")

    compare_shop_rows: list[tuple[str, bool, bool, bool, bool]] = []
    for shop_cfg in target_shops:
        shop_id = _resolve_shop_id(shop_cfg)
        token_data = token_map.get(str(shop_id))
        access_token = token_data.get("access_token") if token_data else None
        token_len = len(access_token) if access_token else 0
        token_sha8 = _sha256_8(access_token)
        token_match = "-"
        if compare_data is not None:
            compare_shop = compare_data.get("shops", {}).get(shop_cfg.shop_key)
            shop_id_ok = bool(compare_shop and compare_shop.get("shop_id") == shop_id)
            len_ok = bool(compare_shop and compare_shop.get("token_len") == token_len)
            sha_ok = bool(compare_shop and compare_shop.get("token_sha8") == token_sha8)
            token_ok = shop_id_ok and len_ok and sha_ok
            token_match = "1" if token_ok else "0"
            compare_shop_rows.append(
                (shop_cfg.shop_key, token_ok, len_ok, sha_ok, shop_id_ok)
            )
        print(
            f"shop={shop_cfg.shop_key} shop_id={shop_id} "
            f"token_len={token_len} token_sha8={token_sha8} token_match={token_match}"
        )

    if compare_data is None:
        return

    compare_partner_id = compare_data.get("partner_id")
    compare_partner_key = compare_data.get("partner_key_sha8")
    partner_id_ok = bool(
        partner_id is not None
        and compare_partner_id is not None
        and int(partner_id) == int(compare_partner_id)
    )
    partner_key_ok = bool(
        compare_partner_key
        and partner_key_sha8 != "-"
        and partner_key_sha8 == compare_partner_key
    )
    print(f"compare_partner_id ok={1 if partner_id_ok else 0}")
    print(f"compare_partner_key ok={1 if partner_key_ok else 0}")

    parity_ok = partner_id_ok and partner_key_ok
    for shop_key, token_ok, len_ok, sha_ok, shop_id_ok in compare_shop_rows:
        print(
            "compare_shop "
            f"{shop_key} ok={1 if token_ok else 0} "
            f"(len_ok={1 if len_ok else 0} "
            f"sha_ok={1 if sha_ok else 0} "
            f"shop_id_ok={1 if shop_id_ok else 0})"
        )
        parity_ok = parity_ok and token_ok

    print(f"parity_ok={1 if parity_ok else 0}")
    if not parity_ok:
        raise typer.Exit(code=2)


def _parse_kv_tokens(line: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    for match in re.finditer(r"([A-Za-z0-9_]+)\s*=\s*([^\s]+)", line):
        parts[match.group(1)] = match.group(2)
    return parts


def _parse_appsscript_sign_fingerprint_text(text: str) -> dict[str, object]:
    data: dict[str, object] = {
        "partner_id": None,
        "partner_key_sha8": None,
        "shops": {},
    }
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        tokens = _parse_kv_tokens(line)
        if not tokens:
            continue
        if "partner_id" in tokens:
            data["partner_id"] = _coerce_int(tokens.get("partner_id"))
        if "partner_key_sha8" in tokens:
            data["partner_key_sha8"] = tokens.get("partner_key_sha8")
        if "shop" in tokens:
            shop_key = tokens.get("shop")
            if not shop_key:
                continue
            shops = data.setdefault("shops", {})
            shop_entry = shops.setdefault(shop_key, {"paths": {}})
            if "path" in tokens:
                path = tokens.get("path") or ""
                if path:
                    paths = shop_entry.setdefault("paths", {})
                    paths[path] = {
                        "ts": _coerce_int(tokens.get("ts")),
                        "sign_input_sha8": tokens.get("sign_input_sha8"),
                        "sign_sha8": tokens.get("sign_sha8"),
                    }
            else:
                shop_entry["shop_id"] = _coerce_int(tokens.get("shop_id"))
                shop_entry["token_len"] = _coerce_int(tokens.get("token_len"))
                shop_entry["token_sha8"] = tokens.get("token_sha8")
    return data


@ops_phase1_auth_app.command("sign-fingerprint")
def ops_phase1_auth_sign_fingerprint(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    token_file: str = typer.Option(..., "--token-file", help="Apps Script export JSON path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    ts: int = typer.Option(1700000000, "--ts", help="Fixed timestamp"),
    out: str = typer.Option(
        "collaboration/env/auth_sign_fingerprint.json",
        "--out",
        help="Output JSON path",
    ),
) -> None:
    _maybe_load_env_file(env_file)
    settings = get_settings()
    if settings.shopee_partner_id is None or not settings.shopee_partner_key:
        raise typer.BadParameter("shopee partner credentials missing")
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    token_path = Path(token_file)
    if not token_path.exists():
        raise typer.BadParameter(f"file not found: {token_path}")
    token_map = _extract_appsscript_token_map(_read_json(token_path))

    ads_daily_path = os.environ.get(
        "ADS_DAILY_PATH", "/api/v2/ads/get_all_cpc_ads_daily_performance"
    ).strip() or "/api/v2/ads/get_all_cpc_ads_daily_performance"
    paths = [
        "/api/v2/shop/get_shop_info",
        ads_daily_path,
    ]
    timestamp = int(ts)
    shop_list = ",".join([shop.shop_key for shop in target_shops])
    print(f"sign_fingerprint_start shops={shop_list} ts={timestamp}")
    print(
        f"partner_id={settings.shopee_partner_id} "
        f"partner_key_sha8={_sha256_8(settings.shopee_partner_key)}"
    )

    payload: dict[str, object] = {
        "partner_id": settings.shopee_partner_id,
        "partner_key_sha8": _sha256_8(settings.shopee_partner_key),
        "timestamp": timestamp,
        "paths": paths,
        "shops": {},
    }

    for shop_cfg in target_shops:
        shop_id = _resolve_shop_id(shop_cfg)
        token_data = token_map.get(str(shop_id))
        access_token = token_data.get("access_token") if token_data else None
        if not access_token:
            print(f"token_missing shop={shop_cfg.shop_key} shop_id={shop_id}")
            raise typer.Exit(code=2)
        token_len = len(access_token)
        token_sha8 = _sha256_8(access_token)
        print(
            f"shop={shop_cfg.shop_key} shop_id={shop_id} "
            f"token_len={token_len} token_sha8={token_sha8}"
        )
        shop_entry: dict[str, object] = {
            "shop_id": shop_id,
            "token_len": token_len,
            "token_sha8": token_sha8,
            "paths": {},
        }
        for path in paths:
            sign_input = build_sign_base(
                settings.shopee_partner_id,
                path,
                timestamp,
                access_token=access_token,
                shop_id=shop_id,
            )
            sign = sign_hmac_sha256_hex(sign_input, settings.shopee_partner_key)
            sign_input_sha8 = _sha256_8(sign_input)
            sign_sha8 = _sha256_8(sign)
            print(
                f"shop={shop_cfg.shop_key} path={path} ts={timestamp} "
                f"sign_input_sha8={sign_input_sha8} sign_sha8={sign_sha8}"
            )
            shop_entry["paths"][path] = {
                "timestamp": timestamp,
                "sign_input_sha8": sign_input_sha8,
                "sign_sha8": sign_sha8,
            }
        payload["shops"][shop_cfg.shop_key] = shop_entry

    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_dump_json(payload, pretty=True), encoding="utf-8")
    print(f"sign_fingerprint_saved={out_path}")


@ops_phase1_auth_app.command("sign-parity")
def ops_phase1_auth_sign_parity(
    python_file: str = typer.Option(..., "--python-file", help="Python fingerprint JSON path"),
    appsscript_txt: str = typer.Option(
        ..., "--appsscript-txt", help="Apps Script fingerprint log text path"
    ),
) -> None:
    py_path = Path(python_file)
    if not py_path.exists():
        raise typer.BadParameter(f"python file not found: {py_path}")
    app_path = Path(appsscript_txt)
    if not app_path.exists():
        raise typer.BadParameter(f"appsscript file not found: {app_path}")

    python_data = _read_json(py_path)
    app_data = _parse_appsscript_sign_fingerprint_text(
        app_path.read_text(encoding="utf-8")
    )

    py_partner_id = python_data.get("partner_id")
    py_partner_key = python_data.get("partner_key_sha8")
    app_partner_id = app_data.get("partner_id")
    app_partner_key = app_data.get("partner_key_sha8")

    partner_id_ok = bool(
        py_partner_id is not None
        and app_partner_id is not None
        and int(py_partner_id) == int(app_partner_id)
    )
    partner_key_ok = bool(
        py_partner_key
        and app_partner_key
        and str(py_partner_key) == str(app_partner_key)
    )
    print(f"compare_partner_id ok={1 if partner_id_ok else 0}")
    print(f"compare_partner_key ok={1 if partner_key_ok else 0}")
    if not partner_id_ok:
        print(f"diff_partner_id python={py_partner_id} appsscript={app_partner_id}")
    if not partner_key_ok:
        print(
            f"diff_partner_key_sha8 python={py_partner_key} "
            f"appsscript={app_partner_key}"
        )

    parity_ok = partner_id_ok and partner_key_ok
    py_shops = python_data.get("shops", {}) if isinstance(python_data, dict) else {}
    app_shops = app_data.get("shops", {}) if isinstance(app_data, dict) else {}

    for shop_key, py_entry in py_shops.items():
        app_entry = app_shops.get(shop_key, {})
        shop_id_ok = bool(
            py_entry.get("shop_id") is not None
            and app_entry.get("shop_id") is not None
            and int(py_entry.get("shop_id")) == int(app_entry.get("shop_id"))
        )
        token_len_ok = py_entry.get("token_len") == app_entry.get("token_len")
        token_sha_ok = py_entry.get("token_sha8") == app_entry.get("token_sha8")
        shop_ok = shop_id_ok and token_len_ok and token_sha_ok
        print(
            "compare_shop "
            f"{shop_key} ok={1 if shop_ok else 0} "
            f"(len_ok={1 if token_len_ok else 0} "
            f"sha_ok={1 if token_sha_ok else 0} "
            f"shop_id_ok={1 if shop_id_ok else 0})"
        )
        if not shop_ok:
            if not shop_id_ok:
                print(
                    f"diff_shop_id shop={shop_key} "
                    f"python={py_entry.get('shop_id')} "
                    f"appsscript={app_entry.get('shop_id')}"
                )
            if not token_len_ok:
                print(
                    f"diff_token_len shop={shop_key} "
                    f"python={py_entry.get('token_len')} "
                    f"appsscript={app_entry.get('token_len')}"
                )
            if not token_sha_ok:
                print(
                    f"diff_token_sha8 shop={shop_key} "
                    f"python={py_entry.get('token_sha8')} "
                    f"appsscript={app_entry.get('token_sha8')}"
                )
        parity_ok = parity_ok and shop_ok

        py_paths = py_entry.get("paths", {}) if isinstance(py_entry, dict) else {}
        app_paths = app_entry.get("paths", {}) if isinstance(app_entry, dict) else {}
        for path, py_path_entry in py_paths.items():
            app_path_entry = app_paths.get(path, {})
            sign_input_ok = (
                py_path_entry.get("sign_input_sha8")
                == app_path_entry.get("sign_input_sha8")
            )
            sign_ok = (
                py_path_entry.get("sign_sha8")
                == app_path_entry.get("sign_sha8")
            )
            ts_ok = (
                _coerce_int(py_path_entry.get("timestamp"))
                == _coerce_int(app_path_entry.get("ts"))
            )
            path_ok = sign_input_ok and sign_ok and ts_ok
            print(
                "compare_sign "
                f"shop={shop_key} path={path} ok={1 if path_ok else 0} "
                f"(sign_input_ok={1 if sign_input_ok else 0} "
                f"sign_ok={1 if sign_ok else 0} "
                f"ts_ok={1 if ts_ok else 0})"
            )
            if not path_ok:
                if not ts_ok:
                    print(
                        f"diff_sign_ts shop={shop_key} path={path} "
                        f"python={py_path_entry.get('timestamp')} "
                        f"appsscript={app_path_entry.get('ts')}"
                    )
                if not sign_input_ok:
                    print(
                        f"diff_sign_input_sha8 shop={shop_key} path={path} "
                        f"python={py_path_entry.get('sign_input_sha8')} "
                        f"appsscript={app_path_entry.get('sign_input_sha8')}"
                    )
                if not sign_ok:
                    print(
                        f"diff_sign_sha8 shop={shop_key} path={path} "
                        f"python={py_path_entry.get('sign_sha8')} "
                        f"appsscript={app_path_entry.get('sign_sha8')}"
                    )
            parity_ok = parity_ok and path_ok

    print(f"sign_parity_ok={1 if parity_ok else 0}")
    if not parity_ok:
        print("next_steps: check token source, path string, timestamp, and encoding parity")
        raise typer.Exit(code=2)


@ops_phase1_baseline_app.command("shop-info")
def ops_phase1_baseline_shop_info(
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    token_file: str = typer.Option(..., "--token-file", help="Apps Script export JSON path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    token_mode: str = typer.Option(
        "passive", "--token-mode", help="default | passive"
    ),
    date_value: str | None = typer.Option(None, "--date", help="YYYY-MM-DD"),
    save_failure_artifacts: bool = typer.Option(
        True, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
) -> None:
    transport_value = "live"
    token_mode_value = _normalize_token_mode(token_mode)
    _maybe_load_env_file(env_file)
    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if not (allow_network or allow_network_env):
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    settings = get_settings()
    _require_shopee_settings(settings)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    token_path = Path(token_file)
    if not token_path.exists():
        raise typer.BadParameter(f"file not found: {token_path}")
    token_map = _extract_appsscript_token_map(_read_json(token_path))

    target_date = _parse_date_or_today(date_value)
    failure_root = _resolve_failure_artifacts_root()
    client = _build_sweep_client(settings)
    overall_ok = True

    for shop_cfg in target_shops:
        shop_key = shop_cfg.shop_key
        shop_id = _resolve_shop_id(shop_cfg)
        token_data = token_map.get(str(shop_id))
        access_token = token_data.get("access_token") if token_data else None
        if not access_token:
            print(f"token_missing shop={shop_key} shop_id={shop_id}")
            overall_ok = False
            continue

        timestamp = int(datetime.now(timezone.utc).timestamp())
        access_token_debug = _build_access_token_encoding_flags(access_token)
        safe_fingerprint = _build_safe_fingerprint(
            access_token=access_token,
            partner_id=settings.shopee_partner_id,
            partner_key=settings.shopee_partner_key,
            path="/api/v2/shop/get_shop_info",
            timestamp=timestamp,
            shop_id=shop_id,
        )
        http_status: int | None = None
        response_payload: dict | None = None
        response_text_head: str | None = None
        api_error = None
        api_message = None
        request_id = None
        ok = True
        error_code = None
        try:
            _validate_outgoing_access_token(
                access_token=access_token,
                shop_key=shop_key,
                path="/api/v2/shop/get_shop_info",
            )
            response_payload = client.request(
                "GET",
                "/api/v2/shop/get_shop_info",
                shop_id=shop_id,
                access_token=access_token,
                timestamp=timestamp,
            )
            http_status = 200
        except Exception as exc:  # noqa: BLE001
            ok = False
            if hasattr(exc, "response") and getattr(exc, "response") is not None:
                http_status = getattr(exc.response, "status_code", None)
                response_payload, response_text_head = _parse_response_payload(
                    exc.response
                )
            else:
                response_text_head = _scrub_sensitive_text(str(exc)) or "unknown_error"

        if isinstance(response_payload, dict):
            error_code = response_payload.get("error")
            if error_code == "":
                error_code = None
        api_error, api_message, request_id, _warn, _dbg = _extract_api_fields(
            response_payload
        )
        if api_message is None and response_text_head:
            api_message = response_text_head
        if ok and error_code not in (None, 0, "0"):
            ok = False

        http_text = str(http_status) if http_status is not None else "-"
        api_error_text = _format_api_value(api_error)
        api_message_text = _format_api_value(api_message)
        request_id_text = _format_api_value(request_id)
        print(
            "baseline_shop_info "
            f"shop={shop_key} http={http_text} "
            f"api_error={api_error_text} api_message={api_message_text} "
            f"request_id={request_id_text}"
        )

        reason = _classify_reachability(http_status, ok, error_code)[1]
        if save_failure_artifacts:
            _write_failure_artifact(
                root=failure_root,
                shop_key=shop_key,
                target_date=target_date,
                call_name="baseline_shop_info",
                api_path="/api/v2/shop/get_shop_info",
                method="GET",
                query_keys=_build_query_keys(
                    None, shop_id=shop_id, access_token=access_token
                ),
                http_status=http_status,
                api_error=api_error,
                api_message=api_message,
                request_id=request_id,
                payload=response_payload,
                response_text_head=response_text_head,
                reason=reason,
                safe_fingerprint=safe_fingerprint,
                access_token_debug=access_token_debug,
            )
        if http_status != 200 or not ok:
            overall_ok = False

    client.close()
    if not overall_ok:
        raise typer.Exit(code=2)


def _normalize_ads_candidate_entries(
    raw_entries: object,
) -> list[dict[str, object | None]]:
    entries: list[dict[str, object | None]] = []
    if not raw_entries:
        return entries
    if not isinstance(raw_entries, list):
        return entries
    for entry in raw_entries:
        if isinstance(entry, str):
            path = entry.strip()
            if path:
                entries.append(
                    {
                        "path": path,
                        "fixture": None,
                        "status": None,
                        "method": None,
                        "body": None,
                    }
                )
            continue
        if isinstance(entry, dict):
            path = entry.get("path") or entry.get("candidate") or entry.get("value")
            if path:
                fixture = entry.get("fixture") or entry.get("fixture_file")
                status = entry.get("status") or entry.get("http_status")
                method = entry.get("method")
                body = entry.get("json") or entry.get("body")
                entries.append(
                    {
                        "path": str(path),
                        "fixture": str(fixture) if fixture else None,
                        "status": int(status) if status is not None else None,
                        "method": str(method).upper() if method else None,
                        "body": body,
                    }
                )
    return entries


def _dedupe_ads_candidates(
    entries: list[dict[str, object | None]],
) -> list[dict[str, object | None]]:
    seen: set[str] = set()
    deduped: list[dict[str, object | None]] = []
    for entry in entries:
        path = (entry.get("path") or "").strip()
        method = (entry.get("method") or "GET").strip().upper()
        key = f"{method} {path}"
        if not path or key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "path": path,
                "fixture": entry.get("fixture"),
                "status": entry.get("status"),
                "method": method,
                "body": entry.get("body"),
            }
        )
    return deduped


def _extract_record_count(payload: dict | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    response = payload.get("response")
    if isinstance(response, dict):
        for list_key in ["records", "data", "items", "list"]:
            value = response.get(list_key)
            if isinstance(value, list):
                return len(value)
    return None


def _select_best_candidate(
    results: list[dict[str, object]],
) -> str | None:
    reachable_results = [item for item in results if item.get("reachable") is True]
    if not reachable_results:
        return None
    def score(item: dict[str, object]) -> tuple[int, int, int]:
        ok_score = 1 if item.get("ok") is True else 0
        record_count = item.get("records")
        record_score = record_count if isinstance(record_count, int) else -1
        order = item.get("order")
        order_score = -order if isinstance(order, int) else 0
        return (ok_score, record_score, order_score)
    best = max(reachable_results, key=score)
    return str(best.get("candidate"))


def _classify_reachability(
    http_status: int | None,
    ok: bool,
    error_code: object | None,
) -> tuple[bool, str]:
    if http_status == 404:
        return False, "path_not_found"
    if http_status is None:
        return False, "request_failed"
    if http_status in {401, 403}:
        return True, "auth_failed"
    if http_status >= 400:
        return True, f"http_{http_status}"
    if ok:
        return True, "ok"
    if error_code is None:
        return True, "shopee_error"
    if isinstance(error_code, int):
        return True, f"shopee_error_{error_code}"
    return True, str(error_code)


def _build_query_keys(
    params: dict | None,
    *,
    shop_id: int | None,
    access_token: str | None,
) -> list[str]:
    keys = {"partner_id", "timestamp", "sign"}
    if shop_id is not None:
        keys.add("shop_id")
    if access_token is not None:
        keys.add("access_token")
    if params:
        keys.update(str(key) for key in params.keys())
    return sorted(keys)


def _build_sweep_client(settings, transport=None, allow_fallback: bool = False) -> ShopeeClient:
    partner_id = settings.shopee_partner_id
    partner_key = settings.shopee_partner_key
    if allow_fallback:
        partner_id = partner_id or 0
        partner_key = partner_key or "DUMMY_PARTNER_KEY"
    if partner_id is None or partner_key is None:
        raise RuntimeError("shopee credentials missing")
    return ShopeeClient(
        partner_id=partner_id,
        partner_key=partner_key,
        host=settings.shopee_api_host,
        transport=transport,
    )


def _extract_api_fields(payload: dict | None) -> tuple[object | None, str | None, str | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None, None, None, None
    api_error = payload.get("error")
    if api_error is None:
        api_error = payload.get("error_code")
    if api_error == "":
        api_error = None
    api_message = payload.get("message")
    if api_message is None:
        api_message = payload.get("msg")
    if api_message is None:
        api_message = payload.get("error_msg")
    if api_message == "":
        api_message = None
    request_id = payload.get("request_id")
    if request_id is None:
        request_id = payload.get("requestId")
    if request_id is None:
        request_id = payload.get("requestid")
    warning = payload.get("warning")
    if warning is None:
        warning = payload.get("warning_msg")
    debug_msg = payload.get("debug_msg")
    if debug_msg is None:
        debug_msg = payload.get("debug")
    return api_error, api_message, request_id, warning, debug_msg


def _format_api_value(value: object | None) -> str:
    if value is None:
        return "-"
    text = str(value)
    if not text:
        return "-"
    return redact_text(text)


def _as_dict(value: object | None) -> dict:
    return value if isinstance(value, dict) else {}


def _pick_value(*values: object | None) -> object | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _extract_failure_summary(
    payload: dict,
    *,
    shop_fallback: str,
    file_path: Path,
) -> dict[str, object] | None:
    meta = _as_dict(payload.get("__meta"))
    request_meta = _as_dict(payload.get("request_meta"))
    response_meta = _as_dict(payload.get("response_meta"))
    parsed_error = _as_dict(payload.get("parsed_error"))

    shop_key = _pick_value(meta.get("shop_key"), payload.get("shop_key"), shop_fallback)
    call_name = _pick_value(
        meta.get("call_name"),
        payload.get("call_name"),
        request_meta.get("call_name"),
        file_path.stem,
    )
    path = _pick_value(
        request_meta.get("path"),
        payload.get("path"),
        meta.get("path"),
    )
    http_status = _pick_value(
        response_meta.get("http_status"),
        payload.get("http_status"),
        payload.get("status"),
    )
    api_error = _pick_value(
        parsed_error.get("api_error"),
        payload.get("api_error"),
        payload.get("error"),
        payload.get("error_code"),
    )
    api_message = _pick_value(
        parsed_error.get("api_message"),
        payload.get("api_message"),
        payload.get("message"),
        payload.get("msg"),
        payload.get("error_msg"),
    )
    request_id = _pick_value(
        parsed_error.get("request_id"),
        payload.get("request_id"),
        payload.get("requestId"),
        payload.get("requestid"),
    )

    http_status = _coerce_int(http_status)
    if not any([path, http_status is not None, api_error, api_message, request_id]):
        return None

    return {
        "shop": str(shop_key) if shop_key is not None else shop_fallback,
        "call_name": call_name,
        "path": path,
        "http": http_status,
        "api_error": api_error,
        "api_message": api_message,
        "request_id": request_id,
    }


def _is_baseline_record(record: dict[str, object]) -> bool:
    path = str(record.get("path") or "")
    call_name = str(record.get("call_name") or "")
    return "/api/v2/shop/get_shop_info" in path or "shop_info" in call_name


def _is_ads_record(record: dict[str, object]) -> bool:
    path = str(record.get("path") or "")
    call_name = str(record.get("call_name") or "")
    return "/api/v2/ads" in path or call_name.startswith("ads")


def _derive_failure_hint(record: dict[str, object], baseline_http: int | None) -> str:
    http_status = _coerce_int(record.get("http"))
    if _is_baseline_record(record):
        if http_status == 200:
            return "base_auth_ok"
        if http_status == 403:
            return "sign_or_token_mismatch"
        return "-"
    if _is_ads_record(record):
        if baseline_http == 200 and http_status == 403:
            return "ads_permission_or_scope"
        if baseline_http == 403:
            return "sign_or_token_mismatch"
    return "-"


def _format_summary_cell(value: object | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, (dict, list)):
        import json

        text = json.dumps(value, ensure_ascii=True)
    else:
        text = str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    text = redact_text(text)
    text = _scrub_sensitive_text(text)
    text = text.replace("|", "\\|")
    if not text:
        return "-"
    if len(text) > 160:
        text = f"{text[:157]}..."
    return text


def _parse_artifact_prefix_ms(path: Path) -> int | None:
    name = path.name
    if "_" not in name:
        return None
    prefix = name.split("_", 1)[0]
    if not prefix.isdigit():
        return None
    try:
        return int(prefix)
    except ValueError:
        return None


def _sha256_8(value: str | None) -> str:
    if not value:
        return "-"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _build_access_token_encoding_flags(access_token: str | None) -> dict[str, int]:
    if not access_token:
        return {
            "token_has_plus": 0,
            "token_has_slash": 0,
            "token_has_equal": 0,
            "access_token_encoded_has_%2B": 0,
            "access_token_encoded_has_plus": 0,
        }
    encoded = quote_plus(access_token, safe="")
    encoded_upper = encoded.upper()
    return {
        "token_has_plus": 1 if "+" in access_token else 0,
        "token_has_slash": 1 if "/" in access_token else 0,
        "token_has_equal": 1 if "=" in access_token else 0,
        "access_token_encoded_has_%2B": 1 if "%2B" in encoded_upper else 0,
        "access_token_encoded_has_plus": 1 if "+" in encoded else 0,
    }


def _build_safe_fingerprint(
    *,
    access_token: str | None,
    partner_id: int | None,
    partner_key: str | None,
    path: str,
    timestamp: int,
    shop_id: int | None,
    omit_access_token: bool = False,
) -> dict[str, object]:
    access_token_len = len(access_token) if access_token else 0
    token_sha = _sha256_8(access_token)
    sign_input = None
    sign = None
    if partner_id is not None:
        sign_input = build_sign_base(
            partner_id,
            path,
            timestamp,
            access_token=access_token,
            shop_id=shop_id,
            omit_access_token=omit_access_token,
        )
        if partner_key:
            sign = sign_hmac_sha256_hex(sign_input, partner_key)
    return {
        "access_token_len": access_token_len,
        "access_token_sha256_8": token_sha,
        "sign_sha256_8": _sha256_8(sign),
        "sign_input_sha256_8": _sha256_8(sign_input),
    }


def _format_auth_debug(safe_fingerprint: dict[str, object] | None) -> str:
    if not safe_fingerprint:
        return ""
    token_len = safe_fingerprint.get("access_token_len", "-")
    token_sha = safe_fingerprint.get("access_token_sha256_8", "-")
    sign_in = safe_fingerprint.get("sign_input_sha256_8", "-")
    sign_sha = safe_fingerprint.get("sign_sha256_8", "-")
    return (
        f"token_len={token_len} token_sha={token_sha} "
        f"sign_in={sign_in} sign={sign_sha}"
    )


def _validate_outgoing_access_token(
    *,
    access_token: str | None,
    shop_key: str,
    path: str,
) -> dict[str, object]:
    token_len = len(access_token) if access_token else 0
    token_sha = _sha256_8(access_token)
    if not access_token or "***" in access_token:
        raise RuntimeError(
            "outgoing_access_token_invalid_or_redacted "
            f"shop={shop_key} path={path} "
            f"access_token_len={token_len} access_token_sha256_8={token_sha}"
        )
    return {
        "access_token_len": token_len,
        "access_token_sha256_8": token_sha,
    }


def _parse_preflight_output(output: str) -> dict[str, object]:
    rows: dict[str, dict[str, object]] = {}
    ok = None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("shop="):
            continue
        if "access_expires_in_sec=" not in line:
            continue
        parts = {}
        for token in line.split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            parts[key.strip()] = value.strip()
        shop_key = parts.get("shop")
        if not shop_key:
            continue
        rows[shop_key] = {
            "shop_id": _coerce_int(parts.get("shop_id")),
            "token_source": parts.get("token_source") or "-",
            "access_expires_in_sec": _coerce_int(parts.get("access_expires_in_sec")),
            "refresh_expires_in_sec": _coerce_int(parts.get("refresh_expires_in_sec")),
            "token_verdict": parts.get("token_verdict") or parts.get("verdict") or "-",
            "token_len": _coerce_int(parts.get("token_len")),
            "token_sha8": parts.get("token_sha8") or "-",
        }
    if "preflight_ok=1" in output:
        ok = True
    elif "preflight_ok=0" in output:
        ok = False
    return {"ok": ok, "rows": rows}


def _parse_sweep_output(output: str, skipped: bool) -> dict[str, object]:
    status = "skipped" if skipped else "unknown"
    reason = "flag" if skipped else "-"
    if "sweep_ok=1" in output:
        status = "ok"
    elif "sweep_ok=0" in output:
        status = "failed"
    if "sweep_skipped reason=flag" in output:
        status = "skipped"
        reason = "flag"

    failure_dir = None
    failure_total = None
    per_shop: dict[str, int] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("failure_artifacts_dir="):
            failure_dir = line.split("=", 1)[1].strip()
        if line.startswith("failure_artifacts_saved="):
            value = line.split("=", 1)[1].strip()
            failure_total = _coerce_int(value)
        if line.startswith("shop=") and "failure_artifacts_saved=" in line:
            parts = {}
            for token in line.split():
                if "=" not in token:
                    continue
                key, value = token.split("=", 1)
                parts[key.strip()] = value.strip()
            shop_key = parts.get("shop")
            count = _coerce_int(parts.get("failure_artifacts_saved"))
            if shop_key and count is not None:
                per_shop[shop_key] = count
    return {
        "status": status,
        "reason": reason,
        "failure_artifacts_dir": failure_dir,
        "failure_artifacts_saved": failure_total,
        "failure_artifacts_per_shop": per_shop,
    }


def _parse_summarize_output(output: str, summary_path: Path) -> dict[str, object]:
    saved_path = None
    records = None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("summarize_failures_saved="):
            parts = line.split()
            for part in parts:
                if part.startswith("summarize_failures_saved="):
                    saved_path = part.split("=", 1)[1]
                if part.startswith("records="):
                    records = _coerce_int(part.split("=", 1)[1])
    if saved_path is None:
        saved_path = str(summary_path)
    if records is None:
        records = _count_summary_records(summary_path)
    preview = _summary_table_preview(summary_path, limit=5)
    return {
        "saved_path": saved_path,
        "records": records,
        "preview": preview,
    }


def _count_summary_records(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("|"):
            continue
        if line.startswith("|---"):
            continue
        if line.startswith("| shop |"):
            continue
        if "call_name" in line and "request_id" in line:
            continue
        count += 1
    return max(0, count)


def _summary_table_preview(path: Path, limit: int = 5) -> list[str]:
    if not path.exists():
        return []
    lines: list[str] = []
    table_lines: list[str] = []
    in_table = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("| shop |"):
            in_table = True
            table_lines.append(line)
            continue
        if in_table and line.startswith("|---"):
            table_lines.append(line)
            continue
        if in_table and line.startswith("|"):
            table_lines.append(line)
            continue
        if in_table and line == "":
            break
    header: list[str] = []
    data_rows: list[str] = []
    for line in table_lines:
        if line.startswith("| shop |") or line.startswith("|---"):
            header.append(line)
        else:
            data_rows.append(line)
    lines.extend(header)
    lines.extend(data_rows[:limit])
    sanitized: list[str] = []
    for line in lines:
        scrubbed = _scrub_sensitive_text(redact_text(line))
        sanitized.append(scrubbed)
    return sanitized


def _parse_failure_summary_markdown(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    if not path.exists():
        return records
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("|"):
            continue
        if line.startswith("|---"):
            continue
        if "call_name" in line and "request_id" in line:
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 8:
            continue
        shop, call_name, path_value, http, api_error, api_message, request_id, hint = (
            parts[:8]
        )
        record = {
            "shop": shop if shop != "-" else None,
            "call_name": call_name if call_name != "-" else None,
            "path": path_value if path_value != "-" else None,
            "http": _coerce_int(http),
            "api_error": api_error if api_error != "-" else None,
            "api_message": api_message if api_message != "-" else None,
            "request_id": request_id if request_id != "-" else None,
            "hint": hint if hint != "-" else None,
        }
        records.append(record)
    return records


def _pick_http_value(values: list[int | None], prefer: list[int]) -> int | None:
    for pref in prefer:
        if pref in values:
            return pref
    for value in values:
        if value is not None:
            return value
    return None


def _compute_shop_verdict(
    records: list[dict[str, object]],
    shop_key: str,
    *,
    force_skipped: bool = False,
) -> tuple[str, int | None, int | None]:
    if force_skipped:
        return "skipped", None, None

    shop_records = [item for item in records if item.get("shop") == shop_key]
    if not shop_records:
        return "no_evidence", None, None

    baseline_records = [item for item in shop_records if _is_baseline_record(item)]
    ads_records = [item for item in shop_records if _is_ads_record(item)]

    baseline_values = [
        _coerce_int(item.get("http")) for item in baseline_records if item is not None
    ]
    ads_values = [
        _coerce_int(item.get("http")) for item in ads_records if item is not None
    ]

    baseline_http = _pick_http_value(baseline_values, prefer=[200, 403, 401])
    ads_http = _pick_http_value(ads_values, prefer=[403, 401, 200])

    baseline_missing = not baseline_records
    ads_missing = not ads_records

    if baseline_missing and ads_missing:
        return "no_evidence", None, None

    if baseline_records and ads_missing:
        if baseline_http == 200:
            return "base_auth_ok_only", baseline_http, None
        if baseline_http in {401, 403}:
            return "sign_or_token_mismatch", baseline_http, None
        return "no_evidence", baseline_http, None

    if baseline_http in {401, 403}:
        return "sign_or_token_mismatch", baseline_http, ads_http
    if baseline_missing and ads_http in {401, 403}:
        return "sign_or_token_mismatch", baseline_http, ads_http
    if baseline_http == 200 and ads_http == 403:
        return "ads_permission_or_scope", baseline_http, ads_http
    if baseline_http == 200 and ads_http == 200:
        return "ok", baseline_http, ads_http
    if baseline_missing and ads_http == 200:
        return "ok", baseline_http, ads_http
    if baseline_http == 200 and ads_http is None:
        return "base_auth_ok_only", baseline_http, ads_http
    if baseline_missing:
        return "no_evidence", baseline_http, ads_http
    return "ok", baseline_http, ads_http


def _first_request_id(records: list[dict[str, object]]) -> str | None:
    for record in records:
        request_id = record.get("request_id")
        if request_id:
            return str(request_id)
    return None


def _build_verdict_entries(
    records: list[dict[str, object]],
    shop_list: list[str],
    *,
    force_skipped: bool = False,
    preflight_rows: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for shop_key in shop_list:
        verdict, baseline_http, ads_http = _compute_shop_verdict(
            records, shop_key, force_skipped=force_skipped
        )
        baseline_records = [
            item for item in records if item.get("shop") == shop_key and _is_baseline_record(item)
        ]
        ads_records = [
            item for item in records if item.get("shop") == shop_key and _is_ads_record(item)
        ]
        baseline_request_id = _first_request_id(baseline_records)
        ads_request_id = _first_request_id(ads_records)

        token_verdict = "-"
        if preflight_rows and shop_key in preflight_rows:
            token_verdict = str(
                preflight_rows[shop_key].get("token_verdict")
                or preflight_rows[shop_key].get("verdict")
                or "-"
            )

        entry = {
            "shop": shop_key,
            "verdict": verdict,
            "baseline_http": str(baseline_http) if baseline_http is not None else "-",
            "ads_http": str(ads_http) if ads_http is not None else "-",
            "baseline_request_id": baseline_request_id or "-",
            "ads_request_id": ads_request_id or "-",
            "token_verdict": token_verdict,
        }
        entries.append(entry)
    return entries


def _next_steps_for_verdict(entry: dict[str, str]) -> str:
    verdict = entry.get("verdict") or "-"
    baseline_http = entry.get("baseline_http") or "-"
    ads_http = entry.get("ads_http") or "-"
    ads_request_id = entry.get("ads_request_id") or "-"
    baseline_request_id = entry.get("baseline_request_id") or "-"
    token_verdict = entry.get("token_verdict") or "-"

    if token_verdict in {"expired", "short_ttl", "missing", "unknown"}:
        return "Apps Script에서 diag_TOKEN() 후 export 재생성"
    if verdict in {"skipped", "no_evidence"}:
        return "증거 없음: sweep 실행 또는 artifacts_root 확인 필요"
    if verdict == "base_auth_ok_only":
        return "baseline만 확인됨: Ads endpoints sweep 재실행 또는 후보 경로 점검"
    if verdict == "ads_permission_or_scope":
        return (
            "Ads API 권한/활성화/스코프 가능성 높음 "
            f"(evidence ads_http={ads_http} request_id={ads_request_id})"
        )
    if verdict == "sign_or_token_mismatch":
        return (
            "sign/token mismatch 가능성 "
            f"(baseline_http={baseline_http} request_id={baseline_request_id})"
        )
    if verdict == "ok":
        return "정상 (추가 액션 필요 없음)"
    return "점검 필요"


def _write_evidence_report(
    path: Path,
    *,
    header: dict[str, str],
    preflight: dict[str, object],
    sweep_status: dict[str, object] | None,
    summary_status: dict[str, object] | None,
    verdicts: list[dict[str, str]],
) -> None:
    lines: list[str] = []
    lines.append("# Phase1 Evidence Report")
    lines.append("")
    for key in [
        "date",
        "shops",
        "transport",
        "token_mode",
        "min_access_ttl_sec",
        "allow_network",
        "skip_sweep",
        "skip_preview",
    ]:
        value = header.get(key, "-")
        lines.append(f"{key}: {value}")
    lines.append("")

    lines.append("## Preflight")
    preflight_ok = preflight.get("ok")
    if preflight_ok is True:
        lines.append("preflight_ok=1")
    elif preflight_ok is False:
        lines.append("preflight_ok=0")
    else:
        lines.append("preflight_ok=-")
    lines.append(
        "| shop | access_expires_in_sec | refresh_expires_in_sec | token_verdict |"
    )
    lines.append("|---|---:|---:|---|")
    rows = preflight.get("rows") if isinstance(preflight, dict) else {}
    for shop_key in _parse_only_shops(header.get("shops")) or []:
        row = rows.get(shop_key, {}) if isinstance(rows, dict) else {}
        access = row.get("access_expires_in_sec", "-")
        refresh = row.get("refresh_expires_in_sec", "-")
        verdict = row.get("token_verdict", row.get("verdict", "-"))
        lines.append(f"| {shop_key} | {access} | {refresh} | {verdict} |")
    lines.append("")

    lines.append("## Sweep")
    if sweep_status is None:
        lines.append("sweep_status=unknown")
    else:
        status = sweep_status.get("status", "-")
        reason = sweep_status.get("reason", "-")
        lines.append(f"sweep_status={status} reason={reason}")
        failure_dir = sweep_status.get("failure_artifacts_dir")
        failure_total = sweep_status.get("failure_artifacts_saved")
        if failure_dir:
            lines.append(f"failure_artifacts_dir={failure_dir}")
        if failure_total is not None:
            lines.append(f"failure_artifacts_saved={failure_total}")
        per_shop = sweep_status.get("failure_artifacts_per_shop")
        if isinstance(per_shop, dict) and per_shop:
            for shop_key, count in per_shop.items():
                lines.append(f"shop={shop_key} failure_artifacts_saved={count}")
    lines.append("")

    lines.append("## Failure Summary")
    if summary_status is None:
        lines.append("summarize_failures_saved=- records=-")
    else:
        saved_path = summary_status.get("saved_path", "-")
        records = summary_status.get("records", "-")
        lines.append(f"summarize_failures_saved={saved_path} records={records}")
        preview = summary_status.get("preview") if isinstance(summary_status, dict) else []
        if isinstance(preview, list) and preview:
            lines.append("preview:")
            lines.append("```")
            for line in preview:
                lines.append(_scrub_sensitive_text(redact_text(line)))
            lines.append("```")
    lines.append("")

    lines.append("## Verdict + Next Steps")
    for entry in verdicts:
        lines.append(
            "shop={shop} verdict={verdict} baseline_http={baseline_http} ads_http={ads_http}".format(
                **entry
            )
        )
        lines.append(f"next_steps: {_next_steps_for_verdict(entry)}")
    lines.append("")

    safe_lines = [_scrub_sensitive_text(redact_text(line)) for line in lines]
    path.write_text("\n".join(safe_lines), encoding="utf-8")


def _collect_artifact_files(
    root: Path,
    shop_list: list[str],
    target_date: date,
) -> list[Path]:
    files: list[Path] = []
    for shop_key in shop_list:
        shop_root = root / shop_key / target_date.isoformat()
        if not shop_root.exists():
            continue
        for path in shop_root.rglob("*"):
            if path.is_file():
                files.append(path)
    return files


def _build_support_request_template(
    *,
    target_date: date,
    shops: list[str],
    failure_records: list[dict[str, object]],
    max_request_ids: int,
) -> list[str]:
    lines: list[str] = []
    lines.append("# Phase1 Support Request")
    lines.append("")
    lines.append(f"date: {target_date.isoformat()}")
    lines.append(f"shops: {','.join(shops)}")
    lines.append("timezone: Asia/Ho_Chi_Minh")
    lines.append("")

    observed_paths = sorted(
        {
            str(record.get("path"))
            for record in failure_records
            if record.get("path")
        }
    )
    failing_paths = sorted(
        {
            str(record.get("path"))
            for record in failure_records
            if record.get("path")
            and _coerce_int(record.get("http")) is not None
            and _coerce_int(record.get("http")) >= 400
        }
    )
    lines.append("## Endpoints Observed (unique paths)")
    if observed_paths:
        for path in observed_paths:
            lines.append(f"- {path}")
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Failing Endpoints (http >= 400)")
    if failing_paths:
        for path in failing_paths:
            lines.append(f"- {path}")
    else:
        lines.append("- (none)")
    lines.append("")

    samples: list[str] = []
    for record in failure_records:
        http = record.get("http")
        api_error = record.get("api_error")
        api_message = record.get("api_message")
        sample = f"http={http} api_error={api_error} api_message={api_message}"
        if sample not in samples:
            samples.append(sample)
        if len(samples) >= 5:
            break
    lines.append("## Sample Errors")
    if samples:
        for sample in samples:
            lines.append(f"- {sample}")
    else:
        lines.append("- (none)")
    lines.append("")

    request_ids: list[str] = []
    for record in failure_records:
        req = record.get("request_id")
        if req is None:
            continue
        req_text = str(req).strip()
        if not req_text or req_text == "-":
            continue
        if req_text not in request_ids:
            request_ids.append(req_text)
        if len(request_ids) >= max_request_ids:
            break
    lines.append(f"## Request IDs (max {max_request_ids})")
    if request_ids:
        lines.append(", ".join(request_ids))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("## Observations")
    for shop_key in shops:
        verdict, baseline_http, ads_http = _compute_shop_verdict(
            failure_records, shop_key
        )
        baseline_text = str(baseline_http) if baseline_http is not None else "-"
        ads_text = str(ads_http) if ads_http is not None else "-"
        if verdict == "ads_permission_or_scope":
            lines.append(
                f"- {shop_key}: baseline shop_info=200 but ads=403 "
                f"(baseline_http={baseline_text} ads_http={ads_text})"
            )
        elif verdict == "sign_or_token_mismatch":
            lines.append(
                f"- {shop_key}: baseline auth failed or missing "
                f"(baseline_http={baseline_text} ads_http={ads_text})"
            )
        elif verdict == "base_auth_ok_only":
            lines.append(
                f"- {shop_key}: baseline ok, ads evidence missing "
                f"(baseline_http={baseline_text})"
            )
        elif verdict == "no_evidence":
            lines.append(f"- {shop_key}: no evidence collected")
        elif verdict == "ok":
            lines.append(
                f"- {shop_key}: baseline and ads returned ok "
                f"(baseline_http={baseline_text} ads_http={ads_text})"
            )
    lines.append("")

    lines.append(
        "Attached zip includes redacted evidence report, failures summary, and artifacts."
    )

    safe_lines = [_scrub_sensitive_text(redact_text(line)) for line in lines]
    return safe_lines


def _scan_files_for_secrets(files: list[Path]) -> Path | None:
    import re

    keys = [
        "access_token",
        "refresh_token",
        "partner_key",
        "authorization",
        "cookie",
        "sign",
    ]
    json_patterns = [
        re.compile(rf'"{key}"\s*:\s*"([^"]*)"', re.IGNORECASE) for key in keys
    ]
    json_null_patterns = [
        re.compile(rf'"{key}"\s*:\s*(null)', re.IGNORECASE) for key in keys
    ]
    kv_patterns = [
        re.compile(rf"\b{key}\b\s*=\s*([^\s,;&]+)", re.IGNORECASE) for key in keys
    ]

    def is_safe(value: str) -> bool:
        lower = value.strip().strip("\"'`").lower()
        while lower and lower[-1] in {"\"", "'", "`", ")", "]", "}", ".", ":"}:
            lower = lower[:-1]
        if lower.startswith("***"):
            return True
        if lower in {"", "***", "null", "none"}:
            return True
        return False

    for path in files:
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            continue
        for pattern in json_null_patterns:
            if pattern.search(content):
                continue
        for pattern in json_patterns:
            for match in pattern.finditer(content):
                value = match.group(1)
                if not is_safe(value):
                    return path
        for pattern in kv_patterns:
            for match in pattern.finditer(content):
                value = match.group(1)
                if not is_safe(value):
                    return path
    return None


def _build_phase1_schedule_support_packet(
    *,
    job: str,
    anchor_date: date,
    ingest_date: date,
    artifacts_root: Path,
    per_shop: dict,
) -> dict[str, object]:
    """
    Minimal support packet for schedule run-once:
    - generated schedule summary markdown
    - generated HTML reports (per shop)
    - failure artifacts for ingest_date (if any)
    """
    out_dir = Path("collaboration") / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_job = re.sub(r"[^a-z0-9_-]+", "_", job.strip().lower())
    stamp = f"{anchor_date.isoformat()}_{safe_job}"
    md_path = out_dir / f"phase1_schedule_run_once_{stamp}.md"
    zip_path = out_dir / f"phase1_schedule_support_packet_{stamp}.zip"

    shop_keys = sorted([str(k) for k in per_shop.keys()])
    lines: list[str] = []
    lines.append("# Phase1 Schedule Support Packet")
    lines.append("")
    lines.append(f"job: {job}")
    lines.append(f"anchor_date: {anchor_date.isoformat()}")
    lines.append(f"ingest_date: {ingest_date.isoformat()}")
    lines.append(f"shops: {', '.join(shop_keys)}")
    lines.append("timezone: Asia/Ho_Chi_Minh")
    lines.append("")

    lines.append("## Per-Shop")
    for shop_key in shop_keys:
        row = per_shop.get(shop_key) if isinstance(per_shop, dict) else {}
        if not isinstance(row, dict):
            row = {}
        report_path = row.get("report_path")
        calls_ok = row.get("calls_ok", "-")
        calls_fail = row.get("calls_fail", "-")
        campaigns = row.get("campaigns", "-")
        daily = row.get("daily", "-")
        snapshots = row.get("snapshots", "-")
        error = row.get("error") or "-"
        lines.append(
            f"- shop={shop_key} calls_ok={calls_ok} calls_fail={calls_fail} "
            f"campaigns={campaigns} daily={daily} snapshots={snapshots}"
        )
        if report_path:
            lines.append(f"  report_path={report_path}")
        if error != "-":
            lines.append(f"  error={error}")
    lines.append("")

    md_path.write_text(
        "\n".join([_scrub_sensitive_text(redact_text(x)) for x in lines]),
        encoding="utf-8",
    )

    entries: list[tuple[Path, str]] = [(md_path, str(Path("schedule") / md_path.name))]

    # Add report HTMLs
    for shop_key in shop_keys:
        row = per_shop.get(shop_key) if isinstance(per_shop, dict) else {}
        if not isinstance(row, dict):
            continue
        report_path = row.get("report_path")
        if not report_path:
            continue
        path = Path(str(report_path))
        if path.exists():
            entries.append((path, str(Path("reports") / shop_key / path.name)))

    # Add failure artifacts if present for this ingest_date
    for shop_key in shop_keys:
        shop_root = artifacts_root / shop_key / ingest_date.isoformat()
        if not shop_root.exists():
            continue
        for path in sorted(shop_root.rglob("*.json")):
            if path.is_file():
                try:
                    rel = path.relative_to(artifacts_root)
                except ValueError:
                    rel = Path(shop_key) / ingest_date.isoformat() / path.name
                entries.append((path, str(Path("artifacts") / rel)))

    leak_path = _scan_files_for_secrets([p for p, _a in entries])
    if leak_path:
        return {"ok": False, "error": f"secret_leak_detected file={leak_path}"}

    import zipfile

    if zip_path.exists():
        try:
            zip_path.unlink()
        except Exception:
            pass
    count = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for path, arcname in entries:
            zipf.write(path, arcname)
            count += 1

    return {"ok": True, "zip_path": str(zip_path), "files": count}


def _write_support_packet_zip(
    *,
    out_zip_path: Path,
    evidence_path: Path,
    failures_path: Path,
    support_request_path: Path,
    artifacts_root: Path,
    artifact_files: list[Path],
) -> int:
    import zipfile

    out_zip_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with zipfile.ZipFile(out_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for path, prefix in [
            (evidence_path, "evidence"),
            (failures_path, "evidence"),
            (support_request_path, "evidence"),
        ]:
            arcname = str(Path(prefix) / path.name)
            zipf.write(path, arcname)
            count += 1

        for path in artifact_files:
            if path.name == ".gitkeep":
                continue
            try:
                rel = path.relative_to(artifacts_root)
            except ValueError:
                continue
            arcname = str(Path("artifacts") / rel)
            zipf.write(path, arcname)
            count += 1
    return count


def _parse_response_payload(response) -> tuple[dict | None, str | None]:
    payload = None
    text_head = None
    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        payload = None
    if payload is None:
        try:
            text_head = response.text
        except Exception:  # noqa: BLE001
            text_head = None
    if text_head:
        text_head = redact_text(text_head[:200])
    return payload, text_head


def _resolve_failure_artifacts_root() -> Path:
    override = os.environ.get("FAILURE_ARTIFACTS_ROOT")
    if override:
        return Path(override)
    return Path("collaboration") / "artifacts" / "shopee_api"


def _write_failure_artifact(
    *,
    root: Path,
    shop_key: str,
    target_date: date,
    call_name: str,
    api_path: str,
    method: str,
    query_keys: list[str],
    http_status: int | None,
    api_error: object | None,
    api_message: object | None,
    request_id: object | None,
    payload: dict | None,
    response_text_head: str | None,
    reason: str | None,
    safe_fingerprint: dict[str, object] | None = None,
    access_token_debug: dict[str, int] | None = None,
) -> Path:
    requested_at = datetime.now(timezone.utc)
    timestamp = int(requested_at.timestamp())
    safe_call = safe_name(call_name)
    safe_api = safe_path(api_path)
    ts_ms = int(requested_at.timestamp() * 1000)
    output_path = root / shop_key / target_date.isoformat() / f"{ts_ms}_{safe_call}_{safe_api}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    safe_fp = safe_fingerprint
    request_meta = {
        "method": method,
        "path": api_path,
        "query_keys": query_keys,
        "timestamp": timestamp,
        "requested_at": requested_at.isoformat(),
    }
    response_meta = {
        "http_status": http_status,
    }
    parsed_error = {
        "api_error": api_error,
        "api_message": api_message,
        "request_id": request_id,
        "reason": reason,
    }
    meta = {
        "shop_key": shop_key,
        "call_name": call_name,
    }
    extra_keys = {
        "partner_key",
        "access_token",
        "refresh_token",
        "sign",
        "authorization",
        "cookie",
        "secret",
        "client_secret",
    }
    meta = redact_secrets(meta, extra_keys=extra_keys)
    request_meta = redact_secrets(request_meta, extra_keys=extra_keys)
    if safe_fp:
        request_meta["safe_fingerprint"] = safe_fp
    if access_token_debug:
        request_meta["access_token_encoding"] = access_token_debug
    response_meta = redact_secrets(response_meta, extra_keys=extra_keys)
    parsed_error = redact_secrets(parsed_error, extra_keys=extra_keys)
    data: dict[str, object] = {
        "__meta": meta,
        "request_meta": request_meta,
        "response_meta": response_meta,
        "parsed_error": parsed_error,
    }
    if payload is not None:
        data["response"] = redact_secrets(
            payload,
            extra_keys=extra_keys,
        )
    if response_text_head:
        data["raw_body_snippet"] = redact_text(response_text_head[:2048])
    output_path.write_text(_dump_json(data, pretty=True), encoding="utf-8")
    return output_path


@ops_phase1_ads_endpoint_app.command("sweep")
def ops_phase1_ads_endpoint_sweep(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Alias for --shops"
    ),
    transport: str = typer.Option("live", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "passive", "--token-mode", help="default | passive"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    token_file_format: str = typer.Option(
        "auto", "--token-file-format", help="auto"
    ),
    token_import: bool = typer.Option(
        True, "--token-import/--no-token-import"
    ),
    baseline_shop_info: bool = typer.Option(
        True, "--baseline-shop-info/--no-baseline-shop-info"
    ),
    save_failure_artifacts: bool = typer.Option(
        False, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    candidates: str = typer.Option(
        "collaboration/endpoints/ads_candidates.yaml",
        "--candidates",
        help="Candidates YAML path",
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    out_md: str | None = typer.Option(
        None, "--out-md", help="Sweep markdown path"
    ),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
    auth_debug: bool = typer.Option(False, "--auth-debug"),
) -> None:
    _maybe_load_env_file(env_file)
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")

    token_mode_value = _normalize_token_mode(token_mode)
    if token_file_format.lower().strip() != "auto":
        raise typer.BadParameter("token-file-format must be auto")

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if transport_value == "live" and not (allow_network or allow_network_env):
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    candidates_path = Path(candidates)
    if not candidates_path.exists():
        raise typer.BadParameter(f"candidates file not found: {candidates_path}")
    import yaml

    data = yaml.safe_load(candidates_path.read_text(encoding="utf-8")) or {}
    daily_entries = _normalize_ads_candidate_entries(data.get("daily"))
    snapshot_entries = _normalize_ads_candidate_entries(data.get("snapshot"))
    legacy_entries = data.get("candidates") or []
    if isinstance(legacy_entries, list):
        for entry in legacy_entries:
            if not isinstance(entry, dict):
                continue
            daily_path = entry.get("ADS_DAILY_PATH") or entry.get("daily")
            if daily_path:
                daily_entries.append(
                    {
                        "path": str(daily_path),
                        "fixture": None,
                        "status": None,
                        "method": None,
                        "body": None,
                    }
                )
            snapshot_path = entry.get("ADS_SNAPSHOT_PATH") or entry.get("snapshot")
            if snapshot_path:
                snapshot_entries.append(
                    {
                        "path": str(snapshot_path),
                        "fixture": None,
                        "status": None,
                        "method": None,
                        "body": None,
                    }
                )

    daily_entries = _dedupe_ads_candidates(daily_entries)
    snapshot_entries = _dedupe_ads_candidates(snapshot_entries)
    if not daily_entries or not snapshot_entries:
        raise typer.BadParameter("candidates must include daily and snapshot entries")

    target_date = _parse_required_date(date_value)
    shops_value = only_shops or shops
    out_path = (
        Path(out_md)
        if out_md
        else Path("collaboration")
        / "results"
        / f"phase1_ads_endpoint_sweep_{target_date.isoformat()}.md"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    summary_lines: list[str] = []
    summary_lines.append("# Phase 1 Ads Endpoint Sweep")
    summary_lines.append("")
    summary_lines.append(f"date: {target_date.isoformat()}")
    summary_lines.append(f"shops: {shops_value}")
    summary_lines.append(f"transport: {transport_value}")
    summary_lines.append(f"candidates: {candidates_path}")
    summary_lines.append("")
    print(
        "ads_endpoint_sweep_start "
        f"shops={shops_value} date={target_date.isoformat()} "
        f"transport={transport_value} candidates={candidates_path}"
    )
    summary_lines.append(
        "ads_endpoint_sweep_start "
        f"shops={shops_value} date={target_date.isoformat()} "
        f"transport={transport_value} candidates={candidates_path}"
    )
    summary_lines.append("")

    if transport_value == "fixtures":
        fixtures_path = Path(fixtures_dir)
        if not fixtures_path.exists():
            raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    token_map: dict[str, dict] | None = None
    if token_file:
        token_path = Path(token_file)
        if token_path.exists():
            token_map = _extract_appsscript_token_map(_read_json(token_path))

    if token_file and token_import:
        import_exit, import_output = _run_capture_step(
            ops_phase1_token_appsscript_import,
            file=token_file,
            env_file=None,
            shops=shops_value,
        )
        if import_output:
            _print_captured_output(import_output)
            summary_lines.append(import_output.rstrip())
        if import_exit != 0:
            out_path.write_text("\n".join(summary_lines), encoding="utf-8")
            raise typer.Exit(code=1)

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops_value)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    if transport_value == "live":
        _require_shopee_settings(settings)

    failure_root = _resolve_failure_artifacts_root() if save_failure_artifacts else None
    failure_counts: dict[str, int] = {}
    failure_saved = 0

    def _bump_failure(shop_key: str) -> None:
        nonlocal failure_saved
        failure_saved += 1
        failure_counts[shop_key] = failure_counts.get(shop_key, 0) + 1

    results: dict[tuple[str, str], list[dict[str, object]]] = {}
    processed_shops: list[object] = []
    skipped_expired: list[str] = []
    skipped_missing: list[str] = []

    for shop_cfg in target_shops:
        shop_key = shop_cfg.shop_key
        shop_id = _resolve_shop_id(shop_cfg)
        token = None
        access_token_value: str | None = None
        if transport_value == "live":
            init_db()
            session = SessionLocal()
            try:
                token = get_token(session, shop_key)
                if token is None:
                    print(
                        f"token_missing shop={shop_key} shop_id={shop_id}"
                    )
                    skipped_missing.append(shop_key)
                    continue
                if token_mode_value == "passive" and needs_refresh(token.access_token_expires_at):
                    print(
                        "token_expired_refresh_disabled "
                        f"shop={shop_key} shop_id={shop_id}"
                    )
                    skipped_expired.append(shop_key)
                    continue
                if needs_refresh(token.access_token_expires_at):
                    refreshed = refresh_access_token(
                        _build_shopee_client(settings),
                        settings.shopee_partner_id,
                        settings.shopee_partner_key,
                        shop_cfg.shopee_shop_id,
                        token.refresh_token,
                        int(datetime.now().timestamp()),
                    )
                    upsert_token(
                        session,
                        shop_key,
                        refreshed.shop_id,
                        refreshed.access_token,
                        refreshed.refresh_token,
                        refreshed.access_expires_at,
                    )
                    session.commit()
                    token = get_token(session, shop_key)
            finally:
                session.close()
            access_token_value = token.access_token if token else None
        else:
            access_token_value = "fixture_access_token"
            if token_mode_value == "passive" and token_map:
                token_data = token_map.get(str(shop_id))
                access = token_data.get("access_token") if token_data else None
                expire_ts = _coerce_int(token_data.get("expire_timestamp")) if token_data else None
                if not access:
                    print(f"token_missing shop={shop_key} shop_id={shop_id}")
                    skipped_missing.append(shop_key)
                    continue
                expires_at = (
                    datetime.fromtimestamp(expire_ts, tz=timezone.utc)
                    if expire_ts
                    else None
                )
                if needs_refresh(expires_at):
                    print(
                        "token_expired_refresh_disabled "
                        f"shop={shop_key} shop_id={shop_id}"
                    )
                    skipped_expired.append(shop_key)
                    continue

        processed_shops.append(shop_cfg)
        client = _build_sweep_client(settings) if transport_value == "live" else None

        def run_request(
            *,
            kind: str,
            entry: dict[str, object | None],
            order: int,
            call_name: str,
            params: dict | None,
        ) -> dict[str, object]:
            candidate_path = str(entry.get("path") or "").strip()
            if not candidate_path:
                return {}
            method = str(entry.get("method") or "GET").upper()
            body = entry.get("body")
            timestamp = int(datetime.now().timestamp())
            http_status: int | None = None
            error_text: str | None = None
            payload: dict | None = None
            response_text_head: str | None = None
            ok = True
            query_keys: list[str] | None = None
            safe_fingerprint: dict[str, object] | None = None
            access_token_debug: dict[str, int] | None = None

            if transport_value == "fixtures":
                import httpx

                fixture_status = entry.get("status")
                fixture_name = entry.get("fixture")
                captured_query_keys: list[str] = []

                def handler(request: httpx.Request) -> httpx.Response:
                    nonlocal captured_query_keys
                    captured_query_keys = sorted(list(request.url.params.keys()))
                    status_code = int(fixture_status) if fixture_status is not None else 200
                    fixture_payload: dict | None = None
                    if fixture_name:
                        fixture_path = Path(fixtures_dir) / str(fixture_name)
                        if fixture_path.exists():
                            fixture_payload = _read_json(fixture_path)
                        else:
                            status_code = 404 if fixture_status is None else status_code
                    else:
                        fixture_payload = _load_probe_fixture_payload(
                            Path(fixtures_dir), call_name
                        )
                        if fixture_payload is None and fixture_status is None:
                            status_code = 404
                    return httpx.Response(
                        status_code=status_code,
                        json=fixture_payload or {},
                    )

                sweep_client = _build_sweep_client(
                    settings, transport=httpx.MockTransport(handler), allow_fallback=True
                )
                safe_fingerprint = _build_safe_fingerprint(
                    access_token=access_token_value,
                    partner_id=getattr(sweep_client, "partner_id", None),
                    partner_key=getattr(sweep_client, "partner_key", None),
                    path=candidate_path,
                    timestamp=timestamp,
                    shop_id=shop_cfg.shopee_shop_id,
                )
                access_token_debug = _build_access_token_encoding_flags(
                    access_token_value
                )
                try:
                    payload = sweep_client.request(
                        method,
                        candidate_path,
                        shop_id=shop_cfg.shopee_shop_id,
                        access_token=access_token_value,
                        params=params,
                        json=body if isinstance(body, dict) else None,
                        timestamp=timestamp,
                    )
                    http_status = 200
                except httpx.HTTPStatusError as exc:
                    ok = False
                    http_status = exc.response.status_code
                    payload, response_text_head = _parse_response_payload(exc.response)
                    error_text = f"HTTP {http_status}"
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
                finally:
                    sweep_client.close()
                query_keys = captured_query_keys or _build_query_keys(
                    params, shop_id=shop_cfg.shopee_shop_id, access_token=access_token_value
                )
            else:
                try:
                    _validate_outgoing_access_token(
                        access_token=access_token_value,
                        shop_key=shop_key,
                        path=candidate_path,
                    )
                    safe_fingerprint = _build_safe_fingerprint(
                        access_token=access_token_value,
                        partner_id=getattr(client, "partner_id", None),
                        partner_key=getattr(client, "partner_key", None),
                        path=candidate_path,
                        timestamp=timestamp,
                        shop_id=shop_cfg.shopee_shop_id,
                    )
                    access_token_debug = _build_access_token_encoding_flags(
                        access_token_value
                    )
                    payload = client.request(
                        method,
                        candidate_path,
                        shop_id=shop_cfg.shopee_shop_id,
                        access_token=access_token_value,
                        params=params,
                        json=body if isinstance(body, dict) else None,
                        timestamp=timestamp,
                    )
                    http_status = 200
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    if hasattr(exc, "response") and getattr(exc, "response") is not None:
                        http_status = getattr(exc.response, "status_code", None)
                        payload, response_text_head = _parse_response_payload(exc.response)
                    error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"
                query_keys = _build_query_keys(
                    params, shop_id=shop_cfg.shopee_shop_id, access_token=access_token_value
                )

            error_code = None
            if isinstance(payload, dict):
                error_code = payload.get("error")
                if error_code == "":
                    error_code = None
            if ok and isinstance(payload, dict):
                if error_code not in (None, 0, "0"):
                    ok = False
                    message = payload.get("message") or payload.get("msg") or "-"
                    error_text = f"shopee_error_{error_code}: {message}"

            record_count = _extract_record_count(payload)
            error_text = _scrub_sensitive_text(error_text) if error_text else None
            api_error, api_message, request_id, warning, debug_msg = _extract_api_fields(
                payload
            )
            if api_message is None and response_text_head:
                api_message = response_text_head
            reachable, reason = _classify_reachability(http_status, ok, error_code)
            return {
                "shop": shop_key,
                "kind": kind,
                "candidate": candidate_path,
                "call_name": call_name,
                "method": method,
                "ok": ok,
                "reachable": reachable,
                "reason": reason,
                "http": http_status,
                "error": error_text,
                "records": record_count,
                "order": order,
                "api_error": api_error,
                "api_message": api_message,
                "request_id": request_id,
                "warning": warning,
                "debug_msg": debug_msg,
                "response_payload": payload,
                "response_text_head": response_text_head,
                "query_keys": query_keys,
                "safe_fingerprint": safe_fingerprint,
                "access_token_debug": access_token_debug,
            }

        if baseline_shop_info:
            if transport_value == "live":
                token_len = len(access_token_value) if access_token_value else 0
                token_sha8 = _sha256_8(access_token_value)
                line = (
                    f"token_source=db shop={shop_key} "
                    f"token_len={token_len} token_sha8={token_sha8}"
                )
                print(line)
                summary_lines.append(line)
            baseline_entry = {"path": "/api/v2/shop/get_shop_info"}
            baseline_result = run_request(
                kind="baseline",
                entry=baseline_entry,
                order=-1,
                call_name="shop_info",
                params=None,
            )
            http_text = (
                str(baseline_result.get("http"))
                if baseline_result.get("http") is not None
                else "-"
            )
            api_error_text = _format_api_value(baseline_result.get("api_error"))
            api_message_text = _format_api_value(baseline_result.get("api_message"))
            request_id_text = _format_api_value(baseline_result.get("request_id"))
            line = (
                f"baseline_shop_info shop={shop_key} http={http_text} "
                f"api_error={api_error_text} api_message={api_message_text} "
                f"request_id={request_id_text}"
            )
            if auth_debug:
                debug_text = _format_auth_debug(baseline_result.get("safe_fingerprint"))
                if debug_text:
                    line = f"{line} {debug_text}"
            print(line)
            summary_lines.append(line)
            if save_failure_artifacts and (
                baseline_result.get("http") != 200 or not baseline_result.get("ok")
            ):
                _write_failure_artifact(
                    root=failure_root or _resolve_failure_artifacts_root(),
                    shop_key=shop_key,
                    target_date=target_date,
                    call_name="baseline_shop_info",
                    api_path="/api/v2/shop/get_shop_info",
                    method=str(baseline_result.get("method") or "GET"),
                    query_keys=baseline_result.get("query_keys") or [],
                    http_status=baseline_result.get("http"),
                    api_error=baseline_result.get("api_error"),
                    api_message=baseline_result.get("api_message"),
                    request_id=baseline_result.get("request_id"),
                    payload=baseline_result.get("response_payload"),
                    response_text_head=baseline_result.get("response_text_head"),
                    reason=str(baseline_result.get("reason") or ""),
                    safe_fingerprint=baseline_result.get("safe_fingerprint"),
                    access_token_debug=baseline_result.get("access_token_debug"),
                )
                _bump_failure(shop_key)

        def _ads_daily_date(fmt: str) -> str:
            if fmt == "iso":
                return target_date.isoformat()
            if fmt == "dmy":
                return target_date.strftime("%d-%m-%Y")
            raise ValueError(f"unknown ads_daily date format: {fmt}")

        def _ads_daily_params(mode: str, fmt: str) -> dict[str, str]:
            date_str = _ads_daily_date(fmt)
            if mode == "range":
                return {"start_date": date_str, "end_date": date_str}
            if mode == "date":
                return {"date": date_str}
            raise ValueError(f"unknown ads_daily params mode: {mode}")

        def _ads_daily_retry_action(
            result: dict[str, object],
            attempted_mode: str,
            attempted_format: str,
        ) -> tuple[str, str] | None:
            api_error = str(result.get("api_error") or "").strip()
            if api_error != "error_param":
                return None
            msg = str(result.get("api_message") or "").lower()
            required_markers = ("required", "require", "missing", "invalid")
            if not any(marker in msg for marker in required_markers):
                return None

            # Format hints first: "DD-MM-YYYY format" etc.
            if attempted_format == "iso" and "dd-mm-yyyy" in msg:
                return attempted_mode, "dmy"
            if attempted_format == "dmy" and "yyyy-mm-dd" in msg:
                return attempted_mode, "iso"

            if attempted_mode == "range":
                if re.search(r"\bdate\b", msg):
                    return "date", attempted_format
                return None
            if attempted_mode == "date":
                if re.search(r"\bend_date\b", msg) or re.search(r"\bstart_date\b", msg):
                    return "range", attempted_format
                return None
            return None

        def run_candidate(kind: str, entry: dict[str, object | None], order: int) -> None:
            call_name = "ads_daily" if kind == "daily" else "ads_snapshot"
            params = None
            if kind == "daily":
                mode = "range"
                fmt = "dmy"
                params = _ads_daily_params(mode, fmt)

            result = run_request(
                kind=kind,
                entry=entry,
                order=order,
                call_name=call_name,
                params=params,
            )
            if kind == "daily" and result:
                for _ in range(2):
                    action = _ads_daily_retry_action(result, mode, fmt)
                    if not action:
                        break
                    next_mode, next_fmt = action
                    if next_mode == mode and next_fmt == fmt:
                        break
                    mode = next_mode
                    fmt = next_fmt
                    params = _ads_daily_params(mode, fmt)
                    result = run_request(
                        kind=kind,
                        entry=entry,
                        order=order,
                        call_name=call_name,
                        params=params,
                    )
            if not result:
                return
            results.setdefault((shop_key, kind), []).append(result)
            http_text = str(result.get("http")) if result.get("http") is not None else "-"
            error_text_out = result.get("error") or "-"
            records_text = (
                str(result.get("records")) if result.get("records") is not None else "-"
            )
            query_text = ",".join(result.get("query_keys") or [])
            api_error_text = _format_api_value(result.get("api_error"))
            api_message_text = _format_api_value(result.get("api_message"))
            request_id_text = _format_api_value(result.get("request_id"))
            warning_text = _format_api_value(result.get("warning"))
            debug_text = _format_api_value(result.get("debug_msg"))
            line = (
                f"shop={shop_key} kind={kind} candidate={result.get('candidate')} "
                f"method={result.get('method')} ok={1 if result.get('ok') else 0} "
                f"reachable={1 if result.get('reachable') else 0} http={http_text} "
                f"reason={result.get('reason')} error={error_text_out} "
                f"api_error={api_error_text} api_message={api_message_text} "
                f"request_id={request_id_text} warning={warning_text} debug_msg={debug_text} "
                f"records={records_text} query_keys={query_text}"
            )
            if auth_debug:
                debug_text = _format_auth_debug(result.get("safe_fingerprint"))
                if debug_text:
                    line = f"{line} {debug_text}"
            print(line)
            summary_lines.append(line)
            if save_failure_artifacts and (result.get("http") != 200 or not result.get("ok")):
                _write_failure_artifact(
                    root=failure_root or _resolve_failure_artifacts_root(),
                    shop_key=shop_key,
                    target_date=target_date,
                    call_name=str(result.get("call_name") or kind),
                    api_path=str(result.get("candidate") or ""),
                    method=str(result.get("method") or "GET"),
                    query_keys=result.get("query_keys") or [],
                    http_status=result.get("http"),
                    api_error=result.get("api_error"),
                    api_message=result.get("api_message"),
                    request_id=result.get("request_id"),
                    payload=result.get("response_payload"),
                    response_text_head=result.get("response_text_head"),
                    reason=str(result.get("reason") or ""),
                    safe_fingerprint=result.get("safe_fingerprint"),
                    access_token_debug=result.get("access_token_debug"),
                )
                _bump_failure(shop_key)

        for idx, entry in enumerate(daily_entries):
            run_candidate("daily", entry, idx)
        for idx, entry in enumerate(snapshot_entries):
            run_candidate("snapshot", entry, idx)

        if client is not None:
            client.close()

    if skipped_expired:
        line = f"sweep_skipped_shops expired_access={','.join(skipped_expired)}"
        print(line)
        summary_lines.append(line)
    if skipped_missing:
        line = f"sweep_skipped_shops missing_token={','.join(skipped_missing)}"
        print(line)
        summary_lines.append(line)

    if not processed_shops:
        line = "sweep_ok=0"
        print(line)
        summary_lines.append(line)
        out_path.write_text("\n".join(summary_lines), encoding="utf-8")
        print(f"sweep_md path={out_path}")
        raise typer.Exit(code=2)

    recommendations: dict[str, dict[str, str | None]] = {}
    for shop_cfg in processed_shops:
        shop_key = shop_cfg.shop_key
        recommendations[shop_key] = {
            "daily": _select_best_candidate(results.get((shop_key, "daily"), [])),
            "snapshot": _select_best_candidate(results.get((shop_key, "snapshot"), [])),
        }

    daily_values = {value["daily"] for value in recommendations.values() if value.get("daily")}
    snapshot_values = {value["snapshot"] for value in recommendations.values() if value.get("snapshot")}

    recommended_daily = None
    recommended_snapshot = None
    if daily_values and len(daily_values) == 1 and all(
        value.get("daily") for value in recommendations.values()
    ):
        recommended_daily = next(iter(daily_values))
    if snapshot_values and len(snapshot_values) == 1 and all(
        value.get("snapshot") for value in recommendations.values()
    ):
        recommended_snapshot = next(iter(snapshot_values))

    if recommended_daily:
        line = f"recommended_ads_daily_path={recommended_daily}"
        print(line)
        summary_lines.append(line)
    if recommended_snapshot:
        line = f"recommended_ads_snapshot_path={recommended_snapshot}"
        print(line)
        summary_lines.append(line)

    for shop_cfg in processed_shops:
        shop_key = shop_cfg.shop_key
        daily_rec = recommendations.get(shop_key, {}).get("daily")
        snapshot_rec = recommendations.get(shop_key, {}).get("snapshot")
        if daily_rec and daily_rec != recommended_daily:
            line = f"recommended_ads_daily_path_shop={shop_key} {daily_rec}"
            print(line)
            summary_lines.append(line)
        if snapshot_rec and snapshot_rec != recommended_snapshot:
            line = f"recommended_ads_snapshot_path_shop={shop_key} {snapshot_rec}"
            print(line)
            summary_lines.append(line)

    if save_failure_artifacts:
        root_text = str(failure_root or _resolve_failure_artifacts_root())
        line = f"failure_artifacts_dir={root_text}"
        print(line)
        summary_lines.append(line)
        line = f"failure_artifacts_saved={failure_saved}"
        print(line)
        summary_lines.append(line)
        for shop_cfg in processed_shops:
            shop_key = shop_cfg.shop_key
            line = (
                f"shop={shop_key} failure_artifacts_saved="
                f"{failure_counts.get(shop_key, 0)}"
            )
            print(line)
            summary_lines.append(line)

    out_path.write_text("\n".join(summary_lines), encoding="utf-8")
    print(f"sweep_md path={out_path}")

    print("sweep_ok=1")
    if not recommended_daily or not recommended_snapshot:
        raise typer.Exit(code=1)


@ops_phase1_artifacts_app.command("summarize-failures")
def ops_phase1_artifacts_summarize_failures(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    out: str | None = typer.Option(
        None, "--out", help="Output markdown path"
    ),
    only_prefix: str | None = typer.Option(
        None, "--only-prefix", help="Optional subfolder prefix filter"
    ),
    since_ms: int | None = typer.Option(
        None, "--since-ms", help="Minimum artifact prefix timestamp (ms)"
    ),
    until_ms: int | None = typer.Option(
        None, "--until-ms", help="Maximum artifact prefix timestamp (ms)"
    ),
) -> None:
    since_ms = _coerce_option_value(since_ms, None)
    until_ms = _coerce_option_value(until_ms, None)
    target_date = _parse_required_date(date_value)
    shops_list = _parse_only_shops(shops) or []
    root = Path(artifacts_root)
    out_path = (
        Path(out)
        if out
        else Path("collaboration")
        / "results"
        / f"phase1_failures_{target_date.isoformat()}.md"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    filter_active = since_ms is not None or until_ms is not None
    matched = 0
    skipped = 0
    records: list[dict[str, object]] = []
    for shop_key in shops_list:
        shop_root = root / shop_key / target_date.isoformat()
        if only_prefix:
            prefix_path = shop_root / only_prefix
            if prefix_path.exists():
                search_root = prefix_path
            else:
                search_root = shop_root
        else:
            search_root = shop_root
        if not search_root.exists():
            continue
        for artifact_path in search_root.rglob("*.json"):
            if filter_active:
                prefix_ms = _parse_artifact_prefix_ms(artifact_path)
                if prefix_ms is None:
                    skipped += 1
                    continue
                if since_ms is not None and prefix_ms < since_ms:
                    skipped += 1
                    continue
                if until_ms is not None and prefix_ms > until_ms:
                    skipped += 1
                    continue
                matched += 1
            try:
                payload = _read_json(artifact_path)
            except Exception:  # noqa: BLE001
                continue
            record = _extract_failure_summary(
                payload, shop_fallback=shop_key, file_path=artifact_path
            )
            if record:
                records.append(record)

    baseline_http: dict[str, int | None] = {}
    for record in records:
        if _is_baseline_record(record):
            shop_key = str(record.get("shop") or "")
            http_status = record.get("http")
            if shop_key and (
                shop_key not in baseline_http
                or http_status == 200
            ):
                baseline_http[shop_key] = _coerce_int(http_status)

    lines: list[str] = []
    lines.append("# Phase1 Failure Artifact Summary")
    lines.append("")
    lines.append(f"date: {target_date.isoformat()}")
    lines.append(f"shops: {','.join(shops_list)}")
    lines.append(f"artifacts_root: {root}")
    if filter_active:
        lines.append(
            "artifact_filter "
            f"since_ms={since_ms if since_ms is not None else '-'} "
            f"until_ms={until_ms if until_ms is not None else '-'} "
            f"matched={matched} skipped={skipped}"
        )
    if only_prefix:
        lines.append(f"only_prefix: {only_prefix}")
    lines.append("")
    lines.append(
        "| shop | call_name | path | http | api_error | api_message | request_id | hint |"
    )
    lines.append("|---|---|---|---:|---:|---|---|---|")

    for record in sorted(
        records,
        key=lambda item: (str(item.get("shop") or ""), str(item.get("call_name") or "")),
    ):
        shop_key = str(record.get("shop") or "-")
        call_name = _format_summary_cell(record.get("call_name"))
        path = _format_summary_cell(record.get("path"))
        http = _format_summary_cell(record.get("http"))
        api_error = _format_summary_cell(record.get("api_error"))
        api_message = _format_summary_cell(record.get("api_message"))
        request_id = _format_summary_cell(record.get("request_id"))
        hint = _derive_failure_hint(
            record, baseline_http.get(shop_key)
        )
        lines.append(
            f"| {shop_key} | {call_name} | {path} | {http} | "
            f"{api_error} | {api_message} | {request_id} | {hint} |"
        )

    out_path.write_text("\n".join(lines), encoding="utf-8")
    if filter_active:
        print(
            "artifact_filter "
            f"since_ms={since_ms if since_ms is not None else '-'} "
            f"until_ms={until_ms if until_ms is not None else '-'} "
            f"matched={matched} skipped={skipped}"
        )
    print(f"summarize_failures_saved={out_path} records={len(records)}")


@ops_phase1_evidence_app.command("run")
def ops_phase1_evidence_run(
    env_file: str = typer.Option(..., "--env-file", help="Env file path"),
    token_file: str = typer.Option(..., "--token-file", help="Apps Script export JSON path"),
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    candidates: str = typer.Option(
        "collaboration/endpoints/ads_candidates.yaml",
        "--candidates",
        help="Candidates YAML path",
    ),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "passive", "--token-mode", help="default | passive"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    baseline_shop_info: bool = typer.Option(
        True, "--baseline-shop-info/--no-baseline-shop-info"
    ),
    save_failure_artifacts: bool = typer.Option(
        True, "--save-failure-artifacts/--no-save-failure-artifacts"
    ),
    auth_debug: bool = typer.Option(False, "--auth-debug"),
    min_access_ttl_sec: int = typer.Option(
        600, "--min-access-ttl-sec", help="Minimum access token TTL in seconds"
    ),
    token_sync: bool = typer.Option(
        True, "--token-sync/--no-token-sync", help="Sync tokens from file into DB"
    ),
    allow_unknown_expiry: bool = typer.Option(
        False, "--allow-unknown-expiry", help="Allow unknown access expiry"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    evidence_out: str | None = typer.Option(
        None, "--evidence-out", help="Evidence report markdown path"
    ),
    out: str | None = typer.Option(
        None, "--out", help="Output markdown path"
    ),
    support_packet: bool = typer.Option(False, "--support-packet"),
    support_zip: str | None = typer.Option(
        None, "--support-zip", help="Support packet zip path"
    ),
    support_md: str | None = typer.Option(
        None, "--support-md", help="Support request markdown path"
    ),
    support_max_request_ids: int = typer.Option(
        50, "--support-max-request-ids", help="Max request_id entries"
    ),
    support_no_scan: bool = typer.Option(False, "--support-no-scan"),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    skip_sweep: bool = typer.Option(False, "--skip-sweep"),
    skip_preview: bool = typer.Option(True, "--skip-preview"),
    no_preview: bool = typer.Option(False, "--no-preview"),
) -> None:
    evidence_out = _coerce_option_value(evidence_out, None)
    out = _coerce_option_value(out, None)
    support_packet = _coerce_option_value(support_packet, False)
    support_zip = _coerce_option_value(support_zip, None)
    support_md = _coerce_option_value(support_md, None)
    support_max_request_ids = _coerce_option_value(support_max_request_ids, 50)
    support_no_scan = _coerce_option_value(support_no_scan, False)
    send_discord = _coerce_option_value(send_discord, False)
    allow_network = _coerce_option_value(allow_network, False)
    skip_sweep = _coerce_option_value(skip_sweep, False)
    skip_preview = _coerce_option_value(skip_preview, True)
    no_preview = _coerce_option_value(no_preview, False)
    min_access_ttl_sec = _coerce_option_value(min_access_ttl_sec, 600)
    token_sync = _coerce_option_value(token_sync, True)
    allow_unknown_expiry = _coerce_option_value(allow_unknown_expiry, False)
    baseline_shop_info = _coerce_option_value(baseline_shop_info, True)
    save_failure_artifacts = _coerce_option_value(save_failure_artifacts, True)
    auth_debug = _coerce_option_value(auth_debug, False)

    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    token_mode = _coerce_option_value(token_mode, "passive")
    token_mode_value = _normalize_token_mode(token_mode)
    target_date = _parse_required_date(date_value)
    shop_list = _parse_only_shops(shops) or []
    _maybe_load_env_file(env_file)
    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = allow_network or allow_network_env
    if no_preview:
        skip_preview = True
    run_started_ms = int(time_module.time() * 1000)
    print(f"run_started_ms={run_started_ms}")

    if transport_value == "live" and not allow_network_effective:
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)
    evidence_out_path = (
        Path(evidence_out)
        if evidence_out
        else Path("collaboration")
        / "results"
        / f"phase1_evidence_{target_date.isoformat()}.md"
    )
    support_zip_path = (
        Path(support_zip)
        if support_zip
        else Path("collaboration")
        / "results"
        / f"phase1_support_packet_{target_date.isoformat()}.zip"
    )
    support_md_path = (
        Path(support_md)
        if support_md
        else Path("collaboration")
        / "results"
        / f"phase1_support_request_{target_date.isoformat()}.md"
    )
    evidence_out_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        "evidence_start "
        f"date={target_date.isoformat()} shops={shops} "
        f"transport={transport_value} token_mode={token_mode_value}"
    )
    if skip_preview:
        print("preview_skipped reason=flag")
    else:
        print("preview_skipped reason=not_implemented")

    pre_exit, pre_output = _run_capture_step(
        ops_phase1_token_appsscript_preflight,
        file=token_file,
        env_file=None,
        shops=shops,
        min_access_ttl_sec=min_access_ttl_sec,
        allow_unknown_expiry=allow_unknown_expiry,
    )
    preflight_info = _parse_preflight_output(pre_output)
    if pre_output:
        _print_captured_output(pre_output)
    if pre_exit != 0:
        _write_evidence_report(
            evidence_out_path,
            header={
                "date": target_date.isoformat(),
                "shops": shops,
                "transport": transport_value,
                "token_mode": token_mode_value,
                "min_access_ttl_sec": str(min_access_ttl_sec),
                "allow_network": "1" if allow_network_effective else "0",
                "skip_sweep": "1" if skip_sweep else "0",
                "skip_preview": "1" if skip_preview else "0",
            },
            preflight=preflight_info,
            sweep_status={
                "status": "skipped",
                "reason": "preflight_failed",
            },
            summary_status=None,
            verdicts=_build_verdict_entries(
                [],
                shop_list,
                force_skipped=True,
                preflight_rows=preflight_info.get("rows") if preflight_info else None,
            ),
        )
        print(f"evidence_report_saved={evidence_out_path}")
        if "preflight_ok=0" in pre_output or pre_exit == 2:
            print("evidence_ok=0 reason=token_expired_or_short_ttl")
            print(
                "next_steps: run Apps Script diag_TOKEN(...) then "
                "exportShopeeTokensToDrive_Normalized()"
            )
            raise typer.Exit(code=2)
        print("evidence_ok=0 reason=preflight_failed")
        raise typer.Exit(code=1)

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    if token_file and token_sync:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        _print_db_token_fingerprints(target_shops)

    sweep_ok = True
    sweep_output = ""
    if not skip_sweep:
        prev_failure_root = os.environ.get("FAILURE_ARTIFACTS_ROOT")
        os.environ["FAILURE_ARTIFACTS_ROOT"] = artifacts_root
        try:
            sweep_exit, sweep_output = _run_capture_step(
                ops_phase1_ads_endpoint_sweep,
                date_value=target_date.isoformat(),
                shops=shops,
                only_shops=None,
                transport=transport_value,
                token_mode=token_mode_value,
                allow_network=allow_network_effective,
                env_file=None,
                token_file=token_file,
                token_file_format="auto",
                token_import=not token_sync,
                candidates=candidates,
                fixtures_dir=fixtures_dir,
                out_md=None,
                send_discord=False,
                baseline_shop_info=baseline_shop_info,
                save_failure_artifacts=save_failure_artifacts,
                auth_debug=auth_debug,
            )
        finally:
            if prev_failure_root is None:
                os.environ.pop("FAILURE_ARTIFACTS_ROOT", None)
            else:
                os.environ["FAILURE_ARTIFACTS_ROOT"] = prev_failure_root
        if sweep_output:
            _print_captured_output(sweep_output)
        # Sweep may exit non-zero for soft issues (e.g. per-shop recommendation mismatch),
        # while still producing valid artifacts/output. Prefer the explicit sweep_ok marker.
        sweep_ok = (sweep_exit == 0) or ("sweep_ok=1" in (sweep_output or ""))
    else:
        print("sweep_skipped reason=flag")

    out_path = (
        out
        if out is not None
        else str(
            Path("collaboration")
            / "results"
            / f"phase1_failures_{target_date.isoformat()}.md"
        )
    )
    print(f"summarize_filter since_ms={run_started_ms}")
    summarize_exit, summarize_output = _run_capture_step(
        ops_phase1_artifacts_summarize_failures,
        date_value=target_date.isoformat(),
        shops=shops,
        artifacts_root=artifacts_root,
        out=out_path,
        only_prefix=None,
        since_ms=run_started_ms,
    )
    if summarize_output:
        _print_captured_output(summarize_output)
    if summarize_exit != 0:
        _write_evidence_report(
            evidence_out_path,
            header={
                "date": target_date.isoformat(),
                "shops": shops,
                "transport": transport_value,
                "token_mode": token_mode_value,
                "min_access_ttl_sec": str(min_access_ttl_sec),
                "allow_network": "1" if allow_network_effective else "0",
                "skip_sweep": "1" if skip_sweep else "0",
                "skip_preview": "1" if skip_preview else "0",
            },
            preflight=preflight_info,
            sweep_status=_parse_sweep_output(sweep_output, skip_sweep),
            summary_status=None,
            verdicts=_build_verdict_entries(
                [],
                shop_list,
                force_skipped=True,
                preflight_rows=preflight_info.get("rows") if preflight_info else None,
            ),
        )
        print(f"evidence_report_saved={evidence_out_path}")
        print("evidence_ok=0 reason=summarize_failed")
        raise typer.Exit(code=1)

    summary_path = Path(out_path)
    if not summary_path.exists():
        _write_evidence_report(
            evidence_out_path,
            header={
                "date": target_date.isoformat(),
                "shops": shops,
                "transport": transport_value,
                "token_mode": token_mode_value,
                "min_access_ttl_sec": str(min_access_ttl_sec),
                "allow_network": "1" if allow_network_effective else "0",
                "skip_sweep": "1" if skip_sweep else "0",
                "skip_preview": "1" if skip_preview else "0",
            },
            preflight=preflight_info,
            sweep_status=_parse_sweep_output(sweep_output, skip_sweep),
            summary_status=None,
            verdicts=_build_verdict_entries(
                [],
                shop_list,
                force_skipped=True,
                preflight_rows=preflight_info.get("rows") if preflight_info else None,
            ),
        )
        print(f"evidence_report_saved={evidence_out_path}")
        print("evidence_ok=0 reason=summary_missing")
        raise typer.Exit(code=1)

    records = _parse_failure_summary_markdown(summary_path)
    verdict_entries = _build_verdict_entries(
        records,
        shop_list,
        preflight_rows=preflight_info.get("rows") if preflight_info else None,
    )
    for entry in verdict_entries:
        baseline_text = entry.get("baseline_http") or "-"
        ads_text = entry.get("ads_http") or "-"
        print(
            f"shop={entry.get('shop')} verdict={entry.get('verdict')} "
            f"baseline_http={baseline_text} ads_http={ads_text}"
        )

    summarize_status = _parse_summarize_output(summarize_output, summary_path)
    _write_evidence_report(
        evidence_out_path,
        header={
            "date": target_date.isoformat(),
            "shops": shops,
            "transport": transport_value,
            "token_mode": token_mode_value,
            "min_access_ttl_sec": str(min_access_ttl_sec),
            "allow_network": "1" if allow_network_effective else "0",
            "skip_sweep": "1" if skip_sweep else "0",
            "skip_preview": "1" if skip_preview else "0",
        },
        preflight=preflight_info,
        sweep_status=_parse_sweep_output(sweep_output, skip_sweep),
        summary_status=summarize_status,
        verdicts=verdict_entries,
    )
    print(f"evidence_report_saved={evidence_out_path}")

    if not skip_sweep and not sweep_ok:
        print("evidence_ok=0 reason=sweep_failed")
        raise typer.Exit(code=1)

    if support_packet:
        support_exit, support_output = _run_capture_step(
            ops_phase1_evidence_support_packet,
            date_value=target_date.isoformat(),
            shops=shops,
            artifacts_root=artifacts_root,
            evidence_file=str(evidence_out_path),
            failures_file=str(summary_path),
            out_zip=str(support_zip_path),
            out_md=str(support_md_path),
            max_request_ids=support_max_request_ids,
            no_scan=support_no_scan,
        )
        if support_output:
            _print_captured_output(support_output)
        if support_exit != 0:
            print("evidence_ok=0 reason=support_packet_failed")
            raise typer.Exit(code=1)

    print("evidence_ok=1")


@ops_phase1_evidence_app.command("support-packet")
def ops_phase1_evidence_support_packet(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    artifacts_root: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-root",
        help="Artifacts root directory",
    ),
    evidence_file: str | None = typer.Option(
        None, "--evidence-file", help="Evidence report markdown path"
    ),
    failures_file: str | None = typer.Option(
        None, "--failures-file", help="Failures summary markdown path"
    ),
    out_zip: str | None = typer.Option(
        None, "--out-zip", help="Output zip path"
    ),
    out_md: str | None = typer.Option(
        None, "--out-md", help="Support request markdown path"
    ),
    max_request_ids: int = typer.Option(
        50, "--max-request-ids", help="Max request_id entries"
    ),
    no_scan: bool = typer.Option(False, "--no-scan"),
) -> None:
    target_date = _parse_required_date(date_value)
    shop_list = _parse_only_shops(shops) or []
    root = Path(artifacts_root)
    evidence_path = (
        Path(evidence_file)
        if evidence_file
        else Path("collaboration")
        / "results"
        / f"phase1_evidence_{target_date.isoformat()}.md"
    )
    failures_path = (
        Path(failures_file)
        if failures_file
        else Path("collaboration")
        / "results"
        / f"phase1_failures_{target_date.isoformat()}.md"
    )
    out_zip_path = (
        Path(out_zip)
        if out_zip
        else Path("collaboration")
        / "results"
        / f"phase1_support_packet_{target_date.isoformat()}.zip"
    )
    out_md_path = (
        Path(out_md)
        if out_md
        else Path("collaboration")
        / "results"
        / f"phase1_support_request_{target_date.isoformat()}.md"
    )

    if not evidence_path.exists():
        print(f"support_packet_ok=0 error=missing_input_file file={evidence_path}")
        raise typer.Exit(code=1)
    if not failures_path.exists():
        print(f"support_packet_ok=0 error=missing_input_file file={failures_path}")
        raise typer.Exit(code=1)

    print(
        "support_packet_start "
        f"date={target_date.isoformat()} shops={','.join(shop_list)}"
    )
    print(
        "support_packet_inputs "
        f"evidence_file={evidence_path} failures_file={failures_path} "
        f"artifacts_root={root}"
    )

    artifact_files = _collect_artifact_files(root, shop_list, target_date)
    failure_records = _parse_failure_summary_markdown(failures_path)
    support_request_lines = _build_support_request_template(
        target_date=target_date,
        shops=shop_list,
        failure_records=failure_records,
        max_request_ids=max_request_ids,
    )
    out_md_path.parent.mkdir(parents=True, exist_ok=True)
    out_md_path.write_text("\n".join(support_request_lines), encoding="utf-8")

    files_to_scan: list[Path] = [evidence_path, failures_path, out_md_path]
    files_to_scan.extend(artifact_files)
    if not no_scan:
        leak_path = _scan_files_for_secrets(files_to_scan)
        if leak_path:
            if out_zip_path.exists():
                try:
                    out_zip_path.unlink()
                except Exception:
                    pass
            print(
                "support_packet_ok=0 error=secret_leak_detected "
                f"file={leak_path}"
            )
            raise typer.Exit(code=1)
        print(f"support_packet_scan_ok=1 files_scanned={len(files_to_scan)}")
    else:
        print("support_packet_scan_ok=0 files_scanned=0 reason=no_scan")

    zip_count = _write_support_packet_zip(
        out_zip_path=out_zip_path,
        evidence_path=evidence_path,
        failures_path=failures_path,
        support_request_path=out_md_path,
        artifacts_root=root,
        artifact_files=artifact_files,
    )

    print(f"support_request_saved={out_md_path}")
    print(f"support_packet_saved={out_zip_path} files={zip_count}")
    print("support_packet_ok=1")


@ops_phase1_env_app.command("patch-ads-endpoints")
def ops_phase1_env_patch_ads_endpoints(
    env_file: str = typer.Option(..., "--env-file", help="Env file path"),
    ads_daily_path: str = typer.Option(..., "--ads-daily-path", help="Daily endpoint path"),
    ads_snapshot_path: str = typer.Option(
        ..., "--ads-snapshot-path", help="Snapshot endpoint path"
    ),
    backup: bool = typer.Option(True, "--backup/--no-backup"),
) -> None:
    env_path = Path(env_file)
    if not env_path.exists():
        raise typer.BadParameter(f"env file not found: {env_path}")
    updates = {
        "ADS_DAILY_PATH": ads_daily_path.strip(),
        "ADS_SNAPSHOT_PATH": ads_snapshot_path.strip(),
    }
    if not updates["ADS_DAILY_PATH"] or not updates["ADS_SNAPSHOT_PATH"]:
        raise typer.BadParameter("ads paths must be non-empty")

    if backup:
        backup_path = Path(str(env_path) + ".bak")
        if not backup_path.exists():
            backup_path.write_text(env_path.read_text(encoding="utf-8"), encoding="utf-8")

    lines = env_path.read_text(encoding="utf-8").splitlines()
    updated: set[str] = set()
    new_lines: list[str] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in raw_line:
            new_lines.append(raw_line)
            continue
        key, _value = raw_line.split("=", 1)
        key = key.strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            updated.add(key)
        else:
            new_lines.append(raw_line)

    for key, value in updates.items():
        if key not in updated:
            new_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(new_lines), encoding="utf-8")
    print(f"ADS_DAILY_PATH={updates['ADS_DAILY_PATH']}")
    print(f"ADS_SNAPSHOT_PATH={updates['ADS_SNAPSHOT_PATH']}")


@ops_phase1_ads_app.command("probe")
def ops_phase1_ads_probe(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Alias for --shops"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    token_mode: str = typer.Option(
        "default", "--token-mode", help="default | passive"
    ),
    allow_network: bool = typer.Option(False, "--allow-network"),
    plan: str = typer.Option(
        "collaboration/plans/ads_probe_phase1.yaml", "--plan", help="Plan YAML path"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-dir",
        help="Artifacts root directory",
    ),
    analyze: bool = typer.Option(True, "--analyze/--no-analyze"),
    analysis_dir: str = typer.Option(
        "collaboration/probes", "--analysis-dir", help="Analysis output directory"
    ),
    send_discord: bool = typer.Option(
        False, "--send-discord/--no-send-discord"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    target_date = _parse_required_date(date_value)
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    token_mode_value = _normalize_token_mode(token_mode)

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if transport_value == "live" and not (allow_network or allow_network_env):
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    fixtures_path = Path(fixtures_dir)
    if transport_value == "fixtures" and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    shops_value = only_shops or shops
    target_shops = _select_shops(shops_cfg, shops_value)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    print(
        "probe_ads_start "
        f"date={target_date.isoformat()} "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} plan_path={plan} "
        f"artifacts_dir={artifacts_dir} analyze={1 if analyze else 0}"
    )

    plan_def = load_plan(Path(plan))
    vars_map = build_builtin_vars("probe", 0)
    vars_map.update(_build_date_vars_probe(target_date))

    if transport_value == "live":
        _require_shopee_settings(settings)

    total_ok = 0
    total_fail = 0
    overall_ok = True
    for shop_cfg in target_shops:
        calls_ok = 0
        calls_fail = 0
        artifacts_saved = 0
        analysis_saved: str | None = None
        probe_records: list[dict[str, object]] = []

        token = None
        if transport_value == "live":
            init_db()
            session = SessionLocal()
            try:
                token = get_token(session, shop_cfg.shop_key)
                if token is None:
                    raise RuntimeError("no token found; run shopee exchange-code first")
                if token_mode_value == "passive" and needs_refresh(token.access_token_expires_at):
                    print(
                        "token_expired_refresh_disabled "
                        f"shop={shop_cfg.shop_key} shop_id={_resolve_shop_id(shop_cfg)}"
                    )
                    raise typer.Exit(code=1)
                if needs_refresh(token.access_token_expires_at):
                    refreshed = refresh_access_token(
                        _build_shopee_client(settings),
                        settings.shopee_partner_id,
                        settings.shopee_partner_key,
                        shop_cfg.shopee_shop_id,
                        token.refresh_token,
                        int(datetime.now().timestamp()),
                    )
                    upsert_token(
                        session,
                        shop_cfg.shop_key,
                        refreshed.shop_id,
                        refreshed.access_token,
                        refreshed.refresh_token,
                        refreshed.access_expires_at,
                    )
                    session.commit()
                    token = get_token(session, shop_cfg.shop_key)
            finally:
                session.close()

        for call in plan_def.calls:
            params = interpolate_data(call.params, vars_map)
            body = interpolate_data(call.body, vars_map) if call.body else None
            api_path = interpolate_data(call.path, vars_map)
            payload: dict | None = None
            ok = True
            error_text: str | None = None
            try:
                if transport_value == "fixtures":
                    payload = _load_probe_fixture_payload(fixtures_path, call.name)
                    if payload is None:
                        ok = False
                        error_text = "fixture_missing"
                else:
                    payload = _build_shopee_client(settings).request(
                        call.method,
                        api_path,
                        shop_id=shop_cfg.shopee_shop_id,
                        access_token=token.access_token if token else None,
                        params=params or None,
                        json=body,
                    )
            except Exception as exc:  # noqa: BLE001
                ok = False
                error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"

            if ok and isinstance(payload, dict):
                error_code = payload.get("error")
                if error_code not in (None, 0):
                    ok = False
                    message = payload.get("message") or payload.get("msg") or "-"
                    error_text = f"shopee_error_{error_code}: {message}"

            if ok:
                calls_ok += 1
            else:
                calls_fail += 1
                overall_ok = False

            artifact_path = _write_probe_artifact(
                Path(artifacts_dir),
                shop_cfg.shop_key,
                target_date,
                call.name,
                payload,
                ok,
                error_text,
            )
            artifacts_saved += 1 if artifact_path else 0

            if analyze:
                probe_records.append(
                    _analyze_probe_payload(call.name, payload or {})
                )

        if analyze:
            analysis_saved = str(
                _write_probe_analysis(
                    Path(analysis_dir),
                    shop_cfg.shop_key,
                    target_date,
                    probe_records,
                )
            )

        total_ok += calls_ok
        total_fail += calls_fail
        print(
            f"shop={shop_cfg.shop_key} calls_ok={calls_ok} calls_fail={calls_fail} "
            f"artifacts_saved={artifacts_saved} analysis_saved={analysis_saved or '-'}"
        )

        if send_discord:
            message = (
                f"[{shop_cfg.label}][ACTION] Ads probe complete "
                f"date={target_date.isoformat()} calls_ok={calls_ok} calls_fail={calls_fail}"
            )
            try:
                send("report", message, shop_label=shop_cfg.label)
            except Exception:
                pass

    if overall_ok:
        print("probe_ok=1")
    else:
        print("probe_ok=0")
        raise typer.Exit(code=1)


@ops_phase1_ads_app.command("campaign-probe")
def ops_phase1_ads_campaign_probe(
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    mode: str = typer.Option("live", "--mode", help="live | dry-run | fixtures"),
    days: int = typer.Option(7, "--days", help="GMS parity lookback days"),
    out: str | None = typer.Option(
        None,
        "--out",
        help="Output root dir (default: collaboration/artifacts/ads_campaign_probe/<ts>)",
    ),
    redact: bool = typer.Option(True, "--redact/--no-redact"),
    json_fixture: str | None = typer.Option(
        None, "--json-fixture", help="Dry-run fixture JSON path"
    ),
    max_requests: int = typer.Option(
        0,
        "--max-requests",
        help="Max API requests per shop in live mode (0 = unlimited)",
    ),
    sync_db: bool = typer.Option(
        True,
        "--sync-db/--no-sync-db",
        help="Sync resolved campaign name/budget to ads_campaign table",
    ),
    ignore_cooldown: bool = typer.Option(
        False,
        "--ignore-cooldown",
        help="Ignore local rate-limit cooldown state for debugging",
    ),
    rate_limit_state: str | None = typer.Option(
        None,
        "--rate-limit-state",
        help=(
            "Persistent ads rate-limit state file path. "
            "Priority: CLI > DOTORI_ADS_RATE_LIMIT_STATE_PATH > <out>/rate_limit_state.json"
        ),
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    if isinstance(max_requests, typer.models.OptionInfo):
        max_requests = int(max_requests.default or 0)
    if isinstance(sync_db, typer.models.OptionInfo):
        sync_db = bool(sync_db.default)
    _maybe_load_env_file(env_file)
    mode_value = str(mode or "").strip().lower()
    if mode_value not in {"live", "dry-run", "fixtures"}:
        raise typer.BadParameter("mode must be one of: live, dry-run, fixtures")
    if days < 1:
        raise typer.BadParameter("days must be >= 1")
    if max_requests < 0:
        raise typer.BadParameter("max-requests must be >= 0")
    if mode_value == "dry-run" and not json_fixture:
        raise typer.BadParameter("--json-fixture is required in dry-run mode")

    fixture_payload: dict[str, Any] | None = None
    if json_fixture:
        fixture_path = Path(json_fixture)
        if not fixture_path.exists():
            raise typer.BadParameter(f"json-fixture not found: {fixture_path}")
        fixture_payload = _read_json(fixture_path)
        if not isinstance(fixture_payload, dict):
            raise typer.BadParameter("json-fixture must contain an object at top-level")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, only_shops)
    if not target_shops:
        print("campaign_probe_ok=0 reason=no_shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    if mode_value == "live":
        _require_shopee_settings(settings)

    out_dir = (
        Path(out)
        if out
        else Path("collaboration")
        / "artifacts"
        / "ads_campaign_probe"
        / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )

    print(
        "campaign_probe_start "
        f"mode={mode_value} shops={','.join([s.shop_key for s in target_shops])} "
        f"days={days} redact={1 if redact else 0} out={out_dir} "
        f"rate_limit_state={rate_limit_state or os.environ.get('DOTORI_ADS_RATE_LIMIT_STATE_PATH') or str(out_dir / 'rate_limit_state.json')}"
    )

    result = run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode=mode_value,
        days=days,
        out_dir=out_dir,
        redact=redact,
        fixture_payload=fixture_payload,
        max_requests_per_shop=max_requests if max_requests > 0 else None,
        sync_db=sync_db,
        ignore_cooldown=ignore_cooldown,
        rate_limit_state_path=rate_limit_state,
    )

    shop_results = result.get("shop_results") or []
    for row in shop_results:
        print(
            "campaign_probe_shop "
            f"shop={row.shop_key} id_list_count={row.id_list_count} "
            f"setting_rows_raw={row.setting_rows_raw} setting_chunks_ok={row.setting_chunks_ok} "
            f"setting_chunks_fail={row.setting_chunks_fail} "
            f"preflight_ok={1 if row.preflight_ok else 0} preflight_reason={row.preflight_reason or '-'} "
            f"registry_rows={len(row.registry_rows)} gms_ok={1 if row.gms_ok else 0} "
            f"gms_campaign_ids={len(row.gms_campaign_ids)}"
        )

    print(f"campaign_registry_csv={result.get('registry_csv')}")
    print(f"campaign_probe_summary_md={result.get('summary_md')}")
    print(f"campaign_probe_rate_limit_state_path={result.get('rate_limit_state_path')}")
    print(result.get("verdict"))
    print("campaign_probe_ok=1")


@ops_phase1_ads_app.command("gms-probe")
def ops_phase1_ads_gms_probe(
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    mode: str = typer.Option("live", "--mode", help="live | fixtures"),
    days: int = typer.Option(7, "--days", help="GMS probe lookback days"),
    out: str | None = typer.Option(
        None,
        "--out",
        help="Output root dir (default: collaboration/artifacts/ads_gms_probe/<ts>)",
    ),
    redact: bool = typer.Option(True, "--redact/--no-redact"),
    json_fixture: str | None = typer.Option(
        None, "--json-fixture", help="Fixture JSON path for --mode fixtures"
    ),
    max_gms_calls_per_shop: int = typer.Option(
        1,
        "--max-gms-calls-per-shop",
        help="Max GMS calls per shop for one run (default: 1)",
    ),
    force_once: bool = typer.Option(
        False,
        "--force-once",
        help="Ignore local cooldown and force one probe run",
    ),
    sync_db: bool = typer.Option(
        False,
        "--sync-db/--no-sync-db",
        help="Sync normalized GMS registry rows into phase1_ads_gms_campaign_registry",
    ),
    rate_limit_state: str | None = typer.Option(
        None,
        "--rate-limit-state",
        help=(
            "Persistent ads rate-limit state file path. "
            "Priority: CLI > DOTORI_ADS_RATE_LIMIT_STATE_PATH > <out>/rate_limit_state.json"
        ),
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    mode_value = str(mode or "").strip().lower()
    if mode_value not in {"live", "fixtures"}:
        raise typer.BadParameter("mode must be one of: live, fixtures")
    if days < 1:
        raise typer.BadParameter("days must be >= 1")
    if max_gms_calls_per_shop < 1:
        raise typer.BadParameter("max-gms-calls-per-shop must be >= 1")

    fixture_payload: dict[str, Any] | None = None
    if mode_value == "fixtures":
        fixture_path = Path(json_fixture) if json_fixture else Path("tests/fixtures/gms_performance_success.json")
        if not fixture_path.exists():
            raise typer.BadParameter(f"json-fixture not found: {fixture_path}")
        loaded = _read_json(fixture_path)
        if not isinstance(loaded, dict):
            raise typer.BadParameter("json-fixture must contain an object at top-level")
        fixture_payload = loaded

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, only_shops)
    if not target_shops:
        print("gms_probe_ok=0 reason=no_shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    if mode_value == "live":
        _require_shopee_settings(settings)

    out_dir = (
        Path(out)
        if out
        else Path("collaboration")
        / "artifacts"
        / "ads_gms_probe"
        / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )

    print(
        "gms_probe_start "
        f"mode={mode_value} shops={','.join([s.shop_key for s in target_shops])} "
        f"days={days} redact={1 if redact else 0} "
        f"max_gms_calls_per_shop={max_gms_calls_per_shop} force_once={1 if force_once else 0} "
        f"out={out_dir} "
        f"rate_limit_state={rate_limit_state or os.environ.get('DOTORI_ADS_RATE_LIMIT_STATE_PATH') or str(out_dir / 'rate_limit_state.json')}"
    )

    result = run_gms_probe(
        settings=settings,
        target_shops=target_shops,
        mode=mode_value,
        days=days,
        out_dir=out_dir,
        redact=redact,
        fixture_payload=fixture_payload,
        max_gms_calls_per_shop=max_gms_calls_per_shop,
        force_once=force_once,
        sync_db=sync_db,
        rate_limit_state_path=rate_limit_state,
    )

    shop_results = result.get("shop_results") or []
    for row in shop_results:
        print(
            "gms_probe_shop "
            f"shop={row.shop_key} gms_http={row.gms_http_status if row.gms_http_status is not None else '-'} "
            f"gms_api_error={row.gms_api_error or '-'} "
            f"gms_ok_count={row.gms_ok_count} gms_campaign_count={row.gms_campaign_count} "
            f"rate_limit_hit={'true' if row.rate_limit_hit else 'false'} "
            f"campaign_level_supported={row.campaign_level_supported} "
            f"gms_name_supported={row.gms_name_supported} "
            f"gms_budget_supported={row.gms_budget_supported} "
            f"probe_reason={row.probe_reason or '-'}"
        )

    verdict_bits = result.get("verdict_bits") or {}
    print(f"gms_registry_csv={result.get('gms_registry_csv')}")
    print(f"gms_probe_summary_md={result.get('summary_md')}")
    print(f"gms_probe_rate_limit_state_path={result.get('rate_limit_state_path')}")
    print(f"gms_as_of_date={result.get('as_of_date')}")
    print(f"gms_db_upserted={result.get('db_upserted')}")
    print(f"gms_campaign_level_supported={verdict_bits.get('gms_campaign_level_supported', 'unknown')}")
    print(f"gms_name_supported={verdict_bits.get('gms_name_supported', 'unknown')}")
    print(f"gms_budget_supported={verdict_bits.get('gms_budget_supported', 'unknown')}")
    print(result.get("verdict"))
    print("gms_probe_ok=1")


@ops_phase1_ads_app.command("campaign-meta")
def ops_phase1_ads_campaign_meta(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    only_shops: str = typer.Option(
        "samord,minmin", "--only-shops", help="Comma-separated shop keys"
    ),
    out: str | None = typer.Option(
        None,
        "--out",
        help="Output root dir (default: collaboration/artifacts/ads_campaign_meta/<date>_<ts>)",
    ),
    max_requests: int = typer.Option(
        30,
        "--max-requests",
        help="Max API requests per shop",
    ),
    redact: bool = typer.Option(True, "--redact/--no-redact"),
    sync_db: bool = typer.Option(
        True,
        "--sync-db/--no-sync-db",
        help="Sync resolved campaign name/budget to ads_campaign table",
    ),
    ignore_cooldown: bool = typer.Option(
        False,
        "--ignore-cooldown",
        help="Ignore local rate-limit cooldown state for debugging",
    ),
    rate_limit_state: str | None = typer.Option(
        None,
        "--rate-limit-state",
        help=(
            "Persistent ads rate-limit state file path. "
            "Priority: CLI > DOTORI_ADS_RATE_LIMIT_STATE_PATH > <out>/rate_limit_state.json"
        ),
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    if isinstance(max_requests, typer.models.OptionInfo):
        max_requests = int(max_requests.default or 30)
    if isinstance(sync_db, typer.models.OptionInfo):
        sync_db = bool(sync_db.default)
    _maybe_load_env_file(env_file)
    if max_requests < 1:
        raise typer.BadParameter("max-requests must be >= 1")
    target_date = _parse_required_date(date_value)

    settings = get_settings()
    _require_shopee_settings(settings)
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, only_shops)
    if not target_shops:
        print("campaign_meta_ok=0 reason=no_shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = (
        Path(out)
        if out
        else Path("collaboration")
        / "artifacts"
        / "ads_campaign_meta"
        / f"{target_date.isoformat()}_{ts}"
    )
    print(
        "campaign_meta_start "
        f"date={target_date.isoformat()} shops={','.join([s.shop_key for s in target_shops])} "
        f"max_requests={max_requests} out={out_dir} "
        f"rate_limit_state={rate_limit_state or os.environ.get('DOTORI_ADS_RATE_LIMIT_STATE_PATH') or str(out_dir / 'rate_limit_state.json')}"
    )
    result = run_campaign_probe(
        settings=settings,
        target_shops=target_shops,
        mode="live",
        days=1,
        out_dir=out_dir,
        redact=redact,
        fixture_payload=None,
        max_requests_per_shop=max_requests,
        sync_db=sync_db,
        ignore_cooldown=ignore_cooldown,
        rate_limit_state_path=rate_limit_state,
    )
    print(f"campaign_meta_registry_csv={result.get('registry_csv')}")
    print(f"campaign_meta_summary_md={result.get('summary_md')}")
    print(f"campaign_meta_rate_limit_state_path={result.get('rate_limit_state_path')}")
    print(result.get("verdict"))
    print("campaign_meta_ok=1")


@ops_phase1_ads_app.command("daily-truth")
def ops_phase1_ads_daily_truth(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    allow_network: bool = typer.Option(False, "--allow-network"),
    token_mode: str = typer.Option("passive", "--token-mode", help="default | passive"),
    plan: str = typer.Option(
        "collaboration/plans/ads_probe_daily_truth.yaml", "--plan", help="Plan YAML path"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-dir",
        help="Artifacts root directory",
    ),
    analysis_dir: str = typer.Option(
        "collaboration/probes", "--analysis-dir", help="Analysis output directory"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    target_date = _parse_required_date(date_value)
    transport_value = transport.lower().strip()
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    token_mode_value = _normalize_token_mode(token_mode)

    allow_network_env = os.environ.get("ALLOW_NETWORK", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    allow_network_effective = bool(allow_network or allow_network_env)
    if transport_value == "live" and not allow_network_effective:
        print("network_disabled error=allow_network_required")
        raise typer.Exit(code=1)

    fixtures_path = Path(fixtures_dir)
    if transport_value == "fixtures" and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    if transport_value == "live":
        _require_shopee_settings(settings)

    plan_path = Path(plan)
    plan_def = load_plan(plan_path)
    artifacts_root = Path(artifacts_dir)
    analysis_root = Path(analysis_dir)

    print(
        "ads_daily_truth_start "
        f"date={target_date.isoformat()} "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"transport={transport_value} plan_path={plan_path} "
        f"artifacts_dir={artifacts_root} analysis_dir={analysis_root}"
    )

    overall_ok = True
    for shop_cfg in target_shops:
        vars_map = build_builtin_vars(shop_cfg.shop_key, shop_cfg.shopee_shop_id or 0)
        vars_map.update(_build_date_vars_probe(target_date))
        token = None
        client = _build_shopee_client(settings) if transport_value == "live" else None
        if transport_value == "live":
            init_db()
            session = SessionLocal()
            try:
                token = get_token(session, shop_cfg.shop_key)
                if token is None:
                    print(
                        f"ads_daily_truth_shop_ok=0 shop={shop_cfg.shop_key} reason=missing_token"
                    )
                    overall_ok = False
                    continue
                if needs_refresh(token.access_token_expires_at):
                    if token_mode_value == "passive":
                        print(
                            f"ads_daily_truth_shop_ok=0 shop={shop_cfg.shop_key} "
                            "reason=expired_access_token"
                        )
                        overall_ok = False
                        continue
                    refreshed = refresh_access_token(
                        _build_shopee_client(settings),
                        settings.shopee_partner_id,
                        settings.shopee_partner_key,
                        shop_cfg.shopee_shop_id,
                        token.refresh_token,
                        int(datetime.now().timestamp()),
                    )
                    upsert_token(
                        session,
                        shop_cfg.shop_key,
                        refreshed.shop_id,
                        refreshed.access_token,
                        refreshed.refresh_token,
                        refreshed.access_expires_at,
                    )
                    session.commit()
                    token = get_token(session, shop_cfg.shop_key)
            finally:
                session.close()

        daily_payload: dict | None = None
        saved_paths: list[Path] = []
        for call in plan_def.calls:
            params = interpolate_data(call.params, vars_map)
            body = interpolate_data(call.body, vars_map) if call.body else None
            api_path = interpolate_data(call.path, vars_map)
            payload: dict | None = None
            ok = True
            error_text: str | None = None
            try:
                if transport_value == "fixtures":
                    payload = _load_probe_fixture_payload(fixtures_path, call.name)
                    if payload is None:
                        ok = False
                        error_text = "fixture_missing"
                else:
                    if call.name == "ads_daily":
                        timestamp = int(datetime.now(timezone.utc).timestamp())

                        def _request_ads_daily(p: dict[str, str]) -> dict:
                            return client.request(
                                call.method,
                                api_path,
                                shop_id=shop_cfg.shopee_shop_id,
                                access_token=token.access_token if token else None,
                                params=p or None,
                                json=body,
                                timestamp=timestamp,
                            )

                        payload, _params_used, mode, fmt, attempts = _call_ads_daily_with_fallback(
                            request_fn=_request_ads_daily,
                            date_iso=target_date.isoformat(),
                            initial_mode="range",
                            initial_format="dmy",
                        )
                        if attempts > 1:
                            print(
                                "ads_daily_truth_param_fallback "
                                f"shop={shop_cfg.shop_key} mode={mode} fmt={fmt} attempts={attempts}"
                            )
                    else:
                        payload = client.request(
                            call.method,
                            api_path,
                            shop_id=shop_cfg.shopee_shop_id,
                            access_token=token.access_token if token else None,
                            params=params or None,
                            json=body,
                        )
            except Exception as exc:  # noqa: BLE001
                ok = False
                error_text = _scrub_sensitive_text(str(exc)) or "unknown_error"

            if ok and isinstance(payload, dict):
                err = payload.get("error")
                if err not in (None, 0, "0", ""):
                    ok = False
                    msg = payload.get("message") or payload.get("msg") or "-"
                    error_text = f"shopee_error_{err}: {msg}"

            saved_path = _write_daily_truth_artifact(
                artifacts_root,
                shop_cfg.shop_key,
                target_date,
                call.name,
                payload,
                ok,
                error_text,
            )
            saved_paths.append(saved_path)
            size_bytes = saved_path.stat().st_size if saved_path.exists() else 0
            print(
                f"saved_json_path={saved_path} size_bytes={size_bytes} "
                f"shop={shop_cfg.shop_key} call={call.name}"
            )
            if call.name == "ads_daily" and isinstance(payload, dict):
                daily_payload = payload

        if daily_payload is None:
            print(
                f"ads_daily_truth_shop_ok=0 shop={shop_cfg.shop_key} reason=missing_ads_daily_payload"
            )
            overall_ok = False
            continue

        summary = _detect_ads_daily_truth(daily_payload)
        print(
            "daily_shape_detected "
            f"shop={shop_cfg.shop_key} items_path={summary.get('items_path', '-')} "
            f"item_count={summary.get('item_count', 0)}"
        )
        print(
            "detected_fields "
            f"shop={shop_cfg.shop_key} "
            f"spend_path={summary.get('spend_path', '-')} "
            f"clicks_path={summary.get('clicks_path', '-')} "
            f"impr_path={summary.get('impr_path', '-')} "
            f"orders_path={summary.get('orders_path', '-')} "
            f"gmv_path={summary.get('gmv_path', '-')}"
        )
        summary_path = _write_ads_daily_truth_summary(
            analysis_root=analysis_root,
            shop_key=shop_cfg.shop_key,
            target_date=target_date,
            summary=summary,
            saved_paths=saved_paths,
        )
        print(f"ads_daily_truth_summary_path={summary_path} shop={shop_cfg.shop_key}")
        print(
            f"ads_daily_truth_shop_ok={1 if int(summary.get('item_count', 0)) >= 1 else 0} "
            f"shop={shop_cfg.shop_key}"
        )
        if int(summary.get("item_count", 0)) < 1:
            overall_ok = False

    print(f"ads_daily_truth_ok={1 if overall_ok else 0}")
    if not overall_ok:
        raise typer.Exit(code=1)


@ops_phase1_ads_app.command("campaign-daily-truth")
def ops_phase1_ads_campaign_daily_truth(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Alias for --shops"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    allow_network: bool = typer.Option(
        False, "--allow-network", help="Enable live network mode"
    ),
    token_mode: str = typer.Option("passive", "--token-mode", help="default | passive"),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-dir",
        help="Artifacts root directory",
    ),
    max_campaigns: int = typer.Option(50, "--max-campaigns", help="Max campaign ids"),
    chunk_size: int = typer.Option(50, "--chunk-size", help="Chunk size for campaign_id_list"),
    try_alt_endpoints: bool = typer.Option(
        True,
        "--try-alt-endpoints/--no-try-alt-endpoints",
        help="Try direct/all-cpc fallback endpoints when id-list flow is blocked",
    ),
) -> None:
    _maybe_load_env_file(env_file)
    target_date = _parse_required_date(date_value)
    shops_value = only_shops or shops
    transport_value = "live" if allow_network else "fixtures"
    token_mode_value = _normalize_token_mode(token_mode)
    artifacts_root = Path(artifacts_dir)
    fixtures_path = Path(fixtures_dir)
    if transport_value == "fixtures" and not fixtures_path.exists():
        raise typer.BadParameter(f"fixtures-dir not found: {fixtures_path}")
    if max_campaigns < 1:
        raise typer.BadParameter("max-campaigns must be >= 1")
    if chunk_size < 1:
        raise typer.BadParameter("chunk-size must be >= 1")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops_value)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_phase1_shops_only(shops_value, target_shops)
    _ensure_shop_ids(target_shops)
    if transport_value == "live":
        _require_shopee_settings(settings)

    print(
        "campaign_daily_truth_start "
        f"date={target_date.isoformat()} shops={','.join([s.shop_key for s in target_shops])} "
        f"transport={transport_value} max_campaigns={max_campaigns} chunk_size={chunk_size} "
        f"try_alt_endpoints={1 if try_alt_endpoints else 0}"
    )

    overall_ok = True
    for shop_cfg in target_shops:
        shop_root = artifacts_root / shop_cfg.shop_key / target_date.isoformat() / "ads_campaign_daily_truth"
        shop_root.mkdir(parents=True, exist_ok=True)
        saved_json_paths: list[str] = []
        endpoint_results: list[dict[str, object]] = []
        token = None
        client = _build_shopee_client(settings) if transport_value == "live" else None

        if transport_value == "live":
            init_db()
            session = SessionLocal()
            try:
                token = get_token(session, shop_cfg.shop_key)
                if token is None:
                    summary = {
                        "shop_key": shop_cfg.shop_key,
                        "date": target_date.isoformat(),
                        "transport": transport_value,
                        "verdict": "NOT_SUPPORTED",
                        "reason": "missing_token",
                        "items_total": 0,
                        "has_campaign_id_field": 0,
                        "non_total_campaign_rows": 0,
                        "spend_total": "0.00",
                        "impressions_total": 0,
                        "clicks_total": 0,
                        "orders_total": 0,
                        "gmv_total": "0.00",
                        "ids_total": 0,
                        "chunks_total": 0,
                        "api_error": "-",
                        "api_message": "-",
                        "selected_endpoint": "-",
                        "try_alt_endpoints": 1 if try_alt_endpoints else 0,
                        "endpoint_results": [],
                        "saved_json_paths": [],
                    }
                    md_path, json_path = _write_campaign_daily_truth_summary_files(
                        root=artifacts_root,
                        shop_key=shop_cfg.shop_key,
                        target_date=target_date,
                        summary=summary,
                    )
                    print(f"campaign_daily_truth_summary_md={md_path}")
                    print(f"campaign_daily_truth_summary_json={json_path}")
                    print(f"campaign_daily_truth_shop shop={shop_cfg.shop_key} verdict=NOT_SUPPORTED reason=missing_token")
                    overall_ok = False
                    continue
                if needs_refresh(token.access_token_expires_at):
                    if token_mode_value == "passive":
                        summary = {
                            "shop_key": shop_cfg.shop_key,
                            "date": target_date.isoformat(),
                            "transport": transport_value,
                            "verdict": "NOT_SUPPORTED",
                            "reason": "expired_access_token",
                            "items_total": 0,
                            "has_campaign_id_field": 0,
                            "non_total_campaign_rows": 0,
                            "spend_total": "0.00",
                            "impressions_total": 0,
                            "clicks_total": 0,
                            "orders_total": 0,
                            "gmv_total": "0.00",
                            "ids_total": 0,
                            "chunks_total": 0,
                            "api_error": "-",
                            "api_message": "-",
                            "selected_endpoint": "-",
                            "try_alt_endpoints": 1 if try_alt_endpoints else 0,
                            "endpoint_results": [],
                            "saved_json_paths": [],
                        }
                        md_path, json_path = _write_campaign_daily_truth_summary_files(
                            root=artifacts_root,
                            shop_key=shop_cfg.shop_key,
                            target_date=target_date,
                            summary=summary,
                        )
                        print(f"campaign_daily_truth_summary_md={md_path}")
                        print(f"campaign_daily_truth_summary_json={json_path}")
                        print(f"campaign_daily_truth_shop shop={shop_cfg.shop_key} verdict=NOT_SUPPORTED reason=expired_access_token")
                        overall_ok = False
                        continue
                    refreshed = refresh_access_token(
                        client,
                        settings.shopee_partner_id,
                        settings.shopee_partner_key,
                        shop_cfg.shopee_shop_id,
                        token.refresh_token,
                        int(datetime.now().timestamp()),
                    )
                    upsert_token(
                        session,
                        shop_cfg.shop_key,
                        refreshed.shop_id,
                        refreshed.access_token,
                        refreshed.refresh_token,
                        refreshed.access_expires_at,
                    )
                    session.commit()
                    token = get_token(session, shop_cfg.shop_key)
            finally:
                session.close()
        breakdown_payload, breakdown_meta = _fetch_campaign_daily_breakdown_payload(
            client=client,
            shop_key=shop_cfg.shop_key,
            shop_id=shop_cfg.shopee_shop_id,
            access_token=token.access_token if token else None,
            target_date=target_date,
            fixtures_dir=fixtures_path if transport_value == "fixtures" else None,
            max_campaigns=max_campaigns,
            chunk_size=chunk_size,
            try_alt_endpoints=try_alt_endpoints,
        )
        endpoint_results = [
            dict(item) for item in (breakdown_meta.get("endpoint_results", []) or [])
        ]
        endpoint_payloads = breakdown_meta.get("endpoint_payloads", []) or []

        for entry in endpoint_payloads:
            endpoint_name = str(entry.get("endpoint") or "unknown")
            endpoint_order = int(entry.get("order") or (len(saved_json_paths) + 1))
            endpoint_payload = entry.get("payload")
            if not isinstance(endpoint_payload, dict):
                continue
            filename = f"endpoint_{endpoint_order:02d}_{safe_name(endpoint_name)}.json"
            output_path = shop_root / filename
            _save_redacted_json(output_path, endpoint_payload)
            saved_json_paths.append(str(output_path))
            print(
                f"saved_json_path={output_path} size_bytes={output_path.stat().st_size} "
                f"shop={shop_cfg.shop_key} call=campaign_daily_endpoint endpoint={endpoint_name}"
            )

        for endpoint_row in endpoint_results:
            endpoint_name = str(endpoint_row.get("endpoint") or "-")
            endpoint_order = int(endpoint_row.get("order") or 0)
            endpoint_ok = int(endpoint_row.get("ok") or 0)
            endpoint_reason = str(endpoint_row.get("reason") or "-")
            endpoint_http = endpoint_row.get("http_status")
            endpoint_api_error = endpoint_row.get("api_error")
            endpoint_items = int(endpoint_row.get("items_total") or 0)
            endpoint_has_cid = int(endpoint_row.get("has_campaign_id_field") or 0)
            endpoint_request_id = endpoint_row.get("request_id")
            print(
                "campaign_daily_truth_endpoint "
                f"shop={shop_cfg.shop_key} order={endpoint_order} endpoint={endpoint_name} "
                f"ok={endpoint_ok} reason={endpoint_reason} "
                f"http_status={endpoint_http if endpoint_http is not None else '-'} "
                f"api_error={endpoint_api_error if endpoint_api_error not in (None, '') else '-'} "
                f"request_id={endpoint_request_id if endpoint_request_id not in (None, '') else '-'} "
                f"items_total={endpoint_items} has_campaign_id_field={endpoint_has_cid}"
            )

        records = _campaign_daily_truth_extract_records(
            breakdown_payload if isinstance(breakdown_payload, dict) else None
        )
        metrics = _campaign_daily_truth_metrics(records)
        items_total = int(metrics["items_total"])
        has_campaign_id_field = int(metrics["has_campaign_id_field"])
        non_total_campaign_rows = int(metrics["non_total_campaign_rows"])
        verdict = "SUPPORTED" if bool(breakdown_meta.get("ok")) else "NOT_SUPPORTED"
        reason = str(breakdown_meta.get("reason") or "campaign_rows_not_detected")
        if verdict != "SUPPORTED":
            overall_ok = False

        summary = {
            "shop_key": shop_cfg.shop_key,
            "date": target_date.isoformat(),
            "transport": transport_value,
            "verdict": verdict,
            "reason": reason,
            "items_total": items_total,
            "has_campaign_id_field": has_campaign_id_field,
            "non_total_campaign_rows": non_total_campaign_rows,
            "spend_total": f"{metrics['spend_total']:.2f}",
            "impressions_total": int(metrics["impressions_total"]),
            "clicks_total": int(metrics["clicks_total"]),
            "orders_total": int(metrics["orders_total"]),
            "gmv_total": f"{metrics['gmv_total']:.2f}",
            "ids_total": int(breakdown_meta.get("ids_total") or 0),
            "chunks_total": int(breakdown_meta.get("chunks") or 0),
            "selected_endpoint": str(breakdown_meta.get("selected_endpoint") or "-"),
            "blocked_403": 1 if breakdown_meta.get("blocked_403") else 0,
            "try_alt_endpoints": 1 if try_alt_endpoints else 0,
            "api_error": "-"
            if breakdown_meta.get("api_error") in (None, "")
            else str(breakdown_meta.get("api_error")),
            "api_message": "-"
            if breakdown_meta.get("api_message") in (None, "")
            else _scrub_sensitive_text(str(breakdown_meta.get("api_message"))),
            "endpoint_results": endpoint_results,
            "saved_json_paths": saved_json_paths,
        }
        md_path, json_path = _write_campaign_daily_truth_summary_files(
            root=artifacts_root,
            shop_key=shop_cfg.shop_key,
            target_date=target_date,
            summary=summary,
        )
        print(f"daily_shape_detected shop={shop_cfg.shop_key} items_path=response.records item_count={items_total}")
        print(
            "detected_fields "
            f"shop={shop_cfg.shop_key} "
            "spend_path=response.records[].spend "
            "clicks_path=response.records[].clicks "
            "impr_path=response.records[].impressions "
            "orders_path=response.records[].orders "
            "gmv_path=response.records[].gmv"
        )
        print(f"campaign_daily_truth_summary_md={md_path}")
        print(f"campaign_daily_truth_summary_json={json_path}")
        print(
            f"campaign_daily_truth_shop shop={shop_cfg.shop_key} verdict={verdict} "
            f"items_total={items_total} has_campaign_id_field={has_campaign_id_field} "
            f"selected_endpoint={summary.get('selected_endpoint', '-')}"
        )

    print(f"campaign_daily_truth_ok={1 if overall_ok else 0}")
    if not overall_ok:
        raise typer.Exit(code=1)


@ops_phase1_ads_app.command("campaign-breakdown-support-pack")
def ops_phase1_ads_campaign_breakdown_support_pack(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Alias for --shops"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    transport: str = typer.Option("fixtures", "--transport", help="fixtures | live"),
    allow_network: bool = typer.Option(
        False, "--allow-network", help="Enable live network mode"
    ),
    token_mode: str = typer.Option("passive", "--token-mode", help="default | passive"),
    token_file: str | None = typer.Option(
        None, "--token-file", help="Apps Script export JSON path"
    ),
    fixtures_dir: str = typer.Option(
        "tests/fixtures/shopee_ads", "--fixtures-dir", help="Fixtures directory"
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/artifacts/shopee_api",
        "--artifacts-dir",
        help="Artifacts root directory",
    ),
    out_dir: str = typer.Option(
        "collaboration/support_packets", "--out-dir", help="Support packet output dir"
    ),
    try_alt_endpoints: bool = typer.Option(
        True,
        "--try-alt-endpoints/--no-try-alt-endpoints",
        help="Try direct/all-cpc fallback endpoints when id-list flow is blocked",
    ),
    max_campaigns: int = typer.Option(50, "--max-campaigns", help="Max campaign ids"),
    chunk_size: int = typer.Option(50, "--chunk-size", help="Chunk size for campaign_id_list"),
) -> None:
    _maybe_load_env_file(env_file)
    target_date = _parse_required_date(date_value)
    shops_value = only_shops or shops
    transport_value = transport.strip().lower()
    if allow_network:
        transport_value = "live"
    if transport_value not in {"fixtures", "live"}:
        raise typer.BadParameter("transport must be fixtures or live")
    if transport_value == "live" and not allow_network:
        raise typer.BadParameter("allow_network_required transport=live")
    if max_campaigns < 1:
        raise typer.BadParameter("max-campaigns must be >= 1")
    if chunk_size < 1:
        raise typer.BadParameter("chunk-size must be >= 1")

    settings = get_settings()
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops_value)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_phase1_shops_only(shops_value, target_shops)
    _ensure_shop_ids(target_shops)

    if token_file:
        _sync_tokens_from_file(token_file=token_file, target_shops=target_shops)
        _print_db_token_fingerprints(target_shops)

    if transport_value == "live":
        _require_shopee_settings(settings)

    artifacts_root = Path(artifacts_dir)
    output_root = Path(out_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    packet_dir = output_root / f"ads_campaign_breakdown_403_{target_date.isoformat()}_{stamp}"
    packet_dir.mkdir(parents=True, exist_ok=True)
    ticket_template_path = packet_dir / "ticket_template.md"
    zip_path = (
        output_root
        / f"support_packet_ads_campaign_breakdown_403_{target_date.isoformat()}_{stamp}.zip"
    )

    print(
        "support_pack_start "
        f"date={target_date.isoformat()} shops={','.join([s.shop_key for s in target_shops])} "
        f"transport={transport_value} try_alt_endpoints={1 if try_alt_endpoints else 0}"
    )

    probe_exit_code = 0
    try:
        ops_phase1_ads_campaign_daily_truth(
            date_value=target_date.isoformat(),
            shops=shops_value,
            only_shops=None,
            env_file=None,
            allow_network=(transport_value == "live"),
            token_mode=token_mode,
            fixtures_dir=fixtures_dir,
            artifacts_dir=artifacts_dir,
            max_campaigns=max_campaigns,
            chunk_size=chunk_size,
            try_alt_endpoints=try_alt_endpoints,
        )
    except typer.Exit as exc:
        probe_exit_code = int(getattr(exc, "exit_code", 1) or 0)
    except SystemExit as exc:  # noqa: PERF203
        probe_exit_code = int(exc.code or 0)

    print(f"support_pack_probe_exit_code={probe_exit_code}")

    artifact_files, summaries = _collect_campaign_daily_truth_artifacts(
        artifacts_root=artifacts_root,
        target_shops=target_shops,
        target_date=target_date,
    )
    if not summaries:
        print("support_pack_ok=0 error=missing_summary_files")
        raise typer.Exit(code=1)

    extra_request_ids: set[str] = set()
    for path in artifact_files:
        if path.suffix.lower() != ".json":
            continue
        try:
            payload = _read_json(path)
        except Exception:  # noqa: BLE001
            continue
        _collect_request_ids(payload, extra_request_ids)

    ticket_text = _build_campaign_breakdown_ticket_template(
        target_date=target_date,
        transport_value=transport_value,
        settings=settings,
        target_shops=target_shops,
        summaries=summaries,
        probe_exit_code=probe_exit_code,
        generated_at_utc=datetime.now(timezone.utc),
        extra_request_ids=extra_request_ids,
    )
    ticket_template_path.write_text(
        _scrub_sensitive_text(redact_text(ticket_text)),
        encoding="utf-8",
    )

    files_to_scan = [ticket_template_path, *artifact_files]
    leak_path = _scan_files_for_secrets(files_to_scan)
    if leak_path:
        print(f"support_pack_ok=0 error=secret_leak_detected file={leak_path}")
        raise typer.Exit(code=1)

    if zip_path.exists():
        try:
            zip_path.unlink()
        except Exception:  # noqa: BLE001
            pass

    zipped = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(ticket_template_path, "ticket_template.md")
        zipped += 1
        for path in artifact_files:
            try:
                rel = path.relative_to(artifacts_root)
            except ValueError:
                rel = Path(path.name)
            arcname = str(Path("artifacts") / rel)
            zipf.write(path, arcname)
            zipped += 1

    print(f"ticket_template_path={ticket_template_path}")
    print(f"zip_path={zip_path}")
    print(f"support_pack_files={zipped}")
    print("support_pack_ok=1")


@ops_phase1_status_app.command("dump")
def ops_phase1_status_dump(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys (Phase1 only)"
    ),
    reports_dir: str | None = typer.Option(
        None, "--reports-dir", help="Override REPORTS_DIR for this command"
    ),
    out: str | None = typer.Option(None, "--out", help="Write JSON output path"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    payload = _build_phase1_status_payload_for_cli(
        shops_value=shops,
        reports_dir=reports_dir,
    )
    text = _dump_json(payload, pretty=pretty)
    if out:
        out_path = Path(out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
    print(text)


@ops_phase1_app.command("doctor")
def ops_phase1_doctor(
    mode: str = typer.Argument("run", help="run | notify"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys (Phase1 only)"
    ),
    reports_dir: str | None = typer.Option(
        None, "--reports-dir", help="Override REPORTS_DIR for this command"
    ),
    artifacts_dir: str | None = typer.Option(
        None, "--artifacts-dir", help="Write doctor_status.json and doctor_summary.md"
    ),
    max_issues: int = typer.Option(20, "--max-issues", help="Top issue codes to print"),
    min_severity: str = typer.Option(
        "error", "--min-severity", help="warn | error (for notify mode)"
    ),
    discord_mode: str = typer.Option(
        "dry-run", "--discord-mode", help="dry-run | send (for notify mode)"
    ),
    persist_state: bool = typer.Option(
        False,
        "--persist-state",
        help="Persist cooldown/state during dry-run notify mode",
    ),
    aggregate: bool = typer.Option(
        False, "--aggregate", help="Send one aggregated message for all shops (notify mode)"
    ),
    confirm_discord_send: bool = typer.Option(
        False, "--confirm-discord-send", help="Required to actually send Discord notifications"
    ),
    cooldown_sec: int = typer.Option(
        3600, "--cooldown-sec", help="Per-shop alert cooldown seconds (notify mode)"
    ),
    resolved_cooldown_sec: int = typer.Option(
        21600, "--resolved-cooldown-sec", help="Per-shop resolved cooldown seconds (notify mode)"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    mode_value = str(mode or "").strip().lower()
    if mode_value not in {"run", "notify"}:
        raise typer.BadParameter("mode must be one of: run, notify")
    payload = _build_phase1_status_payload_for_cli(
        shops_value=shops,
        reports_dir=reports_dir,
    )
    summary_lines = _build_phase1_doctor_summary_lines(
        payload=payload,
        max_issues=max_issues,
    )
    for line in summary_lines:
        print(line)

    status_path, summary_path = _write_phase1_doctor_artifacts(
        artifacts_dir=artifacts_dir,
        payload=payload,
        summary_lines=summary_lines,
    )

    if mode_value == "notify":
        target_shops = _phase1_select_shops(shops)
        _phase1_doctor_notify_run(
            payload=payload,
            target_shops=target_shops,
            min_severity=min_severity,
            discord_mode=discord_mode,
            confirm_discord_send=confirm_discord_send,
            persist_state=persist_state,
            reports_dir=reports_dir,
            aggregate=aggregate,
            cooldown_sec=cooldown_sec,
            resolved_cooldown_sec=resolved_cooldown_sec,
            max_issues=max_issues,
            summary_path=summary_path,
        )
        return

    issues = [row for row in payload.get("issues", []) if isinstance(row, dict)]
    exit_code = _phase1_doctor_exit_code(issues)
    print(f"doctor_exit_code={exit_code}")
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


def _phase1_render_from_db_window(
    *, job_value: str, anchor_date: date
) -> dict[str, object]:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    anchor_dt = datetime.combine(anchor_date, time(13, 0), tzinfo=tz)
    window_start: date
    window_end: date
    report_kind: str | None = None
    report_date: date | None = None
    as_of: datetime | None = None
    week_id_value: str | None = None
    ingest_date: date
    if job_value == "daily-final":
        report_kind = "final"
        report_date = anchor_date - timedelta(days=1)
        window_start = report_date
        window_end = report_date
        ingest_date = report_date
    elif job_value == "daily-midday":
        report_kind = "midday"
        report_date = anchor_date
        window_start = report_date
        window_end = report_date
        ingest_date = report_date
        as_of = datetime.combine(report_date, time(13, 0), tzinfo=tz)
    else:
        window_start, window_end = get_last_week_range(anchor_dt, tz)
        week_id_value = weekly_id(window_start)
        ingest_date = window_end
    return {
        "anchor_dt": anchor_dt,
        "window_start": window_start,
        "window_end": window_end,
        "report_kind": report_kind,
        "report_date": report_date,
        "as_of": as_of,
        "week_id": week_id_value,
        "ingest_date": ingest_date,
    }


def _build_phase1_render_from_db_bundle(
    *, bundle_path: Path, report_paths: list[dict[str, object]]
) -> dict[str, object]:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    files_added: list[str] = []
    seen_paths: set[str] = set()
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for row in report_paths:
            raw_path = str(row.get("path") or "").strip()
            if not raw_path:
                continue
            report_path = Path(raw_path)
            report_key = str(report_path.resolve()) if report_path.exists() else raw_path
            if not report_path.exists() or report_key in seen_paths:
                continue
            seen_paths.add(report_key)
            shop = str(row.get("shop") or "shop")
            job = str(row.get("job") or "job")
            arcname = f"reports/{job}/{shop}/{report_path.name}"
            zf.write(report_path, arcname=arcname)
            files_added.append(arcname)
    bundle_size = bundle_path.stat().st_size if bundle_path.exists() else 0
    return {
        "path": str(bundle_path),
        "files": int(len(files_added)),
        "size": int(bundle_size),
        "entries": files_added,
    }


@ops_phase1_reports_app.command("render-from-db")
def ops_phase1_report_render_from_db(
    date_value: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    job: str = typer.Option(..., "--job", help="daily-final | daily-midday | weekly"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    reports_dir: str = typer.Option(
        "collaboration/reports", "--reports-dir", help="Reports output directory"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
    discord_mode: str = typer.Option(
        "dry-run", "--discord-mode", help="dry-run | send"
    ),
    discord_attach_report_html: bool = typer.Option(
        False,
        "--discord-attach-report-html",
        help="Attach generated daily report HTML when sending Discord report messages",
    ),
    discord_attach_report_zip: bool = typer.Option(
        False,
        "--discord-attach-report-zip",
        help="Attach generated daily report ZIP when sending Discord report messages",
    ),
    discord_attach_report_md: bool = typer.Option(
        False,
        "--discord-attach-report-md",
        help="Attach generated daily report Markdown summary when sending Discord report messages",
    ),
    bundle_out: str | None = typer.Option(
        None, "--bundle-out", help="Optional bundle zip output path"
    ),
) -> None:
    _maybe_load_env_file(env_file)
    job_value = job.strip().lower()
    if job_value not in {"daily-final", "daily-midday", "weekly"}:
        raise typer.BadParameter("job must be one of: daily-final, daily-midday, weekly")

    discord_mode_value = discord_mode.strip().lower()
    if discord_mode_value not in {"dry-run", "send"}:
        raise typer.BadParameter("discord-mode must be one of: dry-run, send")

    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir

    attach_report_html_env = os.environ.get("DISCORD_ATTACH_REPORT_HTML", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_html_effective = bool(discord_attach_report_html or attach_report_html_env)
    attach_report_zip_env = os.environ.get("DISCORD_ATTACH_REPORT_ZIP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_zip_effective = bool(discord_attach_report_zip or attach_report_zip_env)
    attach_report_md_env = os.environ.get("DISCORD_ATTACH_REPORT_MD", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    attach_report_md_effective = bool(discord_attach_report_md or attach_report_md_env)

    if discord_mode_value == "send":
        os.environ.pop("DISCORD_DRY_RUN", None)
    else:
        os.environ["DISCORD_DRY_RUN"] = "1"

    get_settings.cache_clear()
    settings = get_settings()

    anchor_date = _parse_required_date(date_value)
    window = _phase1_render_from_db_window(job_value=job_value, anchor_date=anchor_date)
    report_date = window.get("report_date")
    window_start = window["window_start"]
    window_end = window["window_end"]
    as_of = window.get("as_of")
    week_id_value = window.get("week_id")

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_shop_ids(target_shops)

    print(
        "render_from_db_start "
        f"job={job_value} date={anchor_date.isoformat()} "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"reports_dir={reports_dir} discord_mode={discord_mode_value} "
        f"discord_attach_report_html={1 if attach_report_html_effective else 0} "
        f"discord_attach_report_zip={1 if attach_report_zip_effective else 0} "
        f"discord_attach_report_md={1 if attach_report_md_effective else 0} "
        "transport=none allow_network=0"
    )
    print(
        "computed_report_window "
        f"job={job_value} start={window_start.isoformat()} end={window_end.isoformat()} "
        f"report_date={report_date.isoformat() if isinstance(report_date, date) else '-'} "
        f"as_of={as_of.isoformat() if isinstance(as_of, datetime) else '-'} "
        f"week_id={week_id_value or '-'}"
    )

    init_db()
    session = SessionLocal()
    missing_shops: list[str] = []
    available_counts: dict[str, int] = {}
    try:
        for shop_cfg in target_shops:
            query = session.query(func.count(AdsCampaignDaily.id)).filter(
                AdsCampaignDaily.shop_key == shop_cfg.shop_key
            )
            if job_value == "weekly":
                query = query.filter(
                    AdsCampaignDaily.date >= window_start,
                    AdsCampaignDaily.date <= window_end,
                )
            else:
                assert isinstance(report_date, date)
                query = query.filter(AdsCampaignDaily.date == report_date)
            count_value = int(query.scalar() or 0)
            available_counts[shop_cfg.shop_key] = count_value
            print(
                "render_from_db_data "
                f"shop={shop_cfg.shop_key} rows_daily={count_value}"
            )
            if count_value <= 0:
                missing_shops.append(shop_cfg.shop_key)
    finally:
        session.close()

    if missing_shops:
        print(
            "render_from_db_ok=0 "
            f"reason=missing_db_data missing_shops={','.join(sorted(missing_shops))} "
            f"job={job_value} date={anchor_date.isoformat()}"
        )
        print(
            "next_steps: run ops phase1 schedule run-once --transport fixtures|live "
            f"--job {job_value} --date {anchor_date.isoformat()} first, then retry render-from-db"
        )
        raise typer.Exit(code=2)

    plan_path_value = _default_phase1_schedule_plan(job_value, None)
    result = _phase1_schedule_run_once(
        settings=settings,
        shops=target_shops,
        job=job_value,
        anchor_date=anchor_date,
        transport="none",
        allow_network=False,
        token_mode="passive",
        plan_path=Path(plan_path_value),
        mapping_path=Path("collaboration/mappings/ads_mapping.yaml"),
        fixtures_dir=None,
        save_failure_artifacts=False,
        send_discord=True,
        discord_attach_report_html=attach_report_html_effective,
        discord_attach_report_zip=attach_report_zip_effective,
        discord_attach_report_md=attach_report_md_effective,
    )

    totals = result.get("totals") or {}
    per_shop = result.get("per_shop") or {}
    report_rows: list[dict[str, object]] = []
    for shop_key in sorted(per_shop.keys()):
        row = per_shop.get(shop_key) or {}
        report_path = row.get("report_path")
        if report_path:
            print(f"report_path shop={shop_key} path={report_path}")
            report_rows.append({"shop": shop_key, "job": job_value, "path": str(report_path)})
        if row.get("error"):
            print(f"shop_error shop={shop_key} error={row.get('error')}")

    print(
        "total "
        f"calls_ok={totals.get('calls_ok', 0)} calls_fail={totals.get('calls_fail', 0)} "
        f"campaigns={totals.get('campaigns', 0)} daily={totals.get('daily', 0)} "
        f"snapshots={totals.get('snapshots', 0)}"
    )

    if bundle_out:
        bundle_path = Path(bundle_out)
        if not bundle_path.is_absolute():
            bundle_path = (Path.cwd() / bundle_path).resolve()
        bundle = _build_phase1_render_from_db_bundle(
            bundle_path=bundle_path, report_paths=report_rows
        )
        print(f"bundle_path={bundle.get('path')}")
        print(f"bundle_files={bundle.get('files')}")
        print(f"bundle_size={bundle.get('size')}")

    ok = int(result.get("ok") or 0)
    print(f"render_from_db_ok={ok}")
    if not ok:
        failures = result.get("failures") or {}
        if failures:
            keys = ",".join(sorted([str(k) for k in failures.keys()]))
            print(f"failures shops={keys}")
        raise typer.Exit(code=1)


@ops_phase1_reports_app.command("reconcile")
def ops_phase1_reports_reconcile(
    shop: str = typer.Option(..., "--shop", help="Shop key"),
    kind: str = typer.Option(..., "--kind", help="final | midday"),
    date_value: str = typer.Option(..., "--date", help="Report date YYYY-MM-DD"),
    db: str | None = typer.Option(
        None,
        "--db",
        help="SQLite DB path override (optional)",
    ),
    reports_dir: str | None = typer.Option(
        None,
        "--reports-dir",
        help="Reports directory override (optional)",
    ),
    artifacts_dir: str = typer.Option(
        "collaboration/tmp/task_141_reconcile",
        "--artifacts-dir",
        help="Output directory for reconcile md/json",
    ),
    raw_artifacts_root: str = typer.Option(
        "collaboration/artifacts",
        "--raw-artifacts-root",
        help="Root directory to scan raw JSON artifacts",
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    kind_value = kind.strip().lower()
    if kind_value not in {"final", "midday"}:
        raise typer.BadParameter("kind must be final or midday")
    try:
        report_date = date.fromisoformat(date_value)
    except ValueError as exc:
        raise typer.BadParameter("date must be YYYY-MM-DD") from exc

    if db:
        db_path = Path(db)
        if not db_path.is_absolute():
            db_path = (Path.cwd() / db_path).resolve()
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
        print(f"database_override={db_path}")
    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir
        print(f"reports_dir_override={reports_dir}")
    if db or reports_dir:
        get_settings.cache_clear()

    settings = get_settings()
    reports_root = Path(reports_dir or settings.reports_dir)
    if not reports_root.is_absolute():
        reports_root = (Path.cwd() / reports_root).resolve()
    artifacts_root = Path(artifacts_dir)
    if not artifacts_root.is_absolute():
        artifacts_root = (Path.cwd() / artifacts_root).resolve()
    raw_root = Path(raw_artifacts_root)
    if not raw_root.is_absolute():
        raw_root = (Path.cwd() / raw_root).resolve()

    init_db()
    with SessionLocal() as session:
        result = run_report_reconcile(
            session=session,
            shop_key=shop,
            kind=kind_value,
            report_date=report_date,
            reports_dir=reports_root,
            artifacts_dir=artifacts_root,
            raw_artifacts_root=raw_root,
        )

    payload = result.payload
    print(
        "reconcile_done "
        f"shop={shop} kind={kind_value} date={report_date.isoformat()} "
        f"report_exists={payload.get('report_exists', 0)} "
        f"raw_rows={payload.get('raw_source_row_count', 0)} "
        f"rendered_parse_mode={payload.get('rendered_parse_mode', '-')}"
    )
    print(f"reconcile_json={result.json_path}")
    print(f"reconcile_md={result.md_path}")
    print(f"reconcile_root_cause={payload.get('root_cause_summary', '-')}")


@ops_phase1_reports_app.command("doctor")
def ops_phase1_reports_doctor(
    path: str = typer.Option(..., "--path", help="HTML report path"),
) -> None:
    report_path = Path(path)
    if not report_path.exists():
        raise typer.BadParameter(f"path not found: {report_path}")
    diag = _inspect_html_report(report_path)
    size = report_path.stat().st_size if report_path.exists() else 0
    print(
        "report_doctor "
        f"path={report_path.resolve()} size={size} "
        f"title_ok={diag.get('title_ok', 0)} tables={diag.get('tables', 0)} "
        f"scripts={diag.get('scripts', 0)} text_len={diag.get('text_len', 0)} "
        f"style_tags={diag.get('style_tags', 0)} link_tags={diag.get('link_tags', 0)} "
        f"meta_charset_ok={diag.get('meta_charset_ok', 0)}"
    )
    if int(diag.get("scripts", 0)) > 0:
        print("report_doctor_warning=1 reason=has_script_tags")
    if int(diag.get("meta_charset_ok", 0)) == 0:
        print("report_doctor_warning=1 reason=missing_meta_charset")
    if int(diag.get("text_len", 0)) <= 0:
        print("report_doctor_warning=1 reason=empty_text")
    ok = int(diag.get("meta_charset_ok", 0)) == 1 and int(diag.get("text_len", 0)) > 0
    print(f"report_doctor_ok={1 if ok else 0}")
    if not ok:
        raise typer.Exit(code=1)


@ops_phase1_reports_app.command("find-nonzero-day")
def ops_phase1_reports_find_nonzero_day(
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys"
    ),
    lookback_days: int = typer.Option(14, "--lookback-days", help="Lookback window"),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    if lookback_days < 1:
        raise typer.BadParameter("lookback-days must be >= 1")
    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)

    settings = get_settings()
    tz = resolve_timezone(settings.timezone)
    end_day = datetime.now(tz).date()
    start_day = end_day - timedelta(days=lookback_days - 1)

    print(
        "nonzero_day_scan_start "
        f"shops={','.join([shop.shop_key for shop in target_shops])} "
        f"start={start_day.isoformat()} end={end_day.isoformat()} "
        f"lookback_days={lookback_days}"
    )

    init_db()
    session = SessionLocal()
    found = 0
    try:
        for shop_cfg in target_shops:
            snapshot_rows = (
                session.query(
                    func.date(AdsCampaignSnapshot.ts).label("d"),
                    func.coalesce(func.sum(AdsCampaignSnapshot.spend_today), 0).label("spend"),
                )
                .filter(
                    AdsCampaignSnapshot.shop_key == shop_cfg.shop_key,
                    func.date(AdsCampaignSnapshot.ts) >= start_day.isoformat(),
                    func.date(AdsCampaignSnapshot.ts) <= end_day.isoformat(),
                )
                .group_by(func.date(AdsCampaignSnapshot.ts))
                .order_by(func.date(AdsCampaignSnapshot.ts).desc())
                .all()
            )

            chosen_day: date | None = None
            chosen_spend = 0.0
            source = "-"
            for row in snapshot_rows:
                row_day = row[0]
                if not row_day:
                    continue
                day_value = date.fromisoformat(str(row_day))
                snapshot_spend = float(row[1] or 0)
                if snapshot_spend <= 0:
                    continue
                daily_spend = (
                    session.query(func.coalesce(func.sum(AdsCampaignDaily.spend), 0))
                    .filter(
                        AdsCampaignDaily.shop_key == shop_cfg.shop_key,
                        AdsCampaignDaily.date == day_value,
                    )
                    .scalar()
                )
                daily_spend_value = float(daily_spend or 0)
                if daily_spend_value > 0:
                    chosen_day = day_value
                    chosen_spend = daily_spend_value
                    source = "daily"
                    break
                if chosen_day is None:
                    chosen_day = day_value
                    chosen_spend = snapshot_spend
                    source = "snapshot"

            if chosen_day is None:
                print(
                    f"nonzero_day_found=0 shop={shop_cfg.shop_key} "
                    "reason=no_positive_snapshot_in_window"
                )
                continue

            found += 1
            print(
                f"nonzero_day_found shop={shop_cfg.shop_key} date={chosen_day.isoformat()} "
                f"spend={chosen_spend:.2f} source={source}"
            )
            print(
                f"daily_final_anchor_date shop={shop_cfg.shop_key} "
                f"date={(chosen_day + timedelta(days=1)).isoformat()}"
            )
    finally:
        session.close()

    print(f"nonzero_day_scan_ok={1 if found > 0 else 0} found={found}")
    if found == 0:
        raise typer.Exit(code=1)


@ops_phase1_export_app.command("monthly")
def ops_phase1_export_monthly(
    month: str = typer.Option(..., "--month", help="YYYY-MM"),
    db: str = typer.Option(..., "--db", help="SQLite DB path"),
    shops: str = typer.Option(
        "samord,minmin", "--shops", help="Comma-separated shop keys (Phase1 only)"
    ),
    out_dir: str = typer.Option(
        "collaboration/exports", "--out-dir", help="Export output directory"
    ),
    env_file: str | None = typer.Option(None, "--env-file", help="Env file path"),
) -> None:
    _maybe_load_env_file(env_file)
    month_start, month_end_exclusive = _parse_month_range(month)

    db_path = Path(db)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    if not db_path.exists():
        raise typer.BadParameter(f"db not found: {db_path}")

    shops_cfg = _load_shops_or_exit()
    target_shops = _select_shops(shops_cfg, shops)
    if not target_shops:
        print("no enabled shops")
        raise typer.Exit(code=1)
    _ensure_phase1_shops_only(shops, target_shops)
    _ensure_shop_ids(target_shops)
    shop_label_map = {shop.shop_key: shop.label for shop in target_shops}
    shop_keys = [shop.shop_key for shop in target_shops]

    out_path = Path(out_dir)
    if not out_path.is_absolute():
        out_path = (Path.cwd() / out_path).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    previous_db_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    get_settings.cache_clear()
    init_db()

    session = SessionLocal()
    try:
        shop_daily_rows = _build_monthly_shop_daily_rows(
            session=session,
            target_shops=target_shops,
            month_start=month_start,
            month_end_exclusive=month_end_exclusive,
        )
        latest_snapshot_rows, lifecycle_rows = _build_monthly_snapshot_export_rows(
            session=session,
            shop_keys=shop_keys,
            shop_label_map=shop_label_map,
            month_start=month_start,
            month_end_exclusive=month_end_exclusive,
        )
    finally:
        session.close()
        if previous_db_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_db_url
        get_settings.cache_clear()

    month_id = month_start.strftime("%Y-%m")
    shop_daily_path = out_path / f"shop_daily_{month_id}.csv"
    shop_daily_sparse_path = out_path / f"shop_daily_sparse_{month_id}.csv"
    shop_monthly_summary_path = out_path / f"shop_monthly_summary_{month_id}.csv"
    snapshot_daily_path = out_path / f"campaign_snapshot_daily_latest_{month_id}.csv"
    lifecycle_path = out_path / f"campaign_lifecycle_{month_id}.csv"

    shop_daily_headers = [
        "date",
        "shop_key",
        "shop_label",
        "spend",
        "impr",
        "clicks",
        "ctr",
        "cpc",
        "orders",
        "gmv",
        "roas",
        "budget_source",
        "budget_est",
        "remaining_est",
        "util_pct",
        "cvr",
    ]
    shop_monthly_summary_headers = [
        "month",
        "shop_key",
        "shop_label",
        "total_spend",
        "total_gmv",
        "total_orders",
        "roas",
        "avg_cpc",
        "avg_ctr",
        "avg_cvr",
    ]
    snapshot_daily_headers = [
        "day",
        "captured_at",
        "shop_key",
        "shop_label",
        "campaign_id",
        "campaign_name",
        "status",
        "budget",
        "spend",
        "remaining",
        "currency",
    ]
    lifecycle_headers = [
        "shop_key",
        "shop_label",
        "campaign_id",
        "campaign_name",
        "first_seen_at",
        "last_seen_at",
        "last_status",
        "last_budget",
        "last_spend",
        "last_remaining",
    ]

    shop_daily_sparse_rows = _build_monthly_shop_daily_sparse_rows(shop_daily_rows)
    shop_monthly_summary_rows = _build_monthly_shop_monthly_summary_rows(
        month_start=month_start,
        shop_daily_rows=shop_daily_rows,
    )

    _write_csv_file(shop_daily_path, shop_daily_headers, shop_daily_rows)
    _write_csv_file(shop_daily_sparse_path, shop_daily_headers, shop_daily_sparse_rows)
    _write_csv_file(
        shop_monthly_summary_path,
        shop_monthly_summary_headers,
        shop_monthly_summary_rows,
    )
    _write_csv_file(snapshot_daily_path, snapshot_daily_headers, latest_snapshot_rows)
    _write_csv_file(lifecycle_path, lifecycle_headers, lifecycle_rows)

    print(
        f"monthly_export_written file={shop_daily_path} rows={len(shop_daily_rows)}"
    )
    print(
        f"monthly_export_written file={shop_daily_sparse_path} rows={len(shop_daily_sparse_rows)}"
    )
    print(
        "monthly_export_written "
        f"file={shop_monthly_summary_path} rows={len(shop_monthly_summary_rows)}"
    )
    print(
        f"monthly_export_written file={snapshot_daily_path} rows={len(latest_snapshot_rows)}"
    )
    print(
        f"monthly_export_written file={lifecycle_path} rows={len(lifecycle_rows)}"
    )
    print(
        "monthly_export_ok=1 "
        f"month={month_id} shops={','.join(shop_keys)} out_dir={out_path}"
    )


@shopee_app.command("auth-url")
def shopee_auth_url(
    shop: str = typer.Option(..., help="Shop key"),
    redirect: str | None = typer.Option(None, help="Redirect URL"),
    timestamp: int | None = typer.Option(None, help="Unix timestamp"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    _get_shop_or_exit(shop)
    redirect_url = redirect or settings.shopee_redirect_url
    if not redirect_url:
        raise typer.BadParameter("redirect URL is required (set SHOPEE_REDIRECT_URL)")
    ts = int(timestamp or datetime.now().timestamp())
    url = build_auth_partner_url(
        settings.shopee_partner_id,
        settings.shopee_partner_key,
        redirect_url,
        ts,
        settings.shopee_api_host,
    )
    print(url)


@shopee_app.command("exchange-code")
def shopee_exchange_code(
    shop: str = typer.Option(..., help="Shop key"),
    code: str = typer.Option(..., help="Auth code"),
    timestamp: int | None = typer.Option(None, help="Unix timestamp"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")
    ts = int(timestamp or datetime.now().timestamp())
    client = _build_shopee_client(settings)

    token = exchange_code_for_token(
        client,
        settings.shopee_partner_id,
        settings.shopee_partner_key,
        shop_cfg.shopee_shop_id,
        code,
        ts,
    )

    init_db()
    session = SessionLocal()
    try:
        upsert_token(
            session,
            shop_cfg.shop_key,
            token.shop_id,
            token.access_token,
            token.refresh_token,
            token.access_expires_at,
        )
        session.commit()
    finally:
        session.close()

    print(
        f"shop_key={shop_cfg.shop_key} shop_id={token.shop_id} access_expires_at={token.access_expires_at.isoformat()}"
    )


@shopee_app.command("refresh-token")
def shopee_refresh_token(
    shop: str = typer.Option(..., help="Shop key"),
    timestamp: int | None = typer.Option(None, help="Unix timestamp"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")
    ts = int(timestamp or datetime.now().timestamp())
    client = _build_shopee_client(settings)

    init_db()
    session = SessionLocal()
    try:
        token = get_token(session, shop_cfg.shop_key)
        if token is None:
            print(f"no token found for shop_key={shop_cfg.shop_key}")
            return
        refreshed = refresh_access_token(
            client,
            settings.shopee_partner_id,
            settings.shopee_partner_key,
            shop_cfg.shopee_shop_id,
            token.refresh_token,
            ts,
        )
        upsert_token(
            session,
            shop_cfg.shop_key,
            refreshed.shop_id,
            refreshed.access_token,
            refreshed.refresh_token,
            refreshed.access_expires_at,
        )
        session.commit()
    finally:
        session.close()

    print(
        f"shop_key={shop_cfg.shop_key} shop_id={refreshed.shop_id} access_expires_at={refreshed.access_expires_at.isoformat()}"
    )


@shopee_app.command("refresh")
def shopee_refresh_alias(
    shop: str = typer.Option(..., help="Shop key"),
    timestamp: int | None = typer.Option(None, help="Unix timestamp"),
) -> None:
    shopee_refresh_token(shop, timestamp)


@shopee_app.command("call")
def shopee_call(
    shop: str = typer.Option(..., help="Shop key"),
    method: str = typer.Option(..., help="GET or POST"),
    path: str = typer.Option(..., help="API path, e.g. /api/v2/shop/get_shop_info"),
    params: list[str] = typer.Option([], "--params", help="Query params key=value"),
    json_body: str | None = typer.Option(None, "--json", help="JSON string or @file"),
    save: bool = typer.Option(
        False, "--save", help="Save response JSON to default path"
    ),
    save_path: str | None = typer.Option(
        None, "--save-path", help="Save response JSON to custom path"
    ),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON"),
    no_print: bool = typer.Option(False, "--no-print", help="Do not print response"),
    no_fail: bool = typer.Option(False, "--no-fail", help="Do not fail on API error"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")

    method_upper = method.upper()
    if method_upper not in {"GET", "POST"}:
        raise typer.BadParameter("method must be GET or POST")

    query_params = _parse_params(params)
    body = None
    if json_body:
        body = _load_json_body(json_body)

    client = _build_shopee_client(settings)

    init_db()
    session = SessionLocal()
    try:
        token = get_token(session, shop_cfg.shop_key)
        if token is None:
            raise RuntimeError("no token found; run shopee exchange-code first")

        if needs_refresh(token.access_token_expires_at):
            refreshed = refresh_access_token(
                client,
                settings.shopee_partner_id,
                settings.shopee_partner_key,
                shop_cfg.shopee_shop_id,
                token.refresh_token,
                int(datetime.now().timestamp()),
            )
            upsert_token(
                session,
                shop_cfg.shop_key,
                refreshed.shop_id,
                refreshed.access_token,
                refreshed.refresh_token,
                refreshed.access_expires_at,
            )
            session.commit()
            token = get_token(session, shop_cfg.shop_key)

        response = client.request(
            method_upper,
            path,
            shop_id=shop_cfg.shopee_shop_id,
            access_token=token.access_token,
            params=query_params or None,
            json=body,
        )
        if isinstance(response, dict):
            error_value = response.get("error")
            if error_value not in (None, 0):
                message = response.get("message") or response.get("msg") or "-"
                raise RuntimeError(f"Shopee API error {error_value}: {message}")
    except Exception as exc:
        if no_fail:
            print(f"error={redact_text(str(exc))}")
            return
        raise
    finally:
        session.close()

    redacted = redact_secrets(response)

    output_path: Path | None = None
    if save_path:
        output_path = Path(save_path)
    elif save:
        output_path = _resolve_save_path("__DEFAULT__", shop_cfg.shop_key, path)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(output_path, redacted, pretty=pretty)
        root = (Path.cwd() / "collaboration" / "artifacts").resolve()
        try:
            output_path.resolve().relative_to(root)
        except ValueError:
            print(f"warning: saved outside collaboration/artifacts -> {output_path}")
        else:
            print(f"saved={output_path}")

    if no_print:
        return

    if pretty:
        print(_dump_json(redacted, pretty=True))
    else:
        print(_dump_json(redacted, pretty=False))


@shopee_app.command("run-plan")
def shopee_run_plan(
    shop: str = typer.Option(..., help="Shop key"),
    plan: str = typer.Option(..., help="Plan YAML path"),
    vars: list[str] = typer.Option([], "--vars", help="Template vars key=value"),
    save_root: str | None = typer.Option(
        None, "--save-root", help="Override artifact root dir"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview calls only"),
    no_print: bool = typer.Option(False, "--no-print", help="Do not print JSON payload"),
    continue_on_error: bool = typer.Option(
        False, "--continue-on-error", help="Continue remaining calls on failure"
    ),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")

    plan_def = load_plan(Path(plan))
    vars_map = _parse_vars(vars)
    summary = run_plan_for_shops(
        [shop_cfg],
        plan_def,
        settings,
        vars_map,
        save_root,
        no_print,
        continue_on_error,
        dry_run,
        plan,
        client_factory=_build_shopee_client,
    )
    if summary["failed"] and not continue_on_error:
        raise typer.Exit(code=1)


@shopee_app.command("run-plan-all")
def shopee_run_plan_all(
    plan: str = typer.Option(..., help="Plan YAML path"),
    vars: list[str] = typer.Option([], "--vars", help="Template vars key=value"),
    save_root: str | None = typer.Option(
        None, "--save-root", help="Override artifact root dir"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Comma-separated shop keys"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview calls only"),
    no_print: bool = typer.Option(False, "--no-print", help="Do not print JSON payload"),
    continue_on_error: bool = typer.Option(
        False, "--continue-on-error", help="Continue remaining calls on failure"
    ),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shops = _load_shops_or_exit()
    target_shops = _select_shops(shops, only_shops)
    if not target_shops:
        print("no enabled shops")
        return

    plan_def = load_plan(Path(plan))
    vars_map = _parse_vars(vars)
    _ensure_shop_ids(target_shops)
    summary = run_plan_for_shops(
        target_shops,
        plan_def,
        settings,
        vars_map,
        save_root,
        no_print,
        continue_on_error,
        dry_run,
        plan,
        client_factory=_build_shopee_client,
    )
    if summary["failed"] and not continue_on_error:
        raise typer.Exit(code=1)


@shopee_app.command("ping")
def shopee_ping(
    shop: str = typer.Option(..., help="Shop key"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shop_cfg = _get_shop_or_exit(shop)
    if shop_cfg.shopee_shop_id is None:
        raise typer.BadParameter("shopee_shop_id missing in shops config")
    client = _build_shopee_client(settings)

    init_db()
    session = SessionLocal()
    try:
        session.add(
            EventLog(
                level="INFO",
                message="shopee_ping_start",
                meta_json=_safe_json(
                    {"shop_key": shop_cfg.shop_key, "shop_id": shop_cfg.shopee_shop_id}
                ),
            )
        )
        session.commit()

        token = get_token(session, shop_cfg.shop_key)
        if token is None:
            raise RuntimeError("no token found; run shopee exchange-code first")

        if needs_refresh(token.access_token_expires_at):
            refreshed = refresh_access_token(
                client,
                settings.shopee_partner_id,
                settings.shopee_partner_key,
                shop_cfg.shopee_shop_id,
                token.refresh_token,
                int(datetime.now().timestamp()),
            )
            upsert_token(
                session,
                shop_cfg.shop_key,
                refreshed.shop_id,
                refreshed.access_token,
                refreshed.refresh_token,
                refreshed.access_expires_at,
            )
            session.commit()
            token = get_token(session, shop_cfg.shop_key)

        response = client.request(
            "GET",
            "/api/v2/shop/get_shop_info",
            shop_id=shop_cfg.shopee_shop_id,
            access_token=token.access_token,
        )
        data = response.get("response") or response
        shop_name = data.get("shop_name") or "-"

        session.add(
            EventLog(
                level="INFO",
                message="shopee_ping_end",
                meta_json=_safe_json(
                    {
                        "shop_key": shop_cfg.shop_key,
                        "shop_id": shop_cfg.shopee_shop_id,
                        "shop_name": shop_name,
                    }
                ),
            )
        )
        session.commit()
    except Exception as exc:
        session.add(
            EventLog(
                level="ERROR",
                message="shopee_ping_error",
                meta_json=_safe_json(
                    {
                        "shop_key": shop_cfg.shop_key,
                        "shop_id": shop_cfg.shopee_shop_id,
                        "error": str(exc),
                    }
                ),
            )
        )
        session.commit()
        raise
    finally:
        session.close()

    print(
        f"shop_key={shop_cfg.shop_key} shop_id={shop_cfg.shopee_shop_id} shop_name={shop_name}"
    )


@shopee_app.command("probe-analyze")
def shopee_probe_analyze(
    date_value: str = typer.Option(..., "--date", help="YYYYMMDD"),
    save_root: str | None = typer.Option(
        None, "--save-root", help="Root artifacts dir"
    ),
    out_dir: str | None = typer.Option(
        None, "--out-dir", help="Output directory for summaries"
    ),
    format: str = typer.Option(
        "both", "--format", help="both | md | csv"
    ),
    include_schema_hints: bool = typer.Option(
        False, "--include-schema-hints", help="Include schema hints in markdown"
    ),
    send_discord: bool = typer.Option(
        False, "--send-discord", help="Post a short summary to Discord"
    ),
    channel: str = typer.Option(
        "report", "--channel", help="report | alert"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Comma-separated shop keys"
    ),
) -> None:
    root = (
        Path(save_root)
        if save_root
        else (Path("collaboration") / "artifacts" / "shopee_api")
    )
    only = _parse_only_shops(only_shops)
    out_root = Path(out_dir) if out_dir else (Path("collaboration") / "artifacts")
    format_value = format.lower().strip()
    if format_value not in {"both", "md", "csv"}:
        raise typer.BadParameter("format must be one of: both, md, csv")
    channel_value = channel.lower().strip()
    if channel_value not in {"report", "alert"}:
        raise typer.BadParameter("channel must be report or alert")
    discord_channel = "alerts" if channel_value == "alert" else "report"
    records = analyze_artifacts(root, date_value, only)
    md_path = out_root / f"probe_summary_{date_value}.md"
    csv_path = out_root / f"probe_summary_{date_value}.csv"
    if format_value in {"both", "md"}:
        write_markdown_summary(
            records,
            md_path,
            date_value=date_value,
            save_root=root,
            include_schema_hints=include_schema_hints,
        )
        print(f"saved_markdown={md_path}")
    if format_value in {"both", "csv"}:
        write_csv_summary(records, csv_path)
        print(f"saved_csv={csv_path}")
    if send_discord:
        settings = get_settings()
        summary_name = md_path.name if format_value in {"both", "md"} else csv_path.name
        summary_ref = build_summary_ref(settings, out_root, summary_name)
        shops_list = only if only else sorted({record.shop_key for record in records})
        message = build_discord_summary(
            records,
            date_value,
            root,
            shops_list,
            summary_ref,
        )
        send(discord_channel, message)


@shopee_app.command("probe-list")
def shopee_probe_list(
    date_value: str = typer.Option(..., "--date", help="YYYYMMDD"),
    save_root: str | None = typer.Option(
        None, "--save-root", help="Root artifacts dir"
    ),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Comma-separated shop keys"
    ),
) -> None:
    root = (
        Path(save_root)
        if save_root
        else (Path("collaboration") / "artifacts" / "shopee_api")
    )
    only = _parse_only_shops(only_shops)
    records = analyze_artifacts(root, date_value, only)
    for line in render_console_list(records):
        print(line)


@shopee_app.command("probe-suite")
def shopee_probe_suite(
    date_value: str = typer.Option(..., "--date", help="YYYYMMDD"),
    plan: str = typer.Option(..., "--plan", help="Plan YAML path"),
    only_shops: str | None = typer.Option(
        None, "--only-shops", help="Comma-separated shop keys"
    ),
    save_root: str | None = typer.Option(
        None, "--save-root", help="Root artifacts dir"
    ),
    out_dir: str | None = typer.Option(
        None, "--out-dir", help="Output directory for summaries"
    ),
    vars: list[str] = typer.Option([], "--vars", help="Template vars key=value"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Plan dry-run only"),
    send_discord: bool = typer.Option(
        False, "--send-discord", help="Post a short summary to Discord"
    ),
    channel: str = typer.Option("report", "--channel", help="report | alert"),
) -> None:
    settings = get_settings()
    _require_shopee_settings(settings)
    shops = _load_shops_or_exit()
    target_shops = _select_shops(shops, only_shops)
    if not target_shops:
        print("no enabled shops")
        return
    _ensure_shop_ids(target_shops)

    root = (
        Path(save_root)
        if save_root
        else (Path("collaboration") / "artifacts" / "shopee_api")
    )
    if out_dir:
        out_root = Path(out_dir)
    else:
        out_root = (
            Path("collaboration")
            / "outputs"
            / "probe_summaries"
            / date_value
        )

    vars_map = _parse_vars(vars)
    channel_value = channel.lower().strip()
    if channel_value not in {"report", "alert"}:
        raise typer.BadParameter("channel must be report or alert")

    run_probe_suite(
        settings=settings,
        shops=target_shops,
        plan_path=Path(plan),
        date_value=date_value,
        user_vars=vars_map,
        save_root=root,
        out_dir=out_root,
        dry_run=dry_run,
        send_discord=send_discord,
        channel=channel_value,
        client_factory=_build_shopee_client,
    )


def _build_report_url(
    shop_key: str, target_date: date, kind: str, token: str | None
) -> str:
    relative_path = f"reports/{shop_key}/daily/{target_date.isoformat()}_{kind}.html"
    shared_url, _ = _discord_build_report_url(relative_path)
    if shared_url:
        return _append_token_query(shared_url, token)

    settings = get_settings()
    base = (settings.report_base_url or f"http://{settings.web_host}:{settings.web_port}").rstrip("/")
    filename = f"{target_date.isoformat()}_{kind}.html"
    if base.endswith("/reports"):
        url = f"{base}/{shop_key}/daily/{filename}"
    else:
        url = f"{base}/reports/{shop_key}/daily/{filename}"
    return _append_token_query(url, token)


def _build_weekly_report_url(shop_key: str, week_id_value: str, token: str | None) -> str:
    relative_path = f"reports/{shop_key}/weekly/{week_id_value}.html"
    shared_url, _ = _discord_build_report_url(relative_path)
    if shared_url:
        return _append_token_query(shared_url, token)

    settings = get_settings()
    base = (settings.report_base_url or f"http://{settings.web_host}:{settings.web_port}").rstrip("/")
    if base.endswith("/reports"):
        url = f"{base}/{shop_key}/weekly/{week_id_value}.html"
    else:
        url = f"{base}/reports/{shop_key}/weekly/{week_id_value}.html"
    return _append_token_query(url, token)


def _append_token_query(url: str, token: str | None) -> str:
    token_value = (token or "").strip()
    if not token_value:
        return url
    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    if not any(key == "token" for key, _ in pairs):
        pairs.append(("token", token_value))
    query = urlencode(pairs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _safe_json(meta: dict) -> str:
    import json

    return json.dumps(meta, ensure_ascii=True, default=str)


def _fmt_decimal(value, places: int = 2) -> str:
    if value is None:
        return "-"
    from decimal import Decimal, ROUND_HALF_UP

    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quant = Decimal("1").scaleb(-places)
    return f"{dec.quantize(quant, rounding=ROUND_HALF_UP)}"


def _require_shopee_settings(settings) -> None:
    if settings.shopee_partner_id is None:
        raise typer.BadParameter("SHOPEE_PARTNER_ID is required")
    if not settings.shopee_partner_key:
        raise typer.BadParameter("SHOPEE_PARTNER_KEY is required")


def _build_shopee_client(settings) -> ShopeeClient:
    return ShopeeClient(
        partner_id=settings.shopee_partner_id,
        partner_key=settings.shopee_partner_key,
        host=settings.shopee_api_host,
    )


def _parse_params(params: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in params:
        if "=" not in item:
            raise typer.BadParameter(f"invalid param '{item}', must be key=value")
        key, value = item.split("=", 1)
        parsed[key] = value
    return parsed


def _parse_vars(vars_list: list[str]) -> dict[str, str]:
    return _parse_params(vars_list)


def _parse_only_shops(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        raise typer.BadParameter("only-shops must include at least one shop key")
    return items


def _parse_month_range(value: str) -> tuple[date, date]:
    text = (value or "").strip()
    try:
        month_start = datetime.strptime(text, "%Y-%m").date()
    except ValueError as exc:
        raise typer.BadParameter("month must be YYYY-MM") from exc
    if month_start.month == 12:
        month_end_exclusive = date(month_start.year + 1, 1, 1)
    else:
        month_end_exclusive = date(month_start.year, month_start.month + 1, 1)
    return month_start, month_end_exclusive


def _iter_days(start_day: date, end_day_exclusive: date):
    day = start_day
    while day < end_day_exclusive:
        yield day
        day += timedelta(days=1)


def _build_monthly_shop_daily_rows(
    *,
    session,
    target_shops: list,
    month_start: date,
    month_end_exclusive: date,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for day in _iter_days(month_start, month_end_exclusive):
        for shop_cfg in target_shops:
            report = aggregate_daily_report(session, shop_cfg.shop_key, day, as_of=None)
            scorecard = report.get("scorecard") if isinstance(report.get("scorecard"), dict) else {}
            scorecard = scorecard if isinstance(scorecard, dict) else {}
            rows.append(
                {
                    "date": day.isoformat(),
                    "shop_key": shop_cfg.shop_key,
                    "shop_label": shop_cfg.label,
                    "spend": _csv_decimal(scorecard.get("spend")),
                    "impr": _csv_int(scorecard.get("impressions")),
                    "clicks": _csv_int(scorecard.get("clicks")),
                    "ctr": _csv_percent_number(scorecard.get("ctr")),
                    "cpc": _csv_decimal(scorecard.get("cpc")),
                    "orders": _csv_int(scorecard.get("orders")),
                    "gmv": _csv_decimal(scorecard.get("gmv")),
                    "roas": _csv_ratio(scorecard.get("roas")),
                    "budget_source": str(report.get("budget_source") or "none"),
                    "budget_est": _csv_decimal(scorecard.get("budget_est")),
                    "remaining_est": _csv_decimal(scorecard.get("remaining")),
                    "util_pct": _csv_percent_number(scorecard.get("util_pct")),
                    "cvr": _csv_percent_number(scorecard.get("cvr")),
                }
            )
    return rows


def _build_monthly_shop_daily_sparse_rows(
    shop_daily_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    sparse_rows: list[dict[str, str]] = []
    for row in shop_daily_rows:
        spend = _to_decimal(row.get("spend")) or Decimal("0")
        gmv = _to_decimal(row.get("gmv")) or Decimal("0")
        orders = _csv_to_int(row.get("orders"))
        impressions = _csv_to_int(row.get("impr"))
        clicks = _csv_to_int(row.get("clicks"))
        if spend > 0 or gmv > 0 or orders > 0 or impressions > 0 or clicks > 0:
            sparse_rows.append(row)
    return sparse_rows


def _build_monthly_shop_monthly_summary_rows(
    *,
    month_start: date,
    shop_daily_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    per_shop: dict[tuple[str, str], dict[str, Decimal | int]] = {}
    for row in shop_daily_rows:
        shop_key = str(row.get("shop_key") or "").strip()
        shop_label = str(row.get("shop_label") or "").strip()
        if not shop_key:
            continue
        key = (shop_key, shop_label)
        bucket = per_shop.setdefault(
            key,
            {
                "spend": Decimal("0"),
                "gmv": Decimal("0"),
                "orders": 0,
                "clicks": 0,
                "impressions": 0,
            },
        )
        bucket["spend"] = (bucket["spend"] or Decimal("0")) + (
            _to_decimal(row.get("spend")) or Decimal("0")
        )
        bucket["gmv"] = (bucket["gmv"] or Decimal("0")) + (
            _to_decimal(row.get("gmv")) or Decimal("0")
        )
        bucket["orders"] = int(bucket["orders"] or 0) + _csv_to_int(row.get("orders"))
        bucket["clicks"] = int(bucket["clicks"] or 0) + _csv_to_int(row.get("clicks"))
        bucket["impressions"] = int(bucket["impressions"] or 0) + _csv_to_int(
            row.get("impr")
        )

    out: list[dict[str, str]] = []
    for (shop_key, shop_label), bucket in sorted(per_shop.items(), key=lambda item: item[0][0]):
        total_spend = _to_decimal(bucket.get("spend")) or Decimal("0")
        total_gmv = _to_decimal(bucket.get("gmv")) or Decimal("0")
        total_orders = int(bucket.get("orders") or 0)
        total_clicks = int(bucket.get("clicks") or 0)
        total_impressions = int(bucket.get("impressions") or 0)
        roas = (total_gmv / total_spend) if total_spend > 0 else None
        avg_cpc = (total_spend / Decimal(total_clicks)) if total_clicks > 0 else None
        avg_ctr = (
            Decimal(total_clicks) / Decimal(total_impressions)
            if total_impressions > 0
            else None
        )
        avg_cvr = (
            Decimal(total_orders) / Decimal(total_clicks) if total_clicks > 0 else None
        )

        out.append(
            {
                "month": month_start.strftime("%Y-%m"),
                "shop_key": shop_key,
                "shop_label": shop_label,
                "total_spend": _csv_decimal(total_spend),
                "total_gmv": _csv_decimal(total_gmv),
                "total_orders": str(total_orders),
                "roas": _csv_ratio(roas),
                "avg_cpc": _csv_decimal(avg_cpc),
                "avg_ctr": _csv_percent_number(avg_ctr),
                "avg_cvr": _csv_percent_number(avg_cvr),
            }
        )
    return out


def _build_monthly_snapshot_export_rows(
    *,
    session,
    shop_keys: list[str],
    shop_label_map: dict[str, str],
    month_start: date,
    month_end_exclusive: date,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    records = (
        session.query(
            AdsCampaignSnapshot.shop_key,
            AdsCampaignSnapshot.campaign_id,
            AdsCampaignSnapshot.ts,
            AdsCampaignSnapshot.spend_today,
            AdsCampaign.campaign_name,
            AdsCampaign.status,
            AdsCampaign.daily_budget,
        )
        .outerjoin(
            AdsCampaign,
            (AdsCampaign.shop_key == AdsCampaignSnapshot.shop_key)
            & (AdsCampaign.campaign_id == AdsCampaignSnapshot.campaign_id),
        )
        .filter(AdsCampaignSnapshot.shop_key.in_(shop_keys))
        .all()
    )

    latest_by_day: dict[tuple[str, str, str], dict[str, object]] = {}
    lifecycle_by_campaign: dict[tuple[str, str], dict[str, object]] = {}

    for row in records:
        shop_key = str(row[0] or "").strip()
        campaign_id = str(row[1] or "").strip()
        ts_value = _coerce_datetime_value(row[2], tz)
        if not shop_key or not campaign_id or ts_value is None:
            continue
        day_value = ts_value.astimezone(tz).date()
        if not (month_start <= day_value < month_end_exclusive):
            continue

        campaign_name = str(row[4] or "")
        status = str(row[5] or "")
        budget = _to_decimal(row[6])
        spend = _to_decimal(row[3])
        remaining = _max_zero_decimal(budget, spend)

        latest_key = (shop_key, day_value.isoformat(), campaign_id)
        existing_latest = latest_by_day.get(latest_key)
        existing_latest_ts = (
            _coerce_datetime_value(existing_latest.get("ts"), tz)
            if isinstance(existing_latest, dict)
            else None
        )
        if existing_latest is None or existing_latest_ts is None or ts_value > existing_latest_ts:
            latest_by_day[latest_key] = {
                "shop_key": shop_key,
                "shop_label": shop_label_map.get(shop_key, shop_key.upper()),
                "day": day_value.isoformat(),
                "ts": ts_value,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "status": status,
                "budget": budget,
                "spend": spend,
                "remaining": remaining,
            }

        lifecycle_key = (shop_key, campaign_id)
        existing_lifecycle = lifecycle_by_campaign.get(lifecycle_key)
        if existing_lifecycle is None:
            lifecycle_by_campaign[lifecycle_key] = {
                "shop_key": shop_key,
                "shop_label": shop_label_map.get(shop_key, shop_key.upper()),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "first_seen_at": ts_value,
                "last_seen_at": ts_value,
                "last_status": status,
                "last_budget": budget,
                "last_spend": spend,
                "last_remaining": remaining,
            }
            continue
        first_seen_dt = _coerce_datetime_value(existing_lifecycle.get("first_seen_at"), tz)
        if first_seen_dt is None or ts_value < first_seen_dt:
            existing_lifecycle["first_seen_at"] = ts_value
        last_seen_dt = _coerce_datetime_value(existing_lifecycle.get("last_seen_at"), tz)
        if last_seen_dt is None or ts_value >= last_seen_dt:
            existing_lifecycle["last_seen_at"] = ts_value
            existing_lifecycle["last_status"] = status
            existing_lifecycle["last_budget"] = budget
            existing_lifecycle["last_spend"] = spend
            existing_lifecycle["last_remaining"] = remaining
            if campaign_name:
                existing_lifecycle["campaign_name"] = campaign_name

    latest_rows: list[dict[str, str]] = []
    for row in sorted(
        latest_by_day.values(),
        key=lambda item: (
            str(item.get("day") or ""),
            str(item.get("shop_key") or ""),
            str(item.get("campaign_id") or ""),
        ),
    ):
        latest_rows.append(
            {
                "day": str(row.get("day") or ""),
                "captured_at": _csv_datetime(row.get("ts")),
                "shop_key": str(row.get("shop_key") or ""),
                "shop_label": str(row.get("shop_label") or ""),
                "campaign_id": str(row.get("campaign_id") or ""),
                "campaign_name": str(row.get("campaign_name") or ""),
                "status": str(row.get("status") or ""),
                "budget": _csv_decimal(row.get("budget")),
                "spend": _csv_decimal(row.get("spend")),
                "remaining": _csv_decimal(row.get("remaining")),
                "currency": "",
            }
        )

    lifecycle_rows: list[dict[str, str]] = []
    for row in sorted(
        lifecycle_by_campaign.values(),
        key=lambda item: (str(item.get("shop_key") or ""), str(item.get("campaign_id") or "")),
    ):
        lifecycle_rows.append(
            {
                "shop_key": str(row.get("shop_key") or ""),
                "shop_label": str(row.get("shop_label") or ""),
                "campaign_id": str(row.get("campaign_id") or ""),
                "campaign_name": str(row.get("campaign_name") or ""),
                "first_seen_at": _csv_datetime(row.get("first_seen_at")),
                "last_seen_at": _csv_datetime(row.get("last_seen_at")),
                "last_status": str(row.get("last_status") or ""),
                "last_budget": _csv_decimal(row.get("last_budget")),
                "last_spend": _csv_decimal(row.get("last_spend")),
                "last_remaining": _csv_decimal(row.get("last_remaining")),
            }
        )
    return latest_rows, lifecycle_rows


def _write_csv_file(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _max_zero_decimal(budget: Decimal | None, spend: Decimal | None) -> Decimal | None:
    if budget is None or spend is None:
        return None
    diff = budget - spend
    return diff if diff >= 0 else Decimal("0")


def _coerce_datetime_value(value: object, tz) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt_value = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt_value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=tz)
    return dt_value


def _csv_decimal(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return ""
    return f"{dec.quantize(Decimal('0.01'))}"


def _csv_ratio(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return ""
    return f"{dec.quantize(Decimal('0.01'))}"


def _csv_percent_number(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return ""
    pct = (dec * Decimal("100")).quantize(Decimal("0.01"))
    return f"{pct}"


def _csv_int(value: object) -> str:
    if value is None:
        return ""
    try:
        return str(int(value))
    except Exception:  # noqa: BLE001
        return ""


def _csv_to_int(value: object) -> int:
    if value is None:
        return 0
    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(text)
    except Exception:  # noqa: BLE001
        return 0


def _csv_datetime(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text


def _parse_date_or_today(value: str | None) -> date:
    if value:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise typer.BadParameter("date must be YYYY-MM-DD") from exc
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    return datetime.now(tz).date()


def _parse_required_date(value: str | None) -> date:
    if not value:
        raise typer.BadParameter("date is required (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("date must be YYYY-MM-DD") from exc


def _select_shops(shops, only_shops: str | None):
    if only_shops:
        desired = _parse_only_shops(only_shops) or []
        selected = [shop for shop in shops if shop.shop_key in set(desired)]
        missing = [key for key in desired if key not in {shop.shop_key for shop in shops}]
        if missing:
            raise typer.BadParameter(f"Unknown shop(s): {', '.join(missing)}")
        return selected
    return [shop for shop in shops if shop.enabled]


def _ensure_shop_ids(shops) -> None:
    # Unify shop_id source: env overrides should propagate into shop_cfg so any
    # code path using `shop_cfg.shopee_shop_id` uses the real ID.
    for shop_cfg in shops:
        shop_cfg.shopee_shop_id = _resolve_shop_id(shop_cfg)


def _shop_id_configured(shop_cfg) -> bool:
    env_key = f"SHOPEE_{shop_cfg.shop_key.upper()}_SHOP_ID"
    env_value = os.environ.get(env_key)
    if env_value:
        try:
            int(env_value)
            return True
        except ValueError:
            return False
    return shop_cfg.shopee_shop_id is not None


def _option_is_default(ctx: typer.Context | None, name: str) -> bool:
    if ctx is None:
        return False
    try:
        from click.core import ParameterSource

        return ctx.get_parameter_source(name) == ParameterSource.DEFAULT
    except Exception:
        return False


def _resolve_env_override(
    ctx: typer.Context | None,
    option_name: str,
    env_key: str,
    value: str,
    default: str,
) -> str:
    if _option_is_default(ctx, option_name):
        return os.environ.get(env_key, default)
    return value


def _resolve_bool_option_with_env(
    ctx: typer.Context | None,
    option_name: str,
    *,
    current: bool,
    env_key: str,
    default: bool,
) -> bool:
    if not _option_is_default(ctx, option_name):
        return bool(current)
    raw = os.environ.get(env_key)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes"}


def _resolve_int_option_with_env(
    ctx: typer.Context | None,
    option_name: str,
    *,
    current: int,
    env_key: str,
    default: int,
    minimum: int | None = None,
) -> int:
    value = int(current)
    if _option_is_default(ctx, option_name):
        raw = os.environ.get(env_key, str(default)).strip()
        try:
            value = int(raw)
        except ValueError:
            value = int(default)
    if minimum is not None and value < minimum:
        return int(minimum)
    return value


def _scrub_sensitive_text(text: str) -> str:
    import re

    cleaned = text
    for key in [
        "token",
        "sign",
        "authorization",
        "cookie",
        "secret",
        "partner_key",
        "client_secret",
    ]:
        pattern = re.compile(rf"({re.escape(key)}\s*[:=]\s*)([^\s,;]+)", re.I)
        cleaned = pattern.sub(r"\1***", cleaned)
    return cleaned


def _send_smoke_start(settings, channel: str, date_value: str, shops, live_http: bool) -> None:
    targets = ",".join([shop.shop_key for shop in shops])
    tag = channel.upper()
    text = f"[SMOKE][{tag}] starting smoke date={date_value} shops={targets} live_http={int(live_http)}"
    _try_send_discord(settings, channel, text, optional=True)

    other = "alerts" if channel == "report" else "report"
    other_tag = other.upper()
    other_text = f"[SMOKE][{other_tag}] starting smoke date={date_value} shops={targets} live_http={int(live_http)}"
    _try_send_discord(settings, other, other_text, optional=True)


def _send_smoke_done(settings, result: dict, md_path: str, csv_path: str) -> None:
    ok_value = result.get("ok", 0)
    fail_value = result.get("fail", 0)
    done_text = f"[SMOKE][DONE] ok={ok_value} fail={fail_value} md={md_path} csv={csv_path}"
    _try_send_discord(settings, "report", done_text, optional=True)
    if fail_value:
        fail_text = f"[SMOKE][FAIL] ok={ok_value} fail={fail_value} md={md_path} csv={csv_path}"
        _try_send_discord(settings, "alerts", fail_text, optional=True)


def _send_smoke_error(settings, error_text: str) -> None:
    text = f"[SMOKE][ERROR] {error_text}"
    _try_send_discord(settings, "alerts", text, optional=True)


def _try_send_discord(settings, channel: str, text: str, optional: bool = False) -> None:
    webhook = _get_discord_webhook(settings, channel)
    if not webhook:
        if optional:
            print(f"discord=skipped channel={channel} reason=missing_webhook")
        return
    send(channel, _scrub_sensitive_text(text), webhook_url=webhook)


def _get_discord_webhook(settings, channel: str) -> str | None:
    if channel == "report":
        return settings.discord_webhook_report_url
    if channel == "alerts":
        return settings.discord_webhook_alerts_url
    return None


def _load_fixture_payload(fixtures_dir: Path, call_name: str) -> dict | None:
    if not fixtures_dir.exists():
        return None
    exact = fixtures_dir / f"{call_name}.json"
    if exact.exists():
        return _read_json(exact)
    matches = sorted(fixtures_dir.glob(f"{call_name}*.json"))
    if matches:
        return _read_json(matches[0])
    return None


def _read_json(path: Path) -> dict:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _load_json_body(value: str) -> dict:
    import json
    if value.startswith("@"):
        path = Path(value[1:])
        if not path.exists():
            raise typer.BadParameter(f"json file not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(value)


def _resolve_save_path(
    save_value: str,
    shop_key: str,
    api_path: str,
) -> Path:
    if save_value != "__DEFAULT__":
        return Path(save_value)
    ts = int(datetime.now().timestamp())
    sanitized = api_path.strip("/").replace("/", "_")
    if not sanitized:
        sanitized = "root"
    base = Path("collaboration") / "artifacts" / "shopee_api" / shop_key
    return base / f"{ts}_{sanitized}.json"


def _write_json(path: Path, payload: dict, pretty: bool = False) -> None:
    path.write_text(_dump_json(payload, pretty=pretty), encoding="utf-8")


def _dump_json(payload: dict, pretty: bool = False) -> str:
    import json

    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2)
    return json.dumps(payload, ensure_ascii=True)


if __name__ == "__main__":
    app()

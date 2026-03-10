from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from html.parser import HTMLParser
import os
import sys
import time as pytime
from pathlib import Path
import zipfile
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .ads.alerts import process_alerts
from .ads.campaign_labels import resolve_campaign_display_name
from .ads.incidents import AdsIncident
from .ads.provider_live_plan import ingest_ads_live
from .ads.reporting import (
    aggregate_daily_report,
    build_discord_summary,
    render_daily_html,
    write_report_file,
)
from .ads.weekly_report import (
    build_weekly_discord_message,
    build_weekly_payload,
    get_last_week_range,
    render_weekly_html,
    week_id as weekly_id,
    write_weekly_report_file,
)
from .config import Settings, get_settings, resolve_timezone
from .db import EventLog, SessionLocal, init_db
from .discord_notifier import build_report_url, send
from .ops.alert_dispatch import dispatch_alert_card
from .token_preflight_gate import (
    emit_token_resolved_alerts_with_cooldown,
    emit_token_ttl_alerts_with_cooldown,
    evaluate_token_preflight_gate,
    write_token_preflight_gate_artifacts,
)


@dataclass(frozen=True)
class JobDefinition:
    name: str
    trigger: object


def build_scheduler(settings: Settings, shops, blocking: bool = True):
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    if blocking:
        scheduler = BlockingScheduler(timezone=tz)
    else:
        scheduler = BackgroundScheduler(timezone=tz)
    register_jobs(scheduler, settings, shops)
    return scheduler


def register_jobs(scheduler, settings: Settings, shops) -> None:
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    final_hour, final_minute = _parse_time(settings.daily_final_time)
    midday_hour, midday_minute = _parse_time(settings.daily_midday_time)
    weekly_hour, weekly_minute = _parse_time(settings.weekly_report_time)
    weekly_dow = _parse_dow(settings.weekly_report_dow)

    scheduler.add_job(
        lambda: run_alerts_job(settings, shops),
        IntervalTrigger(minutes=settings.detect_interval_minutes),
        id="ads_alerts_15m",
        name="ads_alerts_15m",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: run_daily_report_job(settings, shops, kind="final"),
        CronTrigger(hour=final_hour, minute=final_minute, timezone=tz),
        id="ads_daily_final_0000",
        name="ads_daily_final_0000",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: run_daily_report_job(settings, shops, kind="midday"),
        CronTrigger(hour=midday_hour, minute=midday_minute, timezone=tz),
        id="ads_daily_midday_1300",
        name="ads_daily_midday_1300",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: run_weekly_report_job(settings, shops),
        CronTrigger(day_of_week=weekly_dow, hour=weekly_hour, minute=weekly_minute, timezone=tz),
        id="ads_weekly_mon_0900",
        name="ads_weekly_mon_0900",
        replace_existing=True,
    )


def run_alerts_job(settings: Settings, shops, now: datetime | None = None, send_discord: bool | None = None) -> None:
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    now_dt = now or datetime.now(tz)
    send_flag = settings.scheduler_send_discord if send_discord is None else send_discord
    alerts_send_flag = _resolve_phase1_alerts_send_discord(send_flag)

    _log_event("INFO", "job_start", {"job": "ads_alerts_15m", "time": now_dt.isoformat()})

    totals: dict[str, int] = {"active": 0, "opened": 0, "updated": 0, "resolved": 0, "notified": 0, "suppressed": 0}
    try:
        skipped_due_to_token, skip_meta = _scheduler_token_preflight_gate(
            job_name="ads_alerts_15m",
            shops=shops,
            send_discord=alerts_send_flag,
        )
        if skipped_due_to_token:
            _log_event(
                "INFO",
                "job_end",
                {
                    "job": "ads_alerts_15m",
                    "time": now_dt.isoformat(),
                    "skipped_due_to_token": 1,
                    **totals,
                },
            )
            return
        transport_value = _resolve_phase1_alerts_transport()
        allow_network = _allow_network_enabled()
        # Guardrail: never hit live HTTP unless explicitly enabled.
        if transport_value == "live" and not allow_network:
            transport_value = "none"
        plan_path, mapping_path, fixtures_dir = _resolve_phase1_alerts_paths(transport_value)
        result = phase1_alerts_run_once(
            settings=settings,
            shops=shops,
            as_of=now_dt,
            transport=transport_value,
            allow_network=allow_network,
            token_mode=_resolve_phase1_alerts_token_mode(),
            plan_path=plan_path,
            mapping_path=mapping_path,
            fixtures_dir=fixtures_dir,
            save_failure_artifacts=_resolve_phase1_alerts_save_failure_artifacts(),
            send_discord=alerts_send_flag,
            notify_resolved=_resolve_phase1_alerts_notify_resolved(),
            cooldown_minutes=settings.alert_cooldown_minutes,
        )
        if skip_meta:
            result["preflight_gate"] = skip_meta.get("preflight_gate")
        totals.update({k: int(v) for k, v in (result.get("alerts_totals") or {}).items()})
        if int(result.get("ok") or 0) != 1:
            _emit_scheduler_failure_alert(
                settings=settings,
                shops=shops,
                job_name="ads_alerts_15m",
                error_summary=_format_failure_summary(result.get("failures")),
                send_discord=alerts_send_flag,
                severity="CRITICAL",
                dedup_cooldown_sec=1800,
            )
    except Exception as exc:
        _emit_scheduler_failure_alert(
            settings=settings,
            shops=shops,
            job_name="ads_alerts_15m",
            error_summary=str(exc),
            send_discord=alerts_send_flag,
            severity="CRITICAL",
            dedup_cooldown_sec=1800,
        )
        _log_event(
            "ERROR",
            "job_error",
            {"job": "ads_alerts_15m", "error": str(exc)},
        )

    _log_event("INFO", "job_end", {"job": "ads_alerts_15m", "time": now_dt.isoformat(), **totals})


def run_daily_report_job(
    settings: Settings,
    shops,
    kind: str,
    now: datetime | None = None,
    send_discord: bool | None = None,
) -> None:
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    now_dt = now or datetime.now(tz)
    send_flag = settings.scheduler_send_discord if send_discord is None else send_discord
    alerts_send_flag = _resolve_phase1_alerts_send_discord(send_flag)
    job_name = "ads_daily_final_0000" if kind == "final" else "ads_daily_midday_1300"

    _log_event("INFO", "job_start", {"job": job_name, "time": now_dt.isoformat()})

    try:
        skipped_due_to_token, _ = _run_report_token_preflight_with_retry(
            job_name=job_name,
            shops=shops,
            send_discord=send_flag,
        )
        if skipped_due_to_token:
            _log_event(
                "INFO",
                "job_end",
                {"job": job_name, "time": now_dt.isoformat(), "skipped_due_to_token": 1},
            )
            return
        transport_value = _resolve_phase1_schedule_transport()
        allow_network = _allow_network_enabled()
        if transport_value == "live" and not allow_network:
            # Guardrail: never hit live HTTP without explicit opt-in.
            transport_value = "none"
        plan_path, mapping_path, fixtures_dir = _resolve_phase1_schedule_paths(
            transport_value, job=f"daily-{kind}"
        )
        result, retry_error = _run_phase1_schedule_with_retry(
            job_name=job_name,
            run_kwargs={
                "settings": settings,
                "shops": shops,
                "job": f"daily-{kind}",
                "anchor_date": now_dt.date(),
                "transport": transport_value,
                "allow_network": allow_network,
                "token_mode": _resolve_phase1_schedule_token_mode(),
                "plan_path": plan_path,
                "mapping_path": mapping_path,
                "fixtures_dir": fixtures_dir,
                "save_failure_artifacts": _resolve_phase1_schedule_save_failure_artifacts(),
                "send_discord": send_flag,
                "discord_attach_report_html": _resolve_discord_attach_report_html(),
                "discord_attach_report_zip": _resolve_discord_attach_report_zip(),
                "discord_attach_report_md": _resolve_discord_attach_report_md(),
            },
        )
        if result is None or int(result.get("ok") or 0) != 1:
            failure_summary = retry_error
            if not failure_summary and isinstance(result, dict):
                failure_summary = _format_failure_summary(result.get("failures"))
            _emit_scheduler_failure_alert(
                settings=settings,
                shops=shops,
                job_name=job_name,
                error_summary=failure_summary or "unknown_daily_report_failure",
                send_discord=alerts_send_flag,
                severity="CRITICAL",
                dedup_cooldown_sec=1800,
            )
        if kind == "final" and isinstance(result, dict) and int(result.get("ok") or 0) == 1:
            _send_daily_incident_digest(
                settings=settings,
                shops=shops,
                result=result,
                send_discord=alerts_send_flag,
            )
    except Exception as exc:
        _emit_scheduler_failure_alert(
            settings=settings,
            shops=shops,
            job_name=job_name,
            error_summary=str(exc),
            send_discord=alerts_send_flag,
            severity="CRITICAL",
            dedup_cooldown_sec=1800,
        )
        _log_event("ERROR", "job_error", {"job": job_name, "error": str(exc)})

    _log_event("INFO", "job_end", {"job": job_name, "time": now_dt.isoformat()})


def run_weekly_report_job(
    settings: Settings,
    shops,
    now: datetime | None = None,
    send_discord: bool | None = None,
) -> None:
    tz = resolve_timezone(settings.scheduler_timezone or settings.timezone)
    now_dt = now or datetime.now(tz)
    send_flag = settings.scheduler_send_discord if send_discord is None else send_discord
    alerts_send_flag = _resolve_phase1_alerts_send_discord(send_flag)

    _log_event("INFO", "job_start", {"job": "ads_weekly_mon_0900", "time": now_dt.isoformat()})

    try:
        skipped_due_to_token, _ = _run_report_token_preflight_with_retry(
            job_name="ads_weekly_mon_0900",
            shops=shops,
            send_discord=send_flag,
        )
        if skipped_due_to_token:
            _log_event(
                "INFO",
                "job_end",
                {
                    "job": "ads_weekly_mon_0900",
                    "time": now_dt.isoformat(),
                    "skipped_due_to_token": 1,
                },
            )
            return
        transport_value = _resolve_phase1_schedule_transport()
        allow_network = _allow_network_enabled()
        if transport_value == "live" and not allow_network:
            transport_value = "none"
        plan_path, mapping_path, fixtures_dir = _resolve_phase1_schedule_paths(
            transport_value, job="weekly"
        )
        result, retry_error = _run_phase1_schedule_with_retry(
            job_name="ads_weekly_mon_0900",
            run_kwargs={
                "settings": settings,
                "shops": shops,
                "job": "weekly",
                "anchor_date": now_dt.date(),
                "transport": transport_value,
                "allow_network": allow_network,
                "token_mode": _resolve_phase1_schedule_token_mode(),
                "plan_path": plan_path,
                "mapping_path": mapping_path,
                "fixtures_dir": fixtures_dir,
                "save_failure_artifacts": _resolve_phase1_schedule_save_failure_artifacts(),
                "send_discord": send_flag,
                "discord_attach_report_html": _resolve_discord_attach_report_html(),
                "discord_attach_report_zip": _resolve_discord_attach_report_zip(),
                "discord_attach_report_md": _resolve_discord_attach_report_md(),
            },
        )
        if result is None or int(result.get("ok") or 0) != 1:
            failure_summary = retry_error
            if not failure_summary and isinstance(result, dict):
                failure_summary = _format_failure_summary(result.get("failures"))
            _emit_scheduler_failure_alert(
                settings=settings,
                shops=shops,
                job_name="ads_weekly_mon_0900",
                error_summary=failure_summary or "unknown_weekly_report_failure",
                send_discord=alerts_send_flag,
                severity="CRITICAL",
                dedup_cooldown_sec=3600,
            )
    except Exception as exc:
        _emit_scheduler_failure_alert(
            settings=settings,
            shops=shops,
            job_name="ads_weekly_mon_0900",
            error_summary=str(exc),
            send_discord=alerts_send_flag,
            severity="CRITICAL",
            dedup_cooldown_sec=3600,
        )
        _log_event("ERROR", "job_error", {"job": "ads_weekly_mon_0900", "error": str(exc)})

    _log_event("INFO", "job_end", {"job": "ads_weekly_mon_0900", "time": now_dt.isoformat()})


def compute_next_daily_run(now: datetime, time_str: str, tz) -> datetime:
    hour, minute = _parse_time(time_str)
    now_local = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    candidate = datetime.combine(now_local.date(), time(hour, minute), tzinfo=tz)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate


def compute_next_weekly_run(now: datetime, dow: str, time_str: str, tz) -> datetime:
    hour, minute = _parse_time(time_str)
    dow_idx = _dow_to_index(_parse_dow(dow))
    now_local = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    candidate = datetime.combine(now_local.date(), time(hour, minute), tzinfo=tz)
    days_ahead = (dow_idx - candidate.weekday()) % 7
    candidate = candidate + timedelta(days=days_ahead)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=7)
    return candidate


def _allow_network_enabled() -> bool:
    return os.environ.get("ALLOW_NETWORK", "").strip().lower() in {"1", "true", "yes"}


def _resolve_phase1_schedule_transport() -> str:
    # Default to live, but enforce ALLOW_NETWORK=1 before doing HTTP.
    value = os.environ.get("PHASE1_SCHEDULE_TRANSPORT", "live").strip().lower()
    if value not in {"live", "fixtures", "none"}:
        return "live"
    return value


def _resolve_phase1_schedule_token_mode() -> str:
    value = os.environ.get("PHASE1_SCHEDULE_TOKEN_MODE", "passive").strip().lower()
    return "passive" if value == "passive" else "default"


def _resolve_phase1_schedule_save_failure_artifacts() -> bool:
    value = os.environ.get("PHASE1_SCHEDULE_SAVE_FAILURE_ARTIFACTS", "1").strip().lower()
    return value not in {"0", "false", "no"}


def _resolve_discord_attach_report_html() -> bool:
    value = os.environ.get("DISCORD_ATTACH_REPORT_HTML", "").strip().lower()
    return value in {"1", "true", "yes"}


def _resolve_discord_attach_report_zip() -> bool:
    value = os.environ.get("DISCORD_ATTACH_REPORT_ZIP", "").strip().lower()
    return value in {"1", "true", "yes"}


def _resolve_discord_attach_report_md() -> bool:
    value = os.environ.get("DISCORD_ATTACH_REPORT_MD", "").strip().lower()
    return value in {"1", "true", "yes"}


def _resolve_phase1_schedule_paths(
    transport: str, *, job: str | None = None
) -> tuple[Path, Path, Path | None]:
    job_value = (job or "").strip().lower()
    if job_value == "daily-final":
        plan_raw = os.environ.get("PHASE1_SCHEDULE_PLAN_DAILY_FINAL", "").strip()
        if not plan_raw:
            plan_raw = os.environ.get(
                "PHASE1_SCHEDULE_PLAN", "collaboration/plans/ads_ingest_daily_final.yaml"
            )
    else:
        plan_raw = os.environ.get("PHASE1_SCHEDULE_PLAN", "collaboration/plans/ads_ingest_minimal.yaml")

    plan = Path(plan_raw)
    mapping = Path(os.environ.get("PHASE1_SCHEDULE_MAPPING", "collaboration/mappings/ads_mapping.yaml"))
    fixtures_dir = None
    if transport == "fixtures":
        fixtures_dir = Path(os.environ.get("PHASE1_SCHEDULE_FIXTURES_DIR", "tests/fixtures/shopee_ads"))
    return plan, mapping, fixtures_dir


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


def _resolve_strict_preflight_enabled() -> bool:
    value = os.environ.get("DOTORI_STRICT_PREFLIGHT", "").strip().lower()
    return value in {"1", "true", "yes"}


def _resolve_min_access_ttl_sec() -> int:
    raw = os.environ.get("DOTORI_MIN_ACCESS_TTL_SEC", "120").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 120
    return max(value, 1)


def _resolve_token_alert_cooldown_sec() -> int:
    raw = os.environ.get("DOTORI_TOKEN_ALERT_COOLDOWN_SEC", "21600").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 21600
    return max(value, 0)


def _resolve_token_resolved_cooldown_sec() -> int:
    raw = os.environ.get("DOTORI_TOKEN_RESOLVED_COOLDOWN_SEC", "21600").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 21600
    return max(value, 0)


def _resolve_token_ttl_low_alert_enabled() -> bool:
    raw = os.environ.get("DOTORI_TOKEN_TTL_LOW_ALERT_ENABLED", "0").strip().lower()
    return raw in {"1", "true", "yes"}


def _resolve_token_gate_blocked_alert_cooldown_sec() -> int:
    raw = os.environ.get("DOTORI_TOKEN_GATE_BLOCKED_ALERT_COOLDOWN_SEC", "3600").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 3600
    return max(value, 0)


def _resolve_preflight_artifacts_root() -> Path:
    raw = os.environ.get(
        "DOTORI_PREFLIGHT_ARTIFACTS_ROOT", "collaboration/tmp/token_preflight_gate"
    ).strip()
    if not raw:
        raw = "collaboration/tmp/token_preflight_gate"
    path = Path(raw)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def _resolve_report_retry_delay_sec() -> int:
    raw = os.environ.get("DOTORI_REPORT_RETRY_DELAY_SEC", "60").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 60
    return max(value, 1)


def _resolve_report_retry_attempts() -> int:
    raw = os.environ.get("DOTORI_REPORT_RETRY_ATTEMPTS", "2").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 2
    return max(value, 1)


def _scheduler_token_preflight_gate(
    *,
    job_name: str,
    shops,
    send_discord: bool,
) -> tuple[bool, dict[str, object]]:
    strict_enabled = _resolve_strict_preflight_enabled()
    if not strict_enabled:
        return False, {}

    enabled = _enabled_shops(shops)
    min_access_ttl_sec = _resolve_min_access_ttl_sec()
    cooldown_sec = _resolve_token_alert_cooldown_sec()
    resolved_cooldown_sec = _resolve_token_resolved_cooldown_sec()
    gate_result = evaluate_token_preflight_gate(
        shops=enabled,
        min_access_ttl_sec=min_access_ttl_sec,
    )
    for row in gate_result.get("rows") or []:
        if not isinstance(row, dict):
            continue
        print(
            "preflight_gate_row "
            f"job={job_name} shop={row.get('shop_key')} "
            f"token_verdict={row.get('token_verdict')} "
            f"access_expires_in_sec={row.get('access_expires_in_sec')} "
            f"min_access_ttl_sec={row.get('min_access_ttl_sec')}"
        )
    if bool(gate_result.get("ok")):
        resolved = emit_token_resolved_alerts_with_cooldown(
            shops=enabled,
            gate_result=gate_result,
            cooldown_sec=resolved_cooldown_sec,
            send_discord=send_discord,
        )
        for row in resolved.get("rows") or []:
            if not isinstance(row, dict):
                continue
            if int(row.get("transitioned_from_blocked") or 0) != 1:
                continue
            shop_key = str(row.get("shop_key") or "-")
            cooldown_until = str(row.get("resolved_cooldown_until_utc") or "-")
            if int(row.get("suppressed") or 0) == 1:
                print(
                    "discord_token_resolved_cooldown_skip=1 "
                    f"job={job_name} shop={shop_key} resolved_cooldown_until_utc={cooldown_until}"
                )
                continue
            if int(row.get("dry_run") or 0) == 1:
                print(
                    "discord_token_resolved_dry_run=1 "
                    f"job={job_name} shop={shop_key} resolved_cooldown_until_utc={cooldown_until}"
                )
            elif send_discord:
                http_status = int(row.get("http_status") or -1)
                print(
                    "discord_token_resolved_send_ok=1 "
                    f"job={job_name} shop={shop_key} http_status={http_status} "
                    f"resolved_cooldown_until_utc={cooldown_until}"
                )
            else:
                print(
                    "discord_token_resolved_skipped=1 "
                    f"job={job_name} shop={shop_key} reason=send_disabled"
                )
        print(f"preflight_gate_ok=1 job={job_name} skipped_due_to_token=0")
        return False, {
            "strict_preflight": 1,
            "preflight_gate": gate_result,
            "token_resolved": resolved,
        }

    alerts = emit_token_ttl_alerts_with_cooldown(
        shops=enabled,
        gate_result=gate_result,
        cooldown_sec=cooldown_sec,
        send_discord=(send_discord and _resolve_token_ttl_low_alert_enabled()),
    )
    if send_discord:
        failed_rows = [
            row
            for row in (gate_result.get("rows") or [])
            if isinstance(row, dict)
            and str(row.get("token_verdict") or "").strip().lower() in {"missing", "unknown", "expired", "short_ttl"}
        ]
        if failed_rows:
            detail_lines: list[str] = []
            for row in failed_rows:
                detail_lines.append(
                    "{}: verdict={} ttl={}s min={}s".format(
                        str(row.get("shop_key") or "-"),
                        str(row.get("token_verdict") or "-"),
                        int(row.get("access_expires_in_sec") or -1),
                        int(row.get("min_access_ttl_sec") or 0),
                    )
                )
            dispatch_alert_card(
                title="Token gate blocked - scheduler tam dung tam thoi",
                severity="WARN",
                event_code="TOKEN_GATE_BLOCKED",
                detail_lines=detail_lines,
                action_line="He thong se tu dong thu lai o chu ky tiep theo.",
                dedup_key=f"token_gate_blocked:{job_name}:{','.join(sorted([shop.shop_key for shop in enabled]))}",
                cooldown_sec=_resolve_token_gate_blocked_alert_cooldown_sec(),
                send_discord=True,
                shop_label="OPS",
                webhook_url=get_settings().discord_webhook_alerts_url,
                meta={
                    "job_name": job_name,
                    "failed_rows": failed_rows,
                    "ttl_low_alert_enabled": int(_resolve_token_ttl_low_alert_enabled()),
                },
            )
    for row in alerts.get("rows") or []:
        if not isinstance(row, dict):
            continue
        shop_key = str(row.get("shop_key") or "-")
        cooldown_until = str(row.get("cooldown_until_utc") or "-")
        if int(row.get("suppressed") or 0) == 1:
            print(
                "discord_token_alert_cooldown_skip=1 "
                f"job={job_name} shop={shop_key} cooldown_until_utc={cooldown_until}"
            )
            continue
        if int(row.get("dry_run") or 0) == 1:
            print(
                "discord_token_alert_dry_run=1 "
                f"job={job_name} shop={shop_key} cooldown_until_utc={cooldown_until}"
            )
        elif send_discord:
            print(
                "discord_token_alert_sent=1 "
                f"job={job_name} shop={shop_key} cooldown_until_utc={cooldown_until}"
            )
        else:
            print(
                "discord_token_alert_skipped=1 "
                f"job={job_name} shop={shop_key} reason=send_disabled"
            )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_dir = _resolve_preflight_artifacts_root() / f"{job_name}_{stamp}"
    artifact_paths = write_token_preflight_gate_artifacts(
        base_dir=artifact_dir,
        gate_result=gate_result,
        alert_result=alerts,
        resolved_result={},
    )
    print(
        "preflight_gate_ok=0 "
        f"job={job_name} skipped_due_to_token=1 reason={gate_result.get('reason', '-')}"
    )
    print(f"preflight_gate_summary_json={artifact_paths.get('json_path')}")
    print(f"preflight_gate_summary_md={artifact_paths.get('md_path')}")
    _log_event(
        "WARN",
        "job_skipped_due_to_token_preflight",
        {
            "job": job_name,
            "strict_preflight": 1,
            "min_access_ttl_sec": min_access_ttl_sec,
            "token_alert_cooldown_sec": cooldown_sec,
            "token_resolved_cooldown_sec": resolved_cooldown_sec,
            "reason": gate_result.get("reason"),
            "alerts_emitted": int(alerts.get("emitted") or 0),
            "alerts_suppressed": int(alerts.get("suppressed") or 0),
            "summary_json": artifact_paths.get("json_path"),
            "summary_md": artifact_paths.get("md_path"),
        },
    )
    return True, {
        "strict_preflight": 1,
        "preflight_gate": gate_result,
        "token_alerts": alerts,
        "artifacts": artifact_paths,
    }


def _run_report_token_preflight_with_retry(
    *,
    job_name: str,
    shops,
    send_discord: bool,
) -> tuple[bool, dict[str, object]]:
    attempts_total = _resolve_report_retry_attempts()
    retry_delay_sec = _resolve_report_retry_delay_sec()
    skipped_due_to_token = True
    gate_meta: dict[str, object] = {}

    for attempt in range(1, attempts_total + 1):
        skipped_due_to_token, gate_meta = _scheduler_token_preflight_gate(
            job_name=job_name,
            shops=shops,
            send_discord=send_discord,
        )
        if not skipped_due_to_token:
            if attempt > 1:
                print(
                    "preflight_gate_retry_success "
                    f"job={job_name} attempts_used={attempt}"
                )
            return False, gate_meta
        if attempt >= attempts_total:
            return True, gate_meta
        print(
            "preflight_gate_retry_wait "
            f"job={job_name} attempt={attempt} next_attempt={attempt + 1} "
            f"wait_sec={retry_delay_sec}"
        )
        pytime.sleep(retry_delay_sec)

    return skipped_due_to_token, gate_meta


def _run_phase1_schedule_with_retry(
    *,
    job_name: str,
    run_kwargs: dict[str, object],
) -> tuple[dict[str, object] | None, str]:
    attempts_total = _resolve_report_retry_attempts()
    retry_delay_sec = _resolve_report_retry_delay_sec()
    last_result: dict[str, object] | None = None
    last_error = ""

    for attempt in range(1, attempts_total + 1):
        try:
            result = phase1_schedule_run_once(**run_kwargs)
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if attempt >= attempts_total:
                return None, last_error
            print(
                "report_retry_wait "
                f"job={job_name} attempt={attempt} next_attempt={attempt + 1} "
                f"wait_sec={retry_delay_sec} reason=exception"
            )
            pytime.sleep(retry_delay_sec)
            continue

        last_result = result
        if int(result.get("ok") or 0) == 1:
            if attempt > 1:
                print(
                    "report_retry_success "
                    f"job={job_name} attempts_used={attempt}"
                )
            return result, ""

        last_error = _format_failure_summary(result.get("failures"))
        if not last_error:
            last_error = "phase1_schedule_run_once_not_ok"
        if attempt >= attempts_total:
            return last_result, last_error
        print(
            "report_retry_wait "
            f"job={job_name} attempt={attempt} next_attempt={attempt + 1} "
            f"wait_sec={retry_delay_sec} reason=result_not_ok"
        )
        pytime.sleep(retry_delay_sec)

    return last_result, last_error


def phase1_schedule_run_once(
    *,
    settings: Settings,
    shops,
    job: str,
    anchor_date: date,
    transport: str,
    allow_network: bool,
    token_mode: str,
    plan_path: Path,
    mapping_path: Path,
    fixtures_dir: Path | None,
    save_failure_artifacts: bool,
    send_discord: bool,
    discord_attach_report_html: bool = False,
    discord_attach_report_zip: bool = False,
    discord_attach_report_md: bool = False,
) -> dict[str, object]:
    """
    Phase 1 scheduler harness core:
    - ingest (fixtures/live/none) -> DB upsert
    - generate daily/weekly HTML report

    Notes:
    - Live HTTP requires allow_network=True (enforced by caller).
    - Token refresh is controlled by token_mode (use passive for Phase 1).
    - Timezone basis is Asia/Ho_Chi_Minh (UTC+7).
    """
    job_value = job.strip().lower()
    transport_value = transport.strip().lower()
    if job_value not in {"daily-final", "daily-midday", "weekly"}:
        raise ValueError(f"unknown job: {job}")
    if transport_value not in {"fixtures", "live", "none"}:
        raise ValueError(f"unknown transport: {transport}")

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
        # Deterministic midday cutoff (for stable replays/tests).
        as_of = datetime.combine(report_date, time(13, 0), tzinfo=tz)
    else:
        window_start, window_end = get_last_week_range(anchor_dt, tz)
        week_id_value = weekly_id(window_start)
        ingest_date = window_end

    if transport_value == "live":
        if not allow_network:
            raise RuntimeError("allow_network_required transport=live")
        daily_ok, snapshot_ok, missing = _ads_endpoint_status()
        if not (daily_ok and snapshot_ok):
            raise RuntimeError(f"ads_endpoints_not_configured missing={','.join(missing)}")
        if settings.shopee_partner_id is None or not settings.shopee_partner_key:
            raise RuntimeError("missing_shopee_settings partner_id_or_key")

    if transport_value == "fixtures":
        if fixtures_dir is None:
            raise ValueError("fixtures_dir is required when transport=fixtures")
        if not fixtures_dir.exists():
            raise FileNotFoundError(f"fixtures-dir not found: {fixtures_dir}")

    enabled = _enabled_shops(shops)
    if not enabled:
        raise RuntimeError("no enabled shops")

    totals = {"calls_ok": 0, "calls_fail": 0, "campaigns": 0, "daily": 0, "snapshots": 0}
    failures: dict[str, str] = {}
    per_shop: dict[str, dict[str, object]] = {}

    for shop in enabled:
        if shop.shopee_shop_id is None:
            raise RuntimeError(f"shopee_shop_id missing for shop_key={shop.shop_key}")

        shop_row: dict[str, object] = {"shop_key": shop.shop_key, "label": shop.label}

        if transport_value != "none":
            fx_dir = fixtures_dir if transport_value == "fixtures" else None
            try:
                summary = ingest_ads_live(
                    shop_cfg=shop,
                    settings=settings,
                    target_date=ingest_date,
                    plan_path=plan_path,
                    mapping_path=mapping_path,
                    save_artifacts=False,
                    save_failure_artifacts=save_failure_artifacts,
                    dry_run=False,
                    strict_mapping=False,
                    fixtures_dir=fx_dir,
                    token_mode=token_mode,
                    client_factory=None,
                )
            except Exception as exc:  # noqa: BLE001
                failures[shop.shop_key] = str(exc)
                shop_row["ingest_ok"] = 0
                shop_row["error"] = str(exc)
            else:
                shop_row.update(
                    {
                        "ingest_ok": 1,
                        "calls_ok": summary.calls_ok,
                        "calls_fail": summary.calls_fail,
                        "campaigns": summary.campaigns,
                        "daily": summary.daily,
                        "snapshots": summary.snapshots,
                        "failure_artifacts_saved": summary.failure_artifacts_saved,
                    }
                )
                totals["calls_ok"] += summary.calls_ok
                totals["calls_fail"] += summary.calls_fail
                totals["campaigns"] += summary.campaigns
                totals["daily"] += summary.daily
                totals["snapshots"] += summary.snapshots

        per_shop[shop.shop_key] = shop_row

    init_db()
    session = SessionLocal()
    try:
        for shop in enabled:
            shop_key = shop.shop_key
            if job_value == "weekly":
                assert week_id_value is not None
                payload = build_weekly_payload(session, shop_key, window_start, window_end)
                html = render_weekly_html(shop.label, week_id_value, window_start, window_end, payload)
                output_path = write_weekly_report_file(shop_key, week_id_value, html)
                per_shop[shop_key]["report_path"] = str(output_path)
                _log_report_doctor(shop_key=shop_key, kind="weekly", output_path=output_path)

                report_url = _build_weekly_report_url(
                    shop_key, week_id_value, settings.report_access_token
                )
                if send_discord:
                    message = build_weekly_discord_message(
                        shop.label,
                        week_id_value,
                        payload["metrics"],
                        payload.get("wow_delta"),
                        report_url,
                    )
                    send(
                        "report",
                        message,
                        shop_label=shop.label,
                        webhook_url=shop.discord_webhook_url,
                    )

                session.add(
                    EventLog(
                        level="INFO",
                        message="ads_weekly_report_generated",
                        meta_json=_safe_json(
                            {
                                "shop_key": shop_key,
                                "week_id": week_id_value,
                                "start_date": window_start.isoformat(),
                                "end_date": window_end.isoformat(),
                                "output_path": str(output_path),
                            }
                        ),
                    )
                )
            else:
                assert report_date is not None
                assert report_kind is not None
                data = aggregate_daily_report(session, shop_key, report_date, as_of)
                generated_at = as_of if (report_kind == "midday" and as_of is not None) else datetime.now(tz)
                data.update({"shop_label": shop.label, "kind": report_kind, "generated_at": generated_at})
                scorecard = data.get("scorecard") if isinstance(data.get("scorecard"), dict) else {}
                budget_est = _to_decimal(scorecard.get("budget_est")) if isinstance(scorecard, dict) else None
                campaigns_budgeted = int(data.get("campaigns_budgeted") or 0)
                budget_source = str(data.get("budget_source") or "none")
                budget_est_text = _fmt_decimal_value(budget_est)
                print(
                    "scorecard_budget_est "
                    f"shop={shop_key} kind={report_kind} date={report_date.isoformat()} "
                    f"budget_est={budget_est_text} campaigns_budgeted={campaigns_budgeted} "
                    f"budget_source={budget_source}"
                )
                snapshot_fallback = data.get("snapshot_fallback") or {}
                fallback_used = int(snapshot_fallback.get("used") or 0) == 1
                fallback_rows = snapshot_fallback.get("rows") or []
                if fallback_used:
                    latest_snapshot_at = snapshot_fallback.get("latest_snapshot_at")
                    latest_snapshot_at_text = (
                        latest_snapshot_at.isoformat()
                        if isinstance(latest_snapshot_at, datetime)
                        else str(latest_snapshot_at or "-")
                    )
                    print(
                        "report_snapshot_fallback_used=1 "
                        f"shop={shop_key} kind={report_kind} date={report_date.isoformat()} "
                        f"rows={len(fallback_rows)} "
                        f"rank_key={str(snapshot_fallback.get('rank_key') or '-')} "
                        f"latest_snapshot_at={latest_snapshot_at_text}"
                    )
                html = render_daily_html(data)
                output_path = write_report_file(shop_key, report_date, report_kind, html)
                per_shop[shop_key]["report_path"] = str(output_path)
                per_shop[shop_key]["report_snapshot_fallback_used"] = 1 if fallback_used else 0
                if fallback_used:
                    per_shop[shop_key]["report_snapshot_fallback_rows"] = len(fallback_rows)
                    per_shop[shop_key]["report_snapshot_fallback_rank_key"] = str(
                        snapshot_fallback.get("rank_key") or ""
                    )
                _log_report_doctor(shop_key=shop_key, kind=report_kind, output_path=output_path)
                relative_path = _build_report_relative_path(output_path, settings)
                report_url, report_url_log = build_report_url(relative_path)
                if report_url_log:
                    print(f"report_url_log shop={shop_key} kind={report_kind} url={report_url_log}")
                if _is_localhost_report_url(report_url):
                    print(f"report_url_is_localhost=1 shop={shop_key} kind={report_kind}")
                kpi_snippet = _build_scorecard_discord_snippet(data)
                if kpi_snippet:
                    print(
                        "discord_report_kpi_snippet "
                        f"shop={shop_key} kind={report_kind} text={_safe_stdout_text(kpi_snippet)}"
                    )
                budget_snippet = None
                budget_source_for_log = _resolve_budget_source(data)
                print(
                    "discord_report_budget_snippet_disabled "
                    f"shop={shop_key} kind={report_kind} reason=hidden_in_report "
                    f"budget_source={budget_source_for_log}"
                )
                if send_discord:
                    summary = build_discord_summary(data, report_url)
                    snapshot_snippet = _build_snapshot_fallback_discord_snippet(data)
                    if snapshot_snippet:
                        print(
                            "discord_report_snapshot_snippet "
                            f"shop={shop_key} kind={report_kind} text={_safe_stdout_text(snapshot_snippet)}"
                        )
                    message = _build_daily_report_discord_message(
                        summary=summary,
                        report_url=report_url,
                        output_path=output_path,
                        data=data,
                        snapshot_snippet=snapshot_snippet,
                        kpi_snippet=kpi_snippet,
                        budget_snippet=budget_snippet,
                    )
                    attachment_path = str(output_path) if discord_attach_report_html else None
                    attachment_name = None
                    if discord_attach_report_html:
                        label_slug = _label_slug(shop.label)
                        attachment_name = (
                            f"{label_slug}_{report_date.isoformat()}_{report_kind}.html"
                        )
                    zip_attachment_path = None
                    zip_attachment_name = None
                    if discord_attach_report_zip:
                        zip_attachment_path, zip_attachment_name = _build_report_zip_attachment(
                            output_path=output_path,
                            shop_label=shop.label,
                            report_date=report_date,
                            report_kind=report_kind,
                        )
                    md_attachment_path = None
                    md_attachment_name = None
                    if discord_attach_report_md:
                        md_attachment_path, md_attachment_name = _build_report_md_attachment(
                            output_path=output_path,
                            shop_key=shop_key,
                            shop_label=shop.label,
                            report_kind=report_kind,
                            report_date=report_date,
                            window_start=window_start,
                            window_end=window_end,
                            report_url=report_url,
                            data=data,
                        )
                    send(
                        "report",
                        message,
                        shop_label=shop.label,
                        webhook_url=shop.discord_webhook_url,
                        attachment_path=attachment_path,
                        attachment_filename=attachment_name,
                        zip_attachment_path=zip_attachment_path,
                        zip_attachment_filename=zip_attachment_name,
                        md_attachment_path=md_attachment_path,
                        md_attachment_filename=md_attachment_name,
                    )

                session.add(
                    EventLog(
                        level="INFO",
                        message="ads_daily_report_generated",
                        meta_json=_safe_json(
                            {
                                "shop_key": shop_key,
                                "kind": report_kind,
                                "date": report_date.isoformat(),
                                "as_of": as_of.isoformat() if as_of else None,
                                "output_path": str(output_path),
                            }
                        ),
                    )
                )
        session.commit()
    finally:
        session.close()

    ok = 1 if not failures else 0
    return {
        "ok": ok,
        "job": job_value,
        "transport": transport_value,
        "anchor_date": anchor_date.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "ingest_date": ingest_date.isoformat(),
        "week_id": week_id_value,
        "kind": report_kind,
        "report_date": report_date.isoformat() if report_date else None,
        "as_of": as_of.isoformat() if as_of else None,
        "totals": totals,
        "per_shop": per_shop,
        "failures": failures,
    }


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


def _resolve_phase1_alerts_transport() -> str:
    value = os.environ.get("PHASE1_ALERTS_TRANSPORT", "live").strip().lower()
    if value not in {"live", "fixtures", "none"}:
        return "live"
    return value


def _resolve_phase1_alerts_token_mode() -> str:
    value = os.environ.get("PHASE1_ALERTS_TOKEN_MODE", "passive").strip().lower()
    return "passive" if value == "passive" else "default"


def _resolve_phase1_alerts_save_failure_artifacts() -> bool:
    value = os.environ.get("PHASE1_ALERTS_SAVE_FAILURE_ARTIFACTS", "1").strip().lower()
    return value not in {"0", "false", "no"}


def _resolve_phase1_alerts_notify_resolved() -> bool:
    value = os.environ.get("PHASE1_ALERTS_NOTIFY_RESOLVED", "1").strip().lower()
    return value not in {"0", "false", "no"}


def _resolve_phase1_alerts_send_discord(default_value: bool) -> bool:
    raw = os.environ.get("PHASE1_ALERTS_SEND_DISCORD", "").strip().lower()
    if not raw:
        return default_value
    return raw not in {"0", "false", "no"}


def _resolve_phase1_alerts_paths(transport: str) -> tuple[Path, Path, Path | None]:
    plan = Path(os.environ.get("PHASE1_ALERTS_PLAN", "collaboration/plans/ads_ingest_alerts.yaml"))
    mapping = Path(os.environ.get("PHASE1_ALERTS_MAPPING", "collaboration/mappings/ads_mapping.yaml"))
    fixtures_dir = None
    if transport == "fixtures":
        fixtures_dir = Path(os.environ.get("PHASE1_ALERTS_FIXTURES_DIR", "tests/fixtures/shopee_ads_alerts/open"))
    return plan, mapping, fixtures_dir


def phase1_alerts_run_once(
    *,
    settings: Settings,
    shops,
    as_of: datetime,
    transport: str,
    allow_network: bool,
    token_mode: str,
    plan_path: Path,
    mapping_path: Path,
    fixtures_dir: Path | None,
    save_artifacts: bool = False,
    save_failure_artifacts: bool,
    send_discord: bool,
    notify_resolved: bool,
    cooldown_minutes: int,
) -> dict[str, object]:
    """
    Phase 1 alerts harness core:
    - ingest snapshot (fixtures/live/none) -> DB upsert
    - run detectors + incident lifecycle
    - emit job telemetry to event_log
    """
    transport_value = transport.strip().lower()
    if transport_value not in {"fixtures", "live", "none"}:
        raise ValueError(f"unknown transport: {transport}")

    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    as_of_local = as_of.replace(tzinfo=tz) if as_of.tzinfo is None else as_of.astimezone(tz)
    target_date = as_of_local.date()

    enabled = _enabled_shops(shops)
    if not enabled:
        raise RuntimeError("no enabled shops")

    _log_event(
        "INFO",
        "phase1_alerts_run_once_start",
        {
            "time": as_of_local.isoformat(),
            "shops": ",".join([shop.shop_key for shop in enabled]),
            "transport": transport_value,
            "plan_path": str(plan_path),
            "mapping_path": str(mapping_path),
            "token_mode": token_mode,
            "send_discord": int(send_discord),
        },
    )

    if transport_value == "live":
        if not allow_network:
            raise RuntimeError("allow_network_required transport=live")
        snapshot_ok, missing = _ads_snapshot_endpoint_status()
        if not snapshot_ok:
            raise RuntimeError(f"ads_endpoints_not_configured missing={','.join(missing)}")
        if settings.shopee_partner_id is None or not settings.shopee_partner_key:
            raise RuntimeError("missing_shopee_settings partner_id_or_key")

    if transport_value == "fixtures":
        if fixtures_dir is None:
            raise ValueError("fixtures_dir is required when transport=fixtures")
        if not fixtures_dir.exists():
            raise FileNotFoundError(f"fixtures-dir not found: {fixtures_dir}")

    ingest_totals = {"calls_ok": 0, "calls_fail": 0, "campaigns": 0, "daily": 0, "snapshots": 0}
    failures: dict[str, str] = {}
    per_shop: dict[str, dict[str, object]] = {}

    for shop in enabled:
        if shop.shopee_shop_id is None:
            raise RuntimeError(f"shopee_shop_id missing for shop_key={shop.shop_key}")
        row: dict[str, object] = {"shop_key": shop.shop_key, "label": shop.label}
        if transport_value != "none":
            fx_dir = fixtures_dir if transport_value == "fixtures" else None
            try:
                summary = ingest_ads_live(
                    shop_cfg=shop,
                    settings=settings,
                    target_date=target_date,
                    plan_path=plan_path,
                    mapping_path=mapping_path,
                    save_artifacts=save_artifacts,
                    save_failure_artifacts=save_failure_artifacts,
                    dry_run=False,
                    strict_mapping=False,
                    fixtures_dir=fx_dir,
                    token_mode=token_mode,
                    client_factory=None,
                )
            except Exception as exc:  # noqa: BLE001
                failures[shop.shop_key] = str(exc)
                row["ingest_ok"] = 0
                row["error"] = str(exc)
            else:
                row.update(
                    {
                        "ingest_ok": 1,
                        "calls_ok": summary.calls_ok,
                        "calls_fail": summary.calls_fail,
                        "campaigns": summary.campaigns,
                        "daily": summary.daily,
                        "snapshots": summary.snapshots,
                        "failure_artifacts_saved": summary.failure_artifacts_saved,
                    }
                )
                ingest_totals["calls_ok"] += summary.calls_ok
                ingest_totals["calls_fail"] += summary.calls_fail
                ingest_totals["campaigns"] += summary.campaigns
                ingest_totals["daily"] += summary.daily
                ingest_totals["snapshots"] += summary.snapshots
        per_shop[shop.shop_key] = row

    alerts_totals = {"active": 0, "opened": 0, "updated": 0, "resolved": 0, "notified": 0, "suppressed": 0}
    init_db()
    session = SessionLocal()
    try:
        for shop in enabled:
            shop_key = shop.shop_key
            try:
                counts = process_alerts(
                    shop_key=shop_key,
                    now=as_of_local,
                    session=session,
                    shop_label=shop.label,
                    webhook_url=shop.discord_webhook_url,
                    cooldown_minutes=cooldown_minutes,
                    send_discord=send_discord,
                    notify_resolved=notify_resolved,
                )
                session.commit()
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                failures[shop_key] = str(exc)
                per_shop.setdefault(shop_key, {})["alerts_ok"] = 0
                per_shop.setdefault(shop_key, {})["error"] = str(exc)
                continue
            per_shop.setdefault(shop_key, {})["alerts_ok"] = 1
            per_shop.setdefault(shop_key, {})["alerts_counts"] = counts
            for key in alerts_totals:
                alerts_totals[key] += int(counts.get(key, 0))
    finally:
        session.close()

    ok = 1 if not failures else 0
    _log_event(
        "INFO",
        "phase1_alerts_run_once_end",
        {
            "time": as_of_local.isoformat(),
            "transport": transport_value,
            "ok": ok,
            "ingest": ingest_totals,
            "alerts": alerts_totals,
            "failures": list(sorted(failures.keys())),
        },
    )
    return {
        "ok": ok,
        "transport": transport_value,
        "as_of": as_of_local.isoformat(),
        "target_date": target_date.isoformat(),
        "ingest_totals": ingest_totals,
        "alerts_totals": alerts_totals,
        "per_shop": per_shop,
        "failures": failures,
    }


def _build_daily_report_url(shop_key: str, target_date, kind: str, token: str | None) -> str:
    relative_path = f"reports/{shop_key}/daily/{target_date.isoformat()}_{kind}.html"
    shared_url, _ = build_report_url(relative_path)
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
    shared_url, _ = build_report_url(relative_path)
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


def _safe_stdout_text(value: str) -> str:
    """Encode log text for current stdout to avoid locale-dependent crashes."""
    text = str(value)
    encoding = (getattr(sys.stdout, "encoding", None) or "utf-8").strip() or "utf-8"
    return text.encode(encoding, errors="backslashreplace").decode(encoding, errors="replace")


def _build_report_relative_path(output_path: Path, settings: Settings) -> str:
    reports_root = Path(settings.reports_dir).resolve()
    path = output_path.resolve()
    try:
        rel = path.relative_to(reports_root)
    except ValueError:
        return output_path.as_posix()
    return (Path("reports") / rel).as_posix()


def _label_slug(label: str) -> str:
    raw = (label or "SHOP").strip().upper()
    slug = "".join(ch if ch.isalnum() else "_" for ch in raw)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_") or "SHOP"


def _is_localhost_report_url(report_url: str | None) -> bool:
    if not report_url:
        return False
    host = (urlsplit(report_url).hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1"}


def _build_daily_report_discord_message(
    *,
    summary: str,
    report_url: str | None,
    output_path: Path,
    data: dict[str, object] | None = None,
    snapshot_snippet: str | None = None,
    kpi_snippet: str | None = None,
    budget_snippet: str | None = None,
) -> str:
    lines = [
        summary,
    ]
    kpi_line = kpi_snippet or _build_scorecard_discord_snippet(data)
    if kpi_line:
        lines.append(kpi_line)
    snippet = snapshot_snippet or _build_snapshot_fallback_discord_snippet(data)
    if snippet:
        lines.append(snippet)
    data_sources_line = _build_data_sources_discord_line(data)
    if data_sources_line:
        lines.append(data_sources_line)
    lines.extend(
        [
            "Lưu ý: Discord không preview file .html. Hãy tải file đính kèm và mở bằng trình duyệt.",
        ]
    )
    if _is_localhost_report_url(report_url):
        lines.append(
            "Nếu link report trỏ đến localhost thì chỉ mở được trên đúng máy đang chạy web server."
        )
    lines.append(f"File cục bộ: {output_path.resolve()}")
    return "\n".join(lines)


def _build_snapshot_fallback_discord_snippet(
    data: dict[str, object] | None,
    *,
    max_items: int = 3,
    max_chars: int = 200,
) -> str | None:
    if not isinstance(data, dict):
        return None
    fallback = data.get("snapshot_fallback")
    if not isinstance(fallback, dict):
        return None
    if int(fallback.get("used") or 0) != 1:
        return None
    rows = fallback.get("rows")
    if not isinstance(rows, list) or not rows:
        return None

    shop_key = str(data.get("shop_key") or "").strip() if isinstance(data, dict) else ""
    parts: list[str] = []
    for row in rows[: max(max_items, 1)]:
        if not isinstance(row, dict):
            continue
        display_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=row.get("campaign_id"),
            campaign_name=row.get("campaign_name"),
        )
        campaign_name = _short_text(
            display_name or str(row.get("campaign_id") or "-"),
            limit=18,
        )
        chunk = campaign_name
        spend = _to_decimal(row.get("spend"))
        if spend is not None:
            chunk += f" chi tiêu={_fmt_decimal_value(spend)}"
        remaining = _snapshot_remaining_decimal(row)
        if remaining is not None:
            chunk += f" còn lại={_fmt_decimal_value(remaining)}"
        parts.append(chunk)

    if not parts:
        return None

    text = "Top snapshot: " + " | ".join(parts)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _build_scorecard_discord_snippet(data: dict[str, object] | None) -> str | None:
    if not isinstance(data, dict):
        return None
    scorecard = data.get("scorecard")
    if not isinstance(scorecard, dict):
        return None

    spend = _fmt_decimal_value(scorecard.get("spend"))
    gmv = _fmt_decimal_value(scorecard.get("gmv"))
    roas = _fmt_ratio_value(scorecard.get("roas"))
    orders = _fmt_int_value(scorecard.get("orders"))
    return f"KPI: Chi tiêu={spend} | GMV={gmv} | ROAS={roas} | Đơn hàng={orders}"


def _build_budget_discord_snippet(data: dict[str, object] | None) -> str | None:
    if not isinstance(data, dict):
        return None
    scorecard = data.get("scorecard")
    if not isinstance(scorecard, dict):
        return None
    budget = _to_decimal(scorecard.get("budget_est"))
    if budget is None:
        return None
    util_pct = _fmt_percent_value(scorecard.get("util_pct"))
    remaining = _fmt_decimal_value(scorecard.get("remaining"))
    return f"Ngân sách: {_fmt_decimal_value(budget)} | Tiêu hao: {util_pct} | Còn lại: {remaining}"


def _resolve_budget_source(data: dict[str, object] | None) -> str:
    if not isinstance(data, dict):
        return "none"
    raw = str(data.get("budget_source") or "").strip()
    if raw:
        return raw
    sources = data.get("data_sources")
    if isinstance(sources, dict):
        value = str(sources.get("budget_source") or "").strip()
        if value:
            return value
    return "none"


def _build_data_sources_discord_line(data: dict[str, object] | None) -> str | None:
    if not isinstance(data, dict):
        return None
    raw_sources = data.get("data_sources")
    if not isinstance(raw_sources, dict):
        return None
    daily_total_source = str(raw_sources.get("daily_total_source") or data.get("data_source") or "-")
    campaign_breakdown_status = str(raw_sources.get("campaign_breakdown_status") or "unknown")
    campaign_table_source = str(raw_sources.get("campaign_table_source") or "-")
    return (
        "Nguồn dữ liệu: "
        f"daily_total_source={daily_total_source} "
        f"campaign_breakdown_status={campaign_breakdown_status} "
        f"campaign_table_source={campaign_table_source}"
    )


def _build_report_zip_attachment(
    *, output_path: Path, shop_label: str, report_date: date, report_kind: str
) -> tuple[str | None, str | None]:
    label_slug = _label_slug(shop_label)
    zip_name = f"{label_slug}_{report_date.isoformat()}_{report_kind}.zip"
    tmp_dir = (Path.cwd() / "collaboration" / "tmp" / "report_zip_attach").resolve()
    tmp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = tmp_dir / zip_name
    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(output_path, arcname=output_path.name)
    except OSError as exc:
        print(
            "report_zip_attach_skipped=1 "
            f"reason=zip_build_failed file={zip_name} error={str(exc).strip()}"
        )
        return None, None
    return str(zip_path), zip_name


def _build_report_md_attachment(
    *,
    output_path: Path,
    shop_key: str,
    shop_label: str,
    report_kind: str,
    report_date: date,
    window_start: date,
    window_end: date,
    report_url: str | None,
    data: dict[str, object],
) -> tuple[str | None, str | None]:
    label_slug = _label_slug(shop_label)
    md_name = f"{label_slug}_{report_date.isoformat()}_{report_kind}.md"
    tmp_dir = (Path.cwd() / "collaboration" / "tmp" / "report_md_attach").resolve()
    tmp_dir.mkdir(parents=True, exist_ok=True)
    md_path = tmp_dir / md_name
    markdown = _render_report_markdown_summary(
        shop_key=shop_key,
        shop_label=shop_label,
        report_kind=report_kind,
        report_date=report_date,
        window_start=window_start,
        window_end=window_end,
        report_url=report_url,
        output_path=output_path,
        data=data,
    )
    try:
        md_path.write_text(markdown, encoding="utf-8")
    except OSError as exc:
        print(
            "report_md_attach_skipped=1 "
            f"reason=md_build_failed file={md_name} error={str(exc).strip()}"
        )
        return None, None
    return str(md_path), md_name


def _render_report_markdown_summary(
    *,
    shop_key: str,
    shop_label: str,
    report_kind: str,
    report_date: date,
    window_start: date,
    window_end: date,
    report_url: str | None,
    output_path: Path,
    data: dict[str, object],
) -> str:
    totals = data.get("totals") if isinstance(data.get("totals"), dict) else {}
    totals = totals if isinstance(totals, dict) else {}
    kpis = data.get("kpis") if isinstance(data.get("kpis"), dict) else {}
    kpis = kpis if isinstance(kpis, dict) else {}
    scorecard = data.get("scorecard") if isinstance(data.get("scorecard"), dict) else {}
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    data_sources = data.get("data_sources") if isinstance(data.get("data_sources"), dict) else {}
    data_sources = data_sources if isinstance(data_sources, dict) else {}
    top_spend = data.get("top_spend") if isinstance(data.get("top_spend"), list) else []
    top_rows = top_spend[:3] if isinstance(top_spend, list) else []

    spend = _fmt_decimal_value(scorecard.get("spend", totals.get("spend")))
    impressions = _fmt_int_value(scorecard.get("impressions", totals.get("impressions")))
    clicks = _fmt_int_value(scorecard.get("clicks", totals.get("clicks")))
    orders = _fmt_int_value(scorecard.get("orders", totals.get("orders")))
    gmv = _fmt_decimal_value(scorecard.get("gmv", totals.get("gmv")))
    roas = _fmt_ratio_value(scorecard.get("roas", kpis.get("roas")))
    ctr = _fmt_percent_value(scorecard.get("ctr", kpis.get("ctr")))
    cpc = _fmt_decimal_value(scorecard.get("cpc", kpis.get("cpc")))
    cvr = _fmt_percent_value(scorecard.get("cvr", kpis.get("cvr")))
    daily_total_source = str(data_sources.get("daily_total_source") or data.get("data_source") or "-")
    campaign_breakdown_status = str(data_sources.get("campaign_breakdown_status") or "unknown")
    campaign_table_source = str(data_sources.get("campaign_table_source") or "-")

    lines = [
        f"# {shop_label} Báo cáo hằng ngày ({report_kind})",
        "",
        f"- shop: {shop_key} ({shop_label})",
        f"- loai: {report_kind}",
        f"- ngày: {report_date.isoformat()}",
        f"- khoang: {window_start.isoformat()} ~ {window_end.isoformat()} (Asia/Ho_Chi_Minh)",
        "",
        (
            f"Chỉ số chính: Chi tiêu={spend} | Hiển thị={impressions} | Click={clicks} | "
            f"Đơn hàng={orders} | GMV={gmv} | ROAS={roas} | CTR={ctr}"
        ),
        "",
        "## Bảng chỉ số",
        "| Chi tiêu | Hiển thị | Click | CTR | CPC | Đơn hàng | GMV | ROAS | CVR |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        (
            f"| {spend} | {impressions} | {clicks} | {ctr} | {cpc} | {orders} | {gmv} | {roas} | {cvr} |"
        ),
        "",
        (
            "Nguồn dữ liệu: "
            f"daily_total_source={daily_total_source} | "
            f"campaign_breakdown_status={campaign_breakdown_status} | "
            f"campaign_table_source={campaign_table_source}"
        ),
        "",
        "Top campaign (theo chi tiêu):",
    ]

    if top_rows:
        for index, row in enumerate(top_rows, start=1):
            if not isinstance(row, dict):
                continue
            campaign_id = str(row.get("campaign_id") or "-")
            display_name = resolve_campaign_display_name(
                shop_key=shop_key,
                campaign_id=campaign_id,
                campaign_name=row.get("campaign_name"),
            )
            row_spend = _fmt_decimal_value(row.get("spend"))
            row_roas = _fmt_ratio_value(row.get("roas"))
            lines.append(
                f"- {index}. {display_name} ({campaign_id}) | chi tiêu={row_spend} | roas={row_roas}"
            )
    else:
        lines.append("- (không có dữ liệu campaign)")

    fallback_lines = _render_snapshot_fallback_markdown_lines(data)
    if fallback_lines:
        lines.extend(["", *fallback_lines])

    lines.extend(
        [
            "",
            "Huong dan mo file local:",
            "1) Nhan Win+R va dan duong dan file local ben duoi.",
            f"2) Duong dan file local: {output_path.resolve()}",
            "3) Neu link report la localhost thi chi mo duoc tren may dang chay web server.",
        ]
    )
    if report_url:
        lines.append(f"- Link report: {report_url}")
    return "\n".join(lines) + "\n"


def _render_snapshot_fallback_markdown_lines(data: dict[str, object]) -> list[str]:
    fallback = data.get("snapshot_fallback")
    if not isinstance(fallback, dict):
        return []
    if int(fallback.get("used") or 0) != 1:
        return []
    rows_raw = fallback.get("rows")
    if not isinstance(rows_raw, list) or not rows_raw:
        return []

    lines = [
        "## Top campaign (snapshot fallback)",
        "| Chiến dịch | Trạng thái | Ngân sách | Chi tiêu | Còn lại | Cập nhật |",
        "|---|---:|---:|---:|---:|---|",
    ]

    shown = 0
    for row in rows_raw[:5]:
        if not isinstance(row, dict):
            continue
        campaign_id = str(row.get("campaign_id") or "").strip()
        shop_key = str(data.get("shop_key") or "").strip()
        campaign_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name=row.get("campaign_name"),
        )
        campaign = (
            campaign_name
            if campaign_name == campaign_id or not campaign_id
            else f"{campaign_name} ({campaign_id})"
        )
        status = str(row.get("status") or "-")
        budget_text = _fmt_decimal_value(row.get("budget"))
        spend_text = _fmt_decimal_value(row.get("spend"))
        remaining_text = _fmt_decimal_value(_snapshot_remaining_decimal(row))
        updated_text = _fmt_datetime_value(row.get("updated_at"))
        lines.append(
            f"| {campaign} | {status} | {budget_text} | {spend_text} | {remaining_text} | {updated_text} |"
        )
        shown += 1

    if shown <= 0:
        return []

    rank_key = str(fallback.get("rank_key") or "-")
    latest_snapshot_at = _fmt_datetime_value(fallback.get("latest_snapshot_at"))
    lines.append("")
    lines.append(f"Dữ liệu: rank_key={rank_key}, latest_snapshot_at={latest_snapshot_at}")
    return lines


class _ReportDoctorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title_open = False
        self.title_ok = False
        self.table_count = 0
        self.script_count = 0
        self.style_count = 0
        self.link_count = 0
        self.meta_charset_ok = False
        self.text_len = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_value = tag.lower()
        if tag_value == "title":
            self.title_open = True
        elif tag_value == "table":
            self.table_count += 1
        elif tag_value == "script":
            self.script_count += 1
        elif tag_value == "style":
            self.style_count += 1
        elif tag_value == "link":
            self.link_count += 1
        elif tag_value == "meta":
            attr_map = {k.lower(): (v or "") for k, v in attrs}
            if attr_map.get("charset", "").strip():
                self.meta_charset_ok = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title" and self.title_open:
            self.title_open = False
            self.title_ok = True

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.text_len += len(text)


def _inspect_html_report(path: Path) -> dict[str, int]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return {
            "title_ok": 0,
            "tables": 0,
            "scripts": 0,
            "text_len": 0,
            "style_tags": 0,
            "link_tags": 0,
            "meta_charset_ok": 0,
        }
    parser = _ReportDoctorParser()
    parser.feed(text)
    parser.close()
    return {
        "title_ok": 1 if parser.title_ok else 0,
        "tables": parser.table_count,
        "scripts": parser.script_count,
        "text_len": parser.text_len,
        "style_tags": parser.style_count,
        "link_tags": parser.link_count,
        "meta_charset_ok": 1 if parser.meta_charset_ok else 0,
    }


def _log_report_doctor(*, shop_key: str, kind: str, output_path: Path) -> None:
    resolved = output_path.resolve()
    size = resolved.stat().st_size if resolved.exists() else 0
    diag = _inspect_html_report(resolved)
    print(
        "report_doctor "
        f"shop={shop_key} kind={kind} path={resolved} size={size} "
        f"title_ok={diag.get('title_ok', 0)} "
        f"tables={diag.get('tables', 0)} scripts={diag.get('scripts', 0)} "
        f"text_len={diag.get('text_len', 0)} "
        f"style_tags={diag.get('style_tags', 0)} link_tags={diag.get('link_tags', 0)} "
        f"meta_charset_ok={diag.get('meta_charset_ok', 0)}"
    )
    if int(diag.get("scripts", 0)) > 0:
        print(
            "report_doctor_warning=1 "
            f"reason=has_script_tags shop={shop_key} kind={kind} scripts={diag.get('scripts', 0)}"
        )
    if int(diag.get("meta_charset_ok", 0)) == 0:
        print(
            "report_doctor_warning=1 "
            f"reason=missing_meta_charset shop={shop_key} kind={kind}"
        )
    if int(diag.get("text_len", 0)) <= 0:
        print(
            "report_doctor_warning=1 "
            f"reason=empty_text shop={shop_key} kind={kind}"
        )


def _format_failure_summary(failures: object) -> str:
    if not isinstance(failures, dict):
        return "unknown_failure"
    if not failures:
        return "unknown_failure"
    chunks: list[str] = []
    for shop_key in sorted(str(key) for key in failures.keys()):
        value = failures.get(shop_key)
        error_text = str(value or "failed").strip()
        if len(error_text) > 120:
            error_text = error_text[:117].rstrip() + "..."
        chunks.append(f"{shop_key}:{error_text}")
    return "; ".join(chunks) if chunks else "unknown_failure"


def _emit_scheduler_failure_alert(
    *,
    settings: Settings,
    shops,
    job_name: str,
    error_summary: str,
    send_discord: bool,
    severity: str,
    dedup_cooldown_sec: int,
) -> None:
    enabled = _enabled_shops(shops)
    shop_keys = ",".join([shop.shop_key for shop in enabled]) if enabled else "-"
    dedup_key = f"scheduler_failure:{job_name}:{shop_keys}"
    dispatch_alert_card(
        title=f"CẢNH BÁO {severity} - Job thất bại",
        severity=severity,
        event_code="SCHEDULER_JOB_FAILED",
        detail_lines=[
            f"Job: {job_name}",
            f"Shop: {shop_keys}",
            f"Lỗi: {error_summary}",
        ],
        action_line="Kiểm tra log scheduler và chạy run-once để xác minh.",
        dedup_key=dedup_key,
        cooldown_sec=dedup_cooldown_sec,
        send_discord=send_discord,
        shop_label="OPS",
        webhook_url=settings.discord_webhook_alerts_url,
        meta={"job_name": job_name, "shop_keys": shop_keys, "error": error_summary},
    )


def _send_daily_incident_digest(
    *,
    settings: Settings,
    shops,
    result: dict[str, object],
    send_discord: bool,
) -> None:
    report_date_raw = str(result.get("report_date") or "").strip()
    if not report_date_raw:
        return
    try:
        report_date = date.fromisoformat(report_date_raw)
    except ValueError:
        return
    if not _resolve_phase1_alerts_daily_digest_enabled():
        return

    init_db()
    session = SessionLocal()
    try:
        for shop in _enabled_shops(shops):
            digest = _build_incident_digest_payload_for_shop(
                session=session,
                shop_key=shop.shop_key,
                report_date=report_date,
            )
            dedup_key = f"daily_digest:{shop.shop_key}:{report_date.isoformat()}"
            if digest["has_incident"] == 0:
                title = f"Tóm tắt sự cố ngày {report_date.isoformat()} (không phát sinh)"
                detail_lines = [
                    f"Shop: {shop.label}",
                    "Mở mới: 0 | Đã xử lý: 0 | Đang mở cuối ngày: 0",
                ]
                severity = "INFO"
                action = "Không cần hành động."
            else:
                title = f"Tóm tắt sự cố ngày {report_date.isoformat()}"
                detail_lines = [
                    f"Shop: {shop.label}",
                    (
                        f"Mở mới: {digest['opened']} | Đã xử lý: {digest['resolved']} | "
                        f"Đang mở cuối ngày: {digest['open_end_of_day']}"
                    ),
                    (
                        f"CRITICAL={digest['critical']} | WARN={digest['warn']} | "
                        f"INFO={digest['info']}"
                    ),
                ]
                top_types = str(digest.get("top_types") or "").strip()
                if top_types:
                    detail_lines.append(f"Top lỗi: {top_types}")
                severity = "WARN" if int(digest["critical"]) > 0 else "INFO"
                action = "Ưu tiên xử lý CRITICAL/WARN trước ca làm việc kế tiếp."

            report_url = _build_daily_report_url(
                shop.shop_key,
                report_date,
                "final",
                settings.report_access_token,
            )
            if report_url:
                detail_lines.append(report_url)
            dispatch_alert_card(
                title=title,
                severity=severity,
                event_code="DAILY_INCIDENT_DIGEST",
                detail_lines=detail_lines,
                action_line=action,
                dedup_key=dedup_key,
                cooldown_sec=20 * 60 * 60,
                send_discord=send_discord,
                shop_label=shop.label,
                webhook_url=shop.discord_webhook_url or settings.discord_webhook_alerts_url,
                meta={
                    "shop_key": shop.shop_key,
                    "report_date": report_date.isoformat(),
                    **digest,
                },
            )
    finally:
        session.close()


def _build_incident_digest_payload_for_shop(
    *,
    session,
    shop_key: str,
    report_date: date,
) -> dict[str, object]:
    rows = (
        session.query(AdsIncident)
        .filter(AdsIncident.shop_key == shop_key)
        .all()
    )

    opened = 0
    resolved = 0
    open_end_of_day = 0
    critical = 0
    warn = 0
    info = 0
    type_counter: dict[str, int] = {}

    for row in rows:
        first_day = _incident_date_local(row.first_seen_at)
        resolved_day = _incident_date_local(row.resolved_at)
        last_day = _incident_date_local(row.last_seen_at)
        if first_day == report_date:
            opened += 1
            code = str(row.incident_type or "unknown")
            type_counter[code] = type_counter.get(code, 0) + 1
            severity = str(row.severity or "").strip().upper()
            if severity == "CRITICAL":
                critical += 1
            elif severity == "WARN":
                warn += 1
            else:
                info += 1
        if resolved_day == report_date:
            resolved += 1
        if row.status == "OPEN":
            if first_day is not None and first_day <= report_date:
                open_end_of_day += 1
        elif row.status == "RESOLVED" and resolved_day is not None and resolved_day > report_date:
            if first_day is not None and first_day <= report_date:
                open_end_of_day += 1
        elif row.status == "RESOLVED" and resolved_day is None and last_day is not None and last_day >= report_date:
            if first_day is not None and first_day <= report_date:
                open_end_of_day += 1

    top_types_pairs = sorted(type_counter.items(), key=lambda item: item[1], reverse=True)[:3]
    top_types = ", ".join([f"{code}({count})" for code, count in top_types_pairs])
    has_incident = 1 if (opened > 0 or resolved > 0 or open_end_of_day > 0) else 0
    return {
        "opened": opened,
        "resolved": resolved,
        "open_end_of_day": open_end_of_day,
        "critical": critical,
        "warn": warn,
        "info": info,
        "top_types": top_types,
        "has_incident": has_incident,
    }


def _incident_date_local(value: datetime | None) -> date | None:
    if value is None:
        return None
    dt = value
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.date()
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    return dt.astimezone(tz).date()


def _resolve_phase1_alerts_daily_digest_enabled() -> bool:
    raw = os.environ.get("PHASE1_ALERTS_DAILY_DIGEST_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no"}


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _fmt_decimal_value(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return "-"
    rounded = dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    return f"{sign}VND {abs(rounded):,}"


def _fmt_int_value(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{int(value):,}"
    except Exception:  # noqa: BLE001
        return "-"


def _fmt_ratio_value(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return "-"
    return f"{dec:.2f}"


def _fmt_percent_value(value: object) -> str:
    dec = _to_decimal(value)
    if dec is None:
        return "-"
    pct = dec * Decimal("100")
    return f"{pct:.2f}%"


def _fmt_datetime_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or "-"


def _snapshot_remaining_decimal(row: dict[str, object]) -> Decimal | None:
    remaining_value = _to_decimal(row.get("remaining"))
    if remaining_value is not None:
        return remaining_value if remaining_value >= 0 else Decimal("0")

    budget = _to_decimal(row.get("budget"))
    spend = _to_decimal(row.get("spend"))
    if budget is None or spend is None:
        return None
    diff = budget - spend
    return diff if diff >= 0 else Decimal("0")


def _short_text(value: str, *, limit: int) -> str:
    if limit <= 0:
        return ""
    text = value.strip()
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def _enabled_shops(shops) -> Iterable:
    return [shop for shop in shops if shop.enabled]


def _parse_time(value: str) -> tuple[int, int]:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time: {value}")
    return int(parts[0]), int(parts[1])


def _parse_dow(value: str) -> str:
    value = value.strip().lower()
    mapping = {
        "mon": "mon",
        "monday": "mon",
        "tue": "tue",
        "tues": "tue",
        "tuesday": "tue",
        "wed": "wed",
        "wednesday": "wed",
        "thu": "thu",
        "thur": "thu",
        "thursday": "thu",
        "fri": "fri",
        "friday": "fri",
        "sat": "sat",
        "saturday": "sat",
        "sun": "sun",
        "sunday": "sun",
    }
    if value not in mapping:
        raise ValueError(f"Invalid day of week: {value}")
    return mapping[value]


def _dow_to_index(value: str) -> int:
    mapping = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    return mapping[value]


def _log_event(level: str, message: str, meta: dict) -> None:
    init_db()
    session = SessionLocal()
    try:
        session.add(EventLog(level=level, message=message, meta_json=_safe_json(meta)))
        session.commit()
    finally:
        session.close()


def _safe_json(meta: dict) -> str:
    import json

    return json.dumps(meta, ensure_ascii=True, default=str)

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from html import escape
from pathlib import Path
import json
from typing import Any
from sqlalchemy import and_

from .campaign_labels import resolve_campaign_display_name
from .metrics import (
    aggregate_metric_rows,
    build_surface_metrics_snapshot,
    compute_kpis_from_totals,
    nullable_decimal as metrics_nullable_decimal,
    safe_div as metrics_safe_div,
    to_decimal as metrics_to_decimal,
)
from .models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot, Phase1AdsGmsCampaignRegistry
from ..config import get_settings, load_shops, resolve_timezone
from ..db import EventLog

BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY = "product_level_only"
GMS_GROUP_SCOPE_AGGREGATE_ONLY = "aggregate_only"
BREAKDOWN_SCOPE_NOTE = (
    "Campaign breakdown: product-level only; GMS/Group shown as aggregate totals"
)


def aggregate_daily_report(
    session,
    shop_key: str,
    date: date_type,
    as_of: datetime | None,
) -> dict[str, Any]:
    source = "ads_daily"
    all_daily_rows = _load_daily_rows(session, shop_key, date)
    daily_rows = _select_daily_rows_for_aggregation(all_daily_rows)
    rows_for_totals = _select_rows_for_totals(all_daily_rows)
    rows = rows_for_totals

    # Prefer ads_daily totals when present. Midday snapshot totals are only a fallback.
    if not rows and as_of is not None:
        snapshot_rows = _load_snapshot_rows(session, shop_key, date, as_of)
        if snapshot_rows:
            rows = _select_rows_for_totals(snapshot_rows)
            source = "snapshot"

    totals = _aggregate_totals(rows)
    kpis = _compute_kpis(totals)
    top_spend = _top_by_spend(daily_rows)
    worst_roas = _worst_by_roas(daily_rows)
    campaign_performance = _campaign_performance_rows(daily_rows, max_rows=500)
    campaign_performance_total = _campaign_performance_count(daily_rows)
    campaign_status_meta = _load_campaign_breakdown_status_meta(session, shop_key, date)
    campaign_breakdown_status = _campaign_breakdown_status_value(
        campaign_status_meta,
        has_campaign_rows=bool(top_spend),
    )
    campaign_breakdown_note = _campaign_breakdown_fallback_note(
        campaign_breakdown_status=campaign_breakdown_status,
        status_meta=campaign_status_meta,
    )
    if not top_spend and campaign_breakdown_status == "supported":
        campaign_breakdown_note = "Không có chiến dịch nào có chi tiêu > 0 trong ngày này."
    latest_snapshot_rows = get_latest_snapshot_rows(
        session,
        shop_key=shop_key,
        date=date,
        as_of=as_of,
        limit=500,
    )
    snapshot_fallback = _empty_snapshot_fallback()
    if not top_spend:
        snapshot_fallback = _build_snapshot_fallback(latest_snapshot_rows, top_n=10)
    campaign_coverage = _campaign_spend_coverage(daily_rows, totals)
    campaign_spend = Decimal("0")
    for row in daily_rows:
        campaign_spend += _to_decimal(row.get("spend"))
    total_spend = _nullable_decimal(totals.get("spend")) or Decimal("0")
    non_product_spend = total_spend - campaign_spend
    if non_product_spend < 0:
        non_product_spend = Decimal("0")
    campaign_performance = _append_unattributed_campaign_row(
        campaign_rows=campaign_performance,
        totals=totals,
    )
    delta = _delta_vs_previous_day(session, shop_key, date, totals)
    benchmark = _build_historical_benchmark(
        session=session,
        shop_key=shop_key,
        date=date,
        totals=totals,
        kpis=kpis,
        as_of=as_of,
    )
    benchmark_7d = _benchmark_window(benchmark, "7d")
    benchmark_30d = _benchmark_window(benchmark, "30d")
    kpi_reference_date = _kpi_threshold_reference_date(date)
    kpi_thresholds = _build_kpi_thresholds(
        session=session,
        shop_key=shop_key,
        reference_date=kpi_reference_date,
        lookback_days=180,
        min_days=45,
    )
    # Do not use configured fallback budgets for report totals.
    # If API budget is unavailable, keep budget as unknown ("-").
    budget_override = None
    campaign_budget_rows = _load_campaign_budget_rows(session, shop_key)
    active_campaign_ids = _active_campaign_ids(daily_rows)
    budget_est, campaigns_budgeted, budget_source = _effective_budget_estimate(
        campaign_budget_rows=campaign_budget_rows,
        active_campaign_ids=active_campaign_ids,
        snapshot_rows=latest_snapshot_rows,
        budget_override=budget_override,
    )
    scorecard = compute_scorecard(totals=totals, kpis=kpis, budget_est=budget_est)
    kpi_evaluation = _evaluate_scorecard_kpis(
        scorecard=scorecard,
        kpi_thresholds=kpi_thresholds,
        intraday=as_of is not None,
    )
    gms_campaigns = _load_gms_campaign_rows(session, shop_key=shop_key, date=date)
    campaign_table_source = "campaign_daily"
    if not top_spend:
        campaign_table_source = (
            "snapshot_fallback"
            if int(snapshot_fallback.get("used") or 0) == 1
            else "none"
        )
    data_sources = {
        "daily_total_source": source,
        "campaign_breakdown_status": campaign_breakdown_status,
        "campaign_table_source": campaign_table_source,
        "budget_source": budget_source,
        "breakdown_scope": BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY,
        "gms_group_scope": GMS_GROUP_SCOPE_AGGREGATE_ONLY,
    }
    cooldown_until_utc = (
        campaign_status_meta.get("cooldown_until_utc") if campaign_status_meta else None
    )
    if cooldown_until_utc:
        data_sources["campaign_breakdown_cooldown_until_utc"] = str(cooldown_until_utc)
    if int(snapshot_fallback.get("used") or 0) == 1:
        data_sources["fallback_source"] = "snapshot"
        rank_key = str(snapshot_fallback.get("rank_key") or "")
        if rank_key:
            data_sources["fallback_rank_key"] = rank_key
        latest_snapshot_at = snapshot_fallback.get("latest_snapshot_at")
        if latest_snapshot_at:
            data_sources["fallback_latest_snapshot_at"] = _fmt_dt(latest_snapshot_at)

    return {
        "shop_key": shop_key,
        "date": date,
        "as_of": as_of,
        "generated_at": datetime.now(as_of.tzinfo if as_of else timezone.utc),
        "data_source": source,
        "row_count": len(rows),
        "totals": totals,
        "kpis": kpis,
        "top_spend": top_spend,
        "worst_roas": worst_roas,
        "campaign_performance": campaign_performance,
        "campaign_performance_total": campaign_performance_total,
        "delta": delta,
        "benchmark": benchmark,
        "benchmark_7d": benchmark_7d,
        "benchmark_30d": benchmark_30d,
        "kpi_thresholds": kpi_thresholds,
        "kpi_evaluation": kpi_evaluation,
        "campaign_breakdown_note": campaign_breakdown_note,
        "campaign_breakdown_status": campaign_breakdown_status,
        "snapshot_fallback": snapshot_fallback,
        "data_sources": data_sources,
        "scorecard": scorecard,
        "budget_est": budget_est,
        "campaigns_budgeted": campaigns_budgeted,
        "budget_source": budget_source,
        "campaign_rows_count": len(daily_rows),
        "campaign_spend_coverage_pct": campaign_coverage,
        "campaign_spend": campaign_spend,
        "non_product_spend": non_product_spend,
        "budget_override": budget_override,
        "gms_campaigns": gms_campaigns,
        "breakdown_scope": BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY,
        "gms_group_scope": GMS_GROUP_SCOPE_AGGREGATE_ONLY,
        "breakdown_scope_note": BREAKDOWN_SCOPE_NOTE,
    }


def load_report_totals_source(
    session,
    *,
    shop_key: str,
    target_date: date_type,
) -> dict[str, Any]:
    rows = _select_rows_for_totals(_load_daily_rows(session, shop_key, target_date))
    totals = _aggregate_totals(rows)
    kpis = _compute_kpis(totals)
    return build_surface_metrics_snapshot(totals=totals, kpis=kpis)


def render_daily_html(data: dict[str, Any]) -> str:
    shop_label = data.get("shop_label") or data.get("shop_key")
    shop_key = str(data.get("shop_key") or "")
    date_str = data["date"].isoformat()
    kind = str(data.get("kind", "final"))
    kind_key = kind.strip().lower()
    generated_at = _fmt_dt(data.get("generated_at"))
    as_of_text = _fmt_dt(data.get("as_of"))
    benchmark_7d = data.get("benchmark_7d") if isinstance(data.get("benchmark_7d"), dict) else {}
    benchmark_30d = data.get("benchmark_30d") if isinstance(data.get("benchmark_30d"), dict) else {}
    scorecard_benchmark, benchmark_label = _select_scorecard_benchmark(
        benchmark_7d=benchmark_7d,
        benchmark_30d=benchmark_30d,
    )
    scorecard_note = "(% so với TB 7 ngày)"
    if benchmark_label == "30d":
        scorecard_note = "(% so với TB 30 ngày)"
    if kind_key == "midday":
        scorecard_note = "(% so với TB 7 ngày cùng mốc giờ)"
        if benchmark_label == "30d":
            scorecard_note = "(% so với TB 30 ngày cùng mốc giờ)"
    meta_segments = [
        f"<div><strong>Ngày:</strong> {date_str} ({kind})</div>",
        f"<div><strong>Tạo lúc:</strong> {generated_at}</div>",
    ]
    if kind_key == "midday" and as_of_text != "-":
        meta_segments.append(f"<div><strong>Mốc dữ liệu:</strong> {as_of_text}</div>")
    meta_line = "<div class='meta-line'>" + "".join(meta_segments) + "</div>"
    scorecard = (
        data.get("scorecard")
        if isinstance(data.get("scorecard"), dict)
        else compute_scorecard(
            totals=data.get("totals") if isinstance(data.get("totals"), dict) else {},
            kpis=data.get("kpis") if isinstance(data.get("kpis"), dict) else {},
            budget_est=None,
        )
    )
    campaign_breakdown_note = data.get("campaign_breakdown_note")
    gms_campaigns = data.get("gms_campaigns") if isinstance(data.get("gms_campaigns"), list) else []
    scope_note = str(data.get("breakdown_scope_note") or BREAKDOWN_SCOPE_NOTE)
    metrics_payload = build_surface_metrics_snapshot(
        totals=data.get("totals") if isinstance(data.get("totals"), dict) else {},
        kpis=data.get("kpis") if isinstance(data.get("kpis"), dict) else {},
    )
    metrics_payload["scope"] = {
        "breakdown_scope": str(
            data.get("breakdown_scope") or BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY
        ),
        "gms_group_scope": str(
            data.get("gms_group_scope") or GMS_GROUP_SCOPE_AGGREGATE_ONLY
        ),
    }
    metrics_payload_json = json.dumps(metrics_payload, ensure_ascii=False, default=str)

    nav_html = (
        _render_report_navigation(
            shop_key=shop_key,
            current_date=data["date"],
            current_kind=str(kind),
        )
        if str(kind).strip().lower() == "final"
        else ""
    )

    lines = [
        "<!doctype html>",
        "<html lang='vi'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>Báo cáo quảng cáo hằng ngày - {shop_label} - {date_str}</title>",
        "<style>",
        ":root{--bg:#f7fafc;--card:#ffffff;--line:#d7e0ea;--text:#0f172a;--muted:#475569;--brand:#0f766e;--brand-soft:#e6fffb;--warn:#b45309}",
        "*{box-sizing:border-box}",
        "body{margin:0;padding:24px;background:linear-gradient(160deg,#f8fbff 0%,#eef7ff 40%,#f6fbf7 100%);color:var(--text);font-family:'Pretendard','Noto Sans KR','Segoe UI',sans-serif}",
        ".report-shell{max-width:1120px;margin:0 auto;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:24px;box-shadow:0 14px 36px rgba(15,23,42,.08)}",
        "h1{font-size:28px;line-height:1.2;margin:0 0 10px 0;letter-spacing:-.3px}",
        "h2{font-size:18px;line-height:1.3;margin:24px 0 10px 0}",
        ".meta-line{display:flex;gap:12px;flex-wrap:wrap;color:var(--muted);font-size:14px;margin-bottom:6px}",
        ".nav-box{border:1px solid var(--line);border-radius:12px;padding:12px;background:#fbfeff;margin:6px 0 16px 0}",
        ".nav-row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:8px 0}",
        ".nav-row label{font-size:12px;color:var(--muted)}",
        ".nav-row select,.nav-row input,.nav-row button{height:36px;border:1px solid var(--line);border-radius:10px;padding:0 10px;background:#fff;color:var(--text);font-size:13px}",
        ".nav-row button{background:var(--brand);border-color:var(--brand);color:#fff;font-weight:700;cursor:pointer}",
        ".nav-hint{font-size:12px;color:var(--muted)}",
        ".section-note{font-size:12px;color:var(--muted);font-weight:500;margin-left:6px}",
        ".delta-suffix{font-size:12px;font-weight:600;margin-left:4px}",
        ".delta-up{color:#0f766e}",
        ".delta-down{color:#b91c1c}",
        ".delta-flat{color:var(--muted)}",
        ".score-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:8px 0 18px 0}",
        ".score-card{border:1px solid var(--line);border-radius:12px;padding:12px;background:var(--brand-soft)}",
        ".score-card .k{font-size:12px;color:var(--muted);margin-bottom:4px}",
        ".score-card .v{font-size:18px;font-weight:700;color:#0b3f3a}",
        ".note{border-left:4px solid var(--warn);padding:10px 12px;background:#fffaf0;color:#7c2d12;border-radius:10px;margin:8px 0 16px 0}",
        "table{width:100%;border-collapse:separate;border-spacing:0;margin-bottom:16px;border:1px solid var(--line);border-radius:12px;overflow:hidden;background:#fff}",
        "th,td{padding:10px 12px;border-bottom:1px solid #e9eef5;text-align:right;vertical-align:top}",
        "th{background:#f8fafc;font-weight:700;font-size:13px;color:#334155}",
        "tr:last-child td{border-bottom:none}",
        "th:first-child,td:first-child{text-align:left}",
        ".score-matrix{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:16px}",
        ".score-metric{border:1px solid var(--line);border-radius:10px;padding:10px;background:#fff}",
        ".score-metric .label{font-size:12px;color:var(--muted);margin-bottom:4px}",
        ".score-metric .value{font-size:16px;font-weight:700;color:#0f172a}",
        ".kpi-chip{display:inline-flex;align-items:center;padding:1px 7px;border-radius:999px;border:1px solid transparent;font-size:10px;font-weight:700;line-height:1.2;margin-left:6px;vertical-align:middle}",
        ".kpi-good{background:#ecfdf5;color:#047857;border-color:#6ee7b7}",
        ".kpi-normal{background:#eff6ff;color:#1d4ed8;border-color:#93c5fd}",
        ".kpi-watch{background:#fffbeb;color:#b45309;border-color:#fcd34d}",
        ".kpi-risk{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}",
        ".kpi-na{background:#f8fafc;color:#64748b;border-color:#cbd5e1}",
        ".table-scroll{overflow-x:auto;margin-bottom:16px}",
        ".table-scroll table{min-width:760px;margin-bottom:0}",
        ".campaign-col{min-width:220px;max-width:300px;text-align:left}",
        ".campaign-name{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;line-height:1.25;max-height:2.5em}",
        ".campaign-id{display:block;font-size:11px;color:var(--muted);margin-top:2px}",
        ".badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:700;border:1px solid transparent}",
        ".badge-excellent{background:#ecfdf5;color:#047857;border-color:#6ee7b7}",
        ".badge-good{background:#eff6ff;color:#1d4ed8;border-color:#93c5fd}",
        ".badge-watch{background:#fffbeb;color:#b45309;border-color:#fcd34d}",
        ".badge-poor{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}",
        ".badge-na{background:#f8fafc;color:#64748b;border-color:#cbd5e1}",
        ".insights-list{margin:0;padding-left:18px}",
        ".insights-list li{margin:6px 0}",
        ".empty{padding:10px 12px;border:1px dashed var(--line);border-radius:10px;background:#fafcff;color:var(--muted)}",
        "small{color:var(--muted)}",
        ".scope-note{margin-top:12px;padding:10px;border:1px dashed var(--line);border-radius:10px;background:#fbfeff;color:var(--muted);font-size:12px}",
        "@media (max-width:900px){.score-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.score-matrix{grid-template-columns:repeat(2,minmax(0,1fr))}}",
        "@media (max-width:640px){body{padding:12px}.report-shell{padding:14px;border-radius:12px}h1{font-size:22px}th,td{padding:8px 9px;font-size:12px}.campaign-col{min-width:170px;max-width:220px}.table-scroll table{min-width:680px}}",
        "</style>",
        "</head>",
        "<body>",
        "<main class='report-shell'>",
        f"<h1>{escape(str(shop_label))} — Báo cáo quảng cáo hằng ngày</h1>",
        meta_line,
        (
            "<script id='dotori-report-metrics' type='application/json'>"
            f"{metrics_payload_json}</script>"
        ),
        nav_html,
        f"<h2>Bảng chỉ số <span class='section-note'>{escape(scorecard_note)}</span></h2>",
        _render_scorecard_table(
            scorecard,
            benchmark_7d=scorecard_benchmark,
            kpi_evaluation=(
                data.get("kpi_evaluation")
                if isinstance(data.get("kpi_evaluation"), dict)
                else {}
            ),
            report_kind=str(kind),
        ),
        _render_scorecard_kpi_legend(data),
        "<h2>Tiến độ ngân sách</h2>",
        _render_budget_progress(
            scorecard,
            budget_source=str(data.get("budget_source") or ""),
            campaign_spend=_nullable_decimal(data.get("campaign_spend")),
            non_product_spend=_nullable_decimal(data.get("non_product_spend")),
            budget_override=_nullable_decimal(data.get("budget_override")),
        ),
        "<h2>Đánh giá trong ngày</h2>",
        _render_daily_evaluation(data),
        "<h2>Hiệu suất chiến dịch</h2>",
        _render_campaign_performance_table(
            rows=data.get("campaign_performance") or [],
            total_count=int(data.get("campaign_performance_total") or 0),
            totals=data.get("totals") if isinstance(data.get("totals"), dict) else None,
            fallback_message=campaign_breakdown_note,
            shop_key=shop_key,
        ),
    ]
    if gms_campaigns:
        lines.extend(
            [
                "<h2>Chiến dịch Group/GMS (nếu có dữ liệu)</h2>",
                _render_gms_campaigns_table(gms_campaigns),
            ]
        )

    snapshot_fallback = data.get("snapshot_fallback") or {}
    snapshot_rows = snapshot_fallback.get("rows") or []
    if snapshot_rows:
        lines.extend(
            [
                f"<h2>{_snapshot_fallback_heading(str(snapshot_fallback.get('rank_key') or 'spend'))}</h2>",
                _render_snapshot_fallback_table(snapshot_rows, shop_key=shop_key),
            ]
        )

    lines.extend(["</main>", "</body>", "</html>"])
    lines.insert(-3, f"<div class='scope-note'>{escape(scope_note)}</div>")
    return "\n".join(lines)


def write_report_file(
    shop_key: str, date: date_type, kind: str, html: str
) -> Path:
    settings = get_settings()
    base_dir = Path(settings.reports_dir)
    target_dir = base_dir / shop_key / "daily"
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{date.isoformat()}_{kind}.html"
    path = target_dir / filename
    path.write_text(html, encoding="utf-8")
    return path


def build_discord_summary(data: dict[str, Any], report_url: str | None) -> str:
    metrics = build_surface_metrics_snapshot(
        totals=data.get("totals") if isinstance(data.get("totals"), dict) else {},
        kpis=data.get("kpis") if isinstance(data.get("kpis"), dict) else {},
    )
    date_str = data["date"].isoformat()
    kind = data.get("kind", "final")
    row_count = int(data.get("row_count") or 0)
    if row_count <= 0:
        summary = f"Báo cáo Ads {kind} {date_str}: no_data=1 rows=0"
    else:
        summary = (
            f"Báo cáo Ads {kind} {date_str}: spend={_fmt_money_h(metrics['spend'])}, "
            f"impressions={_fmt_int_h(metrics['impressions'])}, "
            f"clicks={_fmt_int_h(metrics['clicks'])}, "
            f"orders={_fmt_int_h(metrics['orders'])}, "
            f"gmv={_fmt_money_h(metrics['gmv'])}, "
            f"ROAS={_fmt_ratio(metrics['roas'])}, CTR={_fmt_pct(metrics['ctr'])}"
        )
    gms_rows = data.get("gms_campaigns") if isinstance(data.get("gms_campaigns"), list) else []
    if gms_rows:
        summary += f" | gms_campaigns={len(gms_rows)}"
    if report_url:
        summary += f" | {report_url}"
    return summary


def report_surface_metrics(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return build_surface_metrics_snapshot(totals={}, kpis={})
    return build_surface_metrics_snapshot(
        totals=data.get("totals") if isinstance(data.get("totals"), dict) else {},
        kpis=data.get("kpis") if isinstance(data.get("kpis"), dict) else {},
    )


def report_scope_line(data: dict[str, Any] | None) -> str:
    if not isinstance(data, dict):
        return BREAKDOWN_SCOPE_NOTE
    breakdown_scope = str(
        data.get("breakdown_scope") or BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY
    )
    gms_group_scope = str(
        data.get("gms_group_scope") or GMS_GROUP_SCOPE_AGGREGATE_ONLY
    )
    if (
        breakdown_scope == BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY
        and gms_group_scope == GMS_GROUP_SCOPE_AGGREGATE_ONLY
    ):
        return BREAKDOWN_SCOPE_NOTE
    return (
        f"Campaign breakdown scope={breakdown_scope}; "
        f"GMS/Group scope={gms_group_scope}"
    )


def _render_report_navigation(
    *,
    shop_key: str,
    current_date: date_type,
    current_kind: str,
) -> str:
    catalog = _load_report_navigation_catalog(shop_key=shop_key)
    final_dates = catalog.get("daily_final_dates") or []

    daily_min = ""
    daily_max = ""
    if final_dates:
        daily_min = final_dates[0]
        daily_max = final_dates[-1]

    catalog_json = json.dumps(catalog, ensure_ascii=False)
    lines = [
        "<section class='nav-box'>",
        "<div><strong>Xem nhanh báo cáo ngày (final)</strong></div>",
        "<div class='nav-row'>",
        "<label for='report-nav-daily-date'>Ngày</label>",
        (
            f"<input id='report-nav-daily-date' type='date' value='{current_date.isoformat()}' "
            f"min='{daily_min}' max='{daily_max}'>"
        ),
        "<button id='report-nav-open-daily' type='button'>Xem</button>",
        "</div>",
        "<div class='nav-hint' id='report-nav-hint'></div>",
        "<script>",
        f"const reportNavShopKey={json.dumps(shop_key, ensure_ascii=False)};",
        f"const reportNavCatalog={catalog_json};",
        "function reportNavWithToken(path){const p=new URLSearchParams(window.location.search);const t=p.get('token');if(!t){return path;}return `${path}?token=${encodeURIComponent(t)}`;}",
        "function reportNavSetHint(msg){const el=document.getElementById('report-nav-hint');if(el){el.textContent=msg||'';}}",
        "function reportNavOpenDaily(){const dateEl=document.getElementById('report-nav-daily-date');if(!dateEl){return;}const dateValue=dateEl.value;if(!dateValue){reportNavSetHint('Hãy chọn ngày trước.');return;}const available=(reportNavCatalog.daily_final_dates||[]);if(available.length>0 && !available.includes(dateValue)){reportNavSetHint(`Không có file báo cáo final cho ${dateValue}.`);return;}window.location.href=reportNavWithToken(`/reports/${reportNavShopKey}/daily/${dateValue}_final.html`);}",
        "document.getElementById('report-nav-open-daily')?.addEventListener('click', reportNavOpenDaily);",
        "</script>",
        "</section>",
    ]
    return "\n".join(lines)


def _load_report_navigation_catalog(*, shop_key: str) -> dict[str, list[str]]:
    settings = get_settings()
    reports_root = Path(settings.reports_dir)
    shop_root = reports_root / shop_key
    daily_root = shop_root / "daily"
    weekly_root = shop_root / "weekly"

    daily_final_dates: set[str] = set()
    daily_midday_dates: set[str] = set()
    weekly_ids: set[str] = set()

    if daily_root.exists():
        for path in daily_root.glob("*_final.html"):
            name = path.name
            if len(name) >= 16:
                daily_final_dates.add(name[:10])
        for path in daily_root.glob("*_midday.html"):
            name = path.name
            if len(name) >= 17:
                daily_midday_dates.add(name[:10])

    if weekly_root.exists():
        for path in weekly_root.glob("*.html"):
            if path.stem:
                weekly_ids.add(path.stem)

    return {
        "daily_final_dates": sorted(daily_final_dates),
        "daily_midday_dates": sorted(daily_midday_dates),
        "weekly_ids": sorted(weekly_ids, reverse=True),
    }


def _render_campaign_table(
    rows: list[dict[str, Any]],
    fallback_message: str | None = None,
    *,
    shop_key: str | None = None,
) -> str:
    if not rows:
        if fallback_message:
            return f"<div>{escape(str(fallback_message))}</div>"
        return (
            "<div>"
            "Không có breakdown theo chiến dịch cho ngày này "
            "(API chỉ trả tổng cấp shop)."
            "</div>"
        )
    lines = [
        "<table>",
        "<tr><th>#</th><th>Sản phẩm / Chiến dịch</th><th>ID chiến dịch</th><th>Chi tiêu</th><th>ROAS</th></tr>",
    ]
    for idx, row in enumerate(rows, start=1):
        campaign_id = _display_campaign_id(row.get("campaign_id"))
        display_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name=row.get("campaign_name"),
        )
        lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{escape(display_name)}</td>"
            f"<td>{escape(campaign_id)}</td>"
            f"<td>{_fmt_money(row['spend'])}</td>"
            f"<td>{_fmt_ratio(row['roas'])}</td>"
            "</tr>"
        )
    lines.append("</table>")
    return "\n".join(lines)


def _render_campaign_performance_table(
    *,
    rows: list[dict[str, Any]],
    total_count: int,
    totals: dict[str, Any] | None = None,
    fallback_message: str | None = None,
    shop_key: str | None = None,
) -> str:
    if not rows:
        if fallback_message:
            return f"<div>{escape(str(fallback_message))}</div>"
        return "<div>Không có chiến dịch nào chi tiêu &gt; 0 trong ngày này.</div>"
    lines = [
        "<div class='table-scroll'>",
        "<table>",
        "<tr><th>#</th><th>Chiến dịch</th><th>Chi tiêu</th><th>GMV</th><th>Đơn hàng</th><th>ROAS</th><th>Nhãn đánh giá</th><th>CTR</th><th>CVR</th></tr>",
    ]
    for idx, row in enumerate(rows, start=1):
        campaign_id = _display_campaign_id(row.get("campaign_id"))
        display_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name=row.get("campaign_name"),
        )
        safe_full = escape(display_name)
        lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            "<td class='campaign-col'>"
            f"<span class='campaign-name' title='{safe_full}'>{safe_full}</span>"
            f"<span class='campaign-id'>{escape(campaign_id)}</span>"
            "</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('spend')))}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('gmv')))}</td>"
            f"<td>{_fmt_int(int(row.get('orders') or 0))}</td>"
            f"<td>{_fmt_ratio(_nullable_decimal(row.get('roas')))}</td>"
            f"<td>{_render_performance_badge(row)}</td>"
            f"<td>{_fmt_pct_compact(_nullable_decimal(row.get('ctr')))}</td>"
            f"<td>{_fmt_pct_compact(_nullable_decimal(row.get('cvr')))}</td>"
            "</tr>"
        )
    lines.append("</table>")
    lines.append("</div>")
    return "\n".join(lines)


def _render_gms_campaigns_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    lines = [
        "<div class='table-scroll'>",
        "<table>",
        "<tr><th>#</th><th>Chiến dịch Group/GMS</th><th>Loại</th><th>Ngân sách/ngày</th><th>Tổng ngân sách</th><th>Đã chi</th></tr>",
    ]
    for idx, row in enumerate(rows, start=1):
        campaign_id = str(row.get("campaign_id") or "").strip()
        campaign_name = str(row.get("campaign_name") or "").strip() or campaign_id
        campaign_type = str(row.get("campaign_type") or "gms").strip()
        lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            "<td class='campaign-col'>"
            f"<span class='campaign-name'>{escape(campaign_name)}</span>"
            f"<span class='campaign-id'>{escape(campaign_id)}</span>"
            "</td>"
            f"<td>{escape(campaign_type)}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('daily_budget')))}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('total_budget')))}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('spend')))}</td>"
            "</tr>"
        )
    lines.append("</table>")
    lines.append("</div>")
    return "\n".join(lines)


def _render_daily_evaluation(data: dict[str, Any]) -> str:
    lines = _build_daily_evaluation_lines(data)
    if not lines:
        return "<div class='empty'>(không có đánh giá)</div>"
    out = ["<ul class='insights-list'>"]
    for item in lines:
        out.append(f"<li>{escape(str(item))}</li>")
    out.append("</ul>")
    return "\n".join(out)


def _build_daily_evaluation_lines(data: dict[str, Any]) -> list[str]:
    totals = data.get("totals") if isinstance(data.get("totals"), dict) else {}
    kpis = data.get("kpis") if isinstance(data.get("kpis"), dict) else {}
    scorecard = data.get("scorecard") if isinstance(data.get("scorecard"), dict) else {}
    benchmark_7d = data.get("benchmark_7d") if isinstance(data.get("benchmark_7d"), dict) else {}
    benchmark = data.get("benchmark") if isinstance(data.get("benchmark"), dict) else {}
    delta_map = _scorecard_delta_map(scorecard=scorecard, benchmark_7d=benchmark_7d)
    shop_key = str(data.get("shop_key") or "")
    report_kind = str(data.get("kind") or "").strip().lower()
    campaign_rows = data.get("campaign_performance") if isinstance(data.get("campaign_performance"), list) else []
    kpi_eval = data.get("kpi_evaluation") if isinstance(data.get("kpi_evaluation"), dict) else {}

    insights: list[str] = []
    spend = _nullable_decimal(totals.get("spend")) or Decimal("0")
    orders = int(totals.get("orders") or 0)
    if report_kind == "midday":
        cutoff_text = _fmt_dt(data.get("as_of"))
        if cutoff_text == "-":
            cutoff_text = "mốc hiện tại"
        baseline_days = int(benchmark_7d.get("days_available") or 0)
        baseline_basis = str(benchmark.get("basis") or "").strip().lower()
        if baseline_basis == "intraday_snapshot":
            insights.append(
                f"Trạng thái midday: giá trị lũy kế đến {cutoff_text}, so với snapshot cùng giờ trong 7 ngày gần nhất ({baseline_days} ngày)."
            )
        else:
            insights.append(
                f"Trạng thái midday: giá trị lũy kế đến {cutoff_text}, dữ liệu lịch sử cùng giờ chưa đủ nên độ tin cậy so sánh thấp."
            )

    if spend <= 0:
        if report_kind == "midday":
            return insights + ["Chưa ghi nhận chi tiêu tại mốc giờ hiện tại."]
        return ["Không ghi nhận chi tiêu trong ngày này."]

    status_map: dict[str, str] = {}
    quality_bits: list[str] = []
    volume_bits: list[str] = []
    risk_metrics: list[str] = []
    for key, label, group in (
        ("roas", "ROAS", "quality"),
        ("ctr", "CTR", "quality"),
        ("cvr", "CVR", "quality"),
        ("cpc", "CPC", "quality"),
        ("gmv", "GMV", "volume"),
        ("orders", "Orders", "volume"),
        ("clicks", "Clicks", "volume"),
        ("impressions", "Impressions", "volume"),
    ):
        status = _metric_eval_status(kpi_eval, key)
        if not status or status == "n/a":
            continue
        status_map[key] = status
        pretty = {
            "good": "tốt",
            "normal": "ổn",
            "watch": "cảnh báo",
            "risk": "rủi ro",
            "n/a": "n/a",
        }.get(status, status)
        if group == "quality":
            quality_bits.append(f"{label} {pretty}")
        else:
            volume_bits.append(f"{label} {pretty}")
        if status == "risk":
            risk_metrics.append(label.upper())

    if quality_bits:
        insights.append("KPI chất lượng: " + ", ".join(quality_bits) + ".")
    if volume_bits:
        insights.append("KPI quy mô: " + ", ".join(volume_bits) + ".")

    quality_risk_count = sum(
        1 for key in ("roas", "ctr", "cvr", "cpc") if status_map.get(key) == "risk"
    )
    volume_risk_count = sum(
        1
        for key in ("gmv", "orders", "clicks", "impressions")
        if status_map.get(key) == "risk"
    )
    if quality_risk_count >= 2:
        insights.append("Rủi ro chất lượng mang tính hệ thống hôm nay (>=2 KPI chất lượng đang rủi ro).")
    elif quality_risk_count == 1:
        insights.append("Có 1 KPI chất lượng đang rủi ro; ưu tiên tối ưu chỉ số này trước.")
    elif quality_bits:
        insights.append("Nhóm KPI chất lượng đang ở mức ổn định.")

    if volume_risk_count >= 2:
        insights.append("Quy mo dang thap hon muc tieu tren nhieu tin hieu; uu tien mo rong reach truoc.")
    elif volume_risk_count == 1:
        insights.append("Có 1 KPI quy mô đang rủi ro; cần theo dõi sát traffic và đơn hàng.")

    ctr_status = status_map.get("ctr")
    cvr_status = status_map.get("cvr")
    cpc_status = status_map.get("cpc")
    roas_status = status_map.get("roas")
    spend_status = status_map.get("spend")
    if ctr_status in {"good", "normal"} and cvr_status in {"watch", "risk"}:
        insights.append("Traffic sau click giảm chất lượng (CTR tốt, CVR yếu) - kiểm tra trang sản phẩm/ưu đãi.")
    if ctr_status in {"watch", "risk"} and cvr_status in {"good", "normal"}:
        insights.append("Chất lượng chuyển đổi tạm ổn nhưng traffic đầu vào yếu - cần làm mới creative.")
    if cpc_status == "risk" and ctr_status in {"watch", "risk"}:
        insights.append("Chi phí cao và phản hồi yếu - cần siết target và giá thầu.")
    if roas_status == "good" and spend_status in {"watch", "risk"}:
        insights.append("Hiệu quả tốt nhưng chi tiêu đang hạn chế - cân nhắc tăng ngân sách có kiểm soát.")
    if roas_status == "risk" and spend_status in {"good", "normal"}:
        insights.append("Đã có chi tiêu nhưng hiệu quả yếu - ưu tiên cắt nhóm có hoàn vốn thấp.")

    roas_delta = delta_map.get("roas")
    spend_delta = delta_map.get("spend")
    orders_delta = delta_map.get("orders")
    clicks_delta = delta_map.get("clicks")
    if _is_material_delta(spend_delta, Decimal("0.08")) and _is_material_delta(
        roas_delta, Decimal("0.05")
    ):
        if spend_delta and roas_delta and spend_delta > 0 and roas_delta < 0:
            context_label = (
                "TB 7 ngày cùng mốc giờ" if report_kind == "midday" else "TB 7 ngày"
            )
            insights.append(
                f"Chi tiêu tăng nhưng ROAS giảm so với {context_label} - cần phân bổ ngân sách chọn lọc hơn."
            )
        elif spend_delta and roas_delta and spend_delta < 0 and roas_delta > 0:
            context_label = (
                "TB 7 ngày cùng mốc giờ" if report_kind == "midday" else "TB 7 ngày"
            )
            insights.append(
                f"Chi tiêu giảm nhưng ROAS tăng so với {context_label} - chiến lược cắt lọc hiện tại đang hiệu quả."
            )
    if _is_material_delta(clicks_delta, Decimal("0.08")) and _is_material_delta(
        orders_delta, Decimal("0.08")
    ):
        if clicks_delta and orders_delta and clicks_delta > 0 and orders_delta < 0:
            insights.append("Clicks tăng nhưng đơn hàng giảm - cần tối ưu gap phễu chuyển đổi ngay lập tức.")

    product_rows = [
        row
        for row in campaign_rows
        if not _is_non_product_pool_row(row)
        and (_nullable_decimal(row.get("spend")) or Decimal("0")) > 0
    ]
    no_order_spend = sum(
        (_nullable_decimal(row.get("spend")) or Decimal("0"))
        for row in product_rows
        if int(row.get("orders") or 0) <= 0
    )
    if spend > 0:
        no_order_share = _safe_div(no_order_spend, spend)
        if no_order_share is not None and no_order_share >= Decimal("0.25"):
            insights.append(
                f"Tỷ trọng chi tiêu không có đơn hàng cao ({_fmt_pct_compact(no_order_share)} tổng chi tiêu)."
            )

    if product_rows:
        top = max(
            product_rows,
            key=lambda row: _nullable_decimal(row.get("spend")) or Decimal("0"),
        )
        campaign_id = _display_campaign_id(top.get("campaign_id"))
        campaign_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name=top.get("campaign_name"),
        )
        insights.append(
            f"Chiến dịch chi tiêu cao nhất: {campaign_name} ({_fmt_money(_nullable_decimal(top.get('spend')))}, ROAS {_fmt_ratio(_nullable_decimal(top.get('roas')))})."
        )

    non_product = next((row for row in campaign_rows if _is_non_product_pool_row(row)), None)
    if non_product is not None and spend > 0:
        non_product_spend = _nullable_decimal(non_product.get("spend")) or Decimal("0")
        share = _safe_div(non_product_spend, spend)
        if share is not None and share > 0:
            insights.append(
                f"Nhóm Group/Shop/Auto chiếm {_fmt_pct_compact(share)} tổng chi tiêu trong ngày."
            )
            if share >= Decimal("0.40"):
                insights.append(
                    "Ty trong non-product cao; can doc hieu suat campaign san pham kem boi canh nay."
                )

    if report_kind == "midday":
        if orders <= 0:
            insights.append("Chưa có đơn hàng tại mốc hiện tại; theo dõi sát sự cải thiện trước bản final.")
        elif orders <= 2:
            insights.append(f"Số đơn hàng tại mốc hiện tại vẫn thấp ({orders}); cần theo dõi nhiệt độ chuyển đổi buổi chiều.")
        else:
            insights.append(f"Số đơn hàng tại mốc hiện tại ở mức tốt ({orders}) cho tiến độ trong ngày.")
    else:
        if orders <= 0:
            insights.append("Không ghi nhận đơn hàng dù đã có chi tiêu quảng cáo.")
        elif orders <= 2:
            insights.append("Số đơn hàng thấp; cần theo dõi chất lượng chuyển đổi.")
        else:
            insights.append(f"Số đơn hàng ở mức tốt ({orders}) cho cả ngày.")

    # Operations closing lines for quick operator actions.
    close_actions: list[str] = []
    if risk_metrics:
        close_actions.append("Ưu tiên: ổn định lại " + ", ".join(risk_metrics) + ".")
    if status_map.get("roas") == "good" and status_map.get("orders") in {"good", "normal"}:
        close_actions.append("Hành động: duy trì nhóm đang tốt và thử tăng quy mô từng bước.")
    if status_map.get("cvr") in {"watch", "risk"}:
        close_actions.append("Hành động: tập trung cải thiện luồng chuyển đổi (PDP, giá, trust signal).")
    if status_map.get("ctr") in {"watch", "risk"}:
        close_actions.append("Hành động: làm mới creative/keyword để phục hồi traffic đầu vào.")
    if not close_actions:
        close_actions.append("Hành động: giữ nguyên cấu hình hiện tại và theo dõi độ ổn định ngày tiếp theo.")
    insights.extend("Tổng kết vận hành - " + item for item in close_actions[:2])

    deduped: list[str] = []
    seen: set[str] = set()
    for item in insights:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    closing = [item for item in deduped if item.startswith("Tổng kết vận hành -")]
    core = [item for item in deduped if not item.startswith("Tổng kết vận hành -")]
    out = core[:8]
    for item in closing[:2]:
        if len(out) >= 10:
            break
        out.append(item)
    return out[:10]


def _render_performance_badge(row: dict[str, Any]) -> str:
    label, badge_class = _performance_badge_meta(row)
    return f"<span class='badge {badge_class}'>{escape(label)}</span>"


def _metric_eval_status(kpi_eval: dict[str, Any], key: str) -> str:
    metric_eval = kpi_eval.get(key) if isinstance(kpi_eval.get(key), dict) else {}
    status = str(metric_eval.get("status") or "").strip().lower()
    return status


def _is_material_delta(value: Decimal | None, threshold: Decimal) -> bool:
    if value is None:
        return False
    return abs(value) >= abs(threshold)


def _performance_badge_meta(row: dict[str, Any]) -> tuple[str, str]:
    band = str(row.get("roas_band") or "").strip().lower()
    if not band:
        band = _roas_band(_nullable_decimal(row.get("roas")))
    if band == "excellent":
        return "Xuất sắc", "badge-excellent"
    if band == "good":
        return "Tốt", "badge-good"
    if band == "watch":
        return "Cảnh báo", "badge-watch"
    if band == "poor":
        return "Rủi ro", "badge-poor"
    return "Không dữ liệu", "badge-na"


def _render_today_vs_avg_line(data: dict[str, Any]) -> str:
    window_7d = data.get("benchmark_7d")
    window_30d = data.get("benchmark_30d")
    days_7d = int(window_7d.get("days_available") or 0) if isinstance(window_7d, dict) else 0
    days_30d = int(window_30d.get("days_available") or 0) if isinstance(window_30d, dict) else 0
    status_7d = "du" if days_7d >= 7 else "thiếu"
    status_30d = "du" if days_30d >= 30 else "thiếu"
    return (
        "<div><small>"
        f"TB 7 ngày: số ngày={days_7d} ({status_7d}) | TB 30 ngày: số ngày={days_30d} ({status_30d})"
        "</small></div>"
    )


def _render_today_vs_avg_table(data: dict[str, Any]) -> str:
    totals = data.get("totals") if isinstance(data.get("totals"), dict) else {}
    kpis = data.get("kpis") if isinstance(data.get("kpis"), dict) else {}
    window_7d = data.get("benchmark_7d") if isinstance(data.get("benchmark_7d"), dict) else {}
    window_30d = (
        data.get("benchmark_30d") if isinstance(data.get("benchmark_30d"), dict) else {}
    )
    lines = [
        "<table>",
        "<tr><th>Chỉ số</th><th>Hôm nay</th><th>TB 7 ngày</th><th>TB 30 ngày</th></tr>",
        (
            "<tr><td>Chi tiêu</td>"
            f"<td>{_fmt_money(_nullable_decimal(totals.get('spend')))}</td>"
            f"<td>{_fmt_money(_window_avg_value(window_7d, 'spend_avg', min_days=7))}</td>"
            f"<td>{_fmt_money(_window_avg_value(window_30d, 'spend_avg', min_days=30))}</td>"
            "</tr>"
        ),
        (
            "<tr><td>GMV</td>"
            f"<td>{_fmt_money(_nullable_decimal(totals.get('gmv')))}</td>"
            f"<td>{_fmt_money(_window_avg_value(window_7d, 'gmv_avg', min_days=7))}</td>"
            f"<td>{_fmt_money(_window_avg_value(window_30d, 'gmv_avg', min_days=30))}</td>"
            "</tr>"
        ),
        (
            "<tr><td>Đơn hàng</td>"
            f"<td>{_fmt_int(int(totals.get('orders') or 0))}</td>"
            f"<td>{_fmt_ratio(_window_avg_value(window_7d, 'orders_avg', min_days=7))}</td>"
            f"<td>{_fmt_ratio(_window_avg_value(window_30d, 'orders_avg', min_days=30))}</td>"
            "</tr>"
        ),
        (
            "<tr><td>ROAS</td>"
            f"<td>{_fmt_ratio(_nullable_decimal(kpis.get('roas')))}</td>"
            f"<td>{_fmt_ratio(_window_avg_value(window_7d, 'roas_avg', min_days=7))}</td>"
            f"<td>{_fmt_ratio(_window_avg_value(window_30d, 'roas_avg', min_days=30))}</td>"
            "</tr>"
        ),
        "</table>",
    ]
    return "\n".join(lines)


def _window_avg_value(window: dict[str, Any], key: str, *, min_days: int) -> Decimal | None:
    days_available = int(window.get("days_available") or 0)
    if days_available < max(min_days, 1):
        return None
    return _nullable_decimal(window.get(key))


def _render_campaign_coverage_note(
    *,
    rows: list[dict[str, Any]],
    totals: dict[str, Any] | None,
) -> str:
    if not totals:
        return ""
    total_spend = _nullable_decimal(totals.get("spend"))
    if total_spend is None or total_spend <= 0:
        return ""
    attributed_rows = [row for row in rows if not _is_non_product_pool_row(row)]
    attributed_spend = sum(
        (_nullable_decimal(row.get("spend")) or Decimal("0")) for row in attributed_rows
    )
    coverage = _safe_div(attributed_spend, total_spend)
    gap = total_spend - attributed_spend
    return (
        "<div><small>"
        f"Độ phủ chi tiêu: {_fmt_pct_compact(coverage)} "
        f"({_fmt_money(attributed_spend)} / {_fmt_money(total_spend)})"
        + (
            (
                f" | Tổng hợp Group/Shop/Auto: {_fmt_money(gap)} "
                "(ngoài dữ liệu API campaign sản phẩm)"
            )
            if gap > Decimal("0")
            else ""
        )
        + "</small></div>"
    )


def _trim_text(value: str, *, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max(max_chars - 1, 1)].rstrip() + "..."


def _is_non_product_pool_row(row: dict[str, Any]) -> bool:
    campaign_id = str(row.get("campaign_id") or "").strip().upper()
    return campaign_id in {"UNATTRIBUTED", "NON_PRODUCT_POOL"}


def _render_data_sources_line(data: dict[str, Any]) -> str:
    data_sources = data.get("data_sources") or {}
    daily_total_source = str(data_sources.get("daily_total_source") or data.get("data_source") or "-")
    campaign_breakdown_status = str(
        data_sources.get("campaign_breakdown_status")
        or data.get("campaign_breakdown_status")
        or "unknown"
    )
    segments = [
        f"daily_total_source={daily_total_source}",
        f"campaign_breakdown_status={campaign_breakdown_status}",
    ]
    campaign_table_source = str(data_sources.get("campaign_table_source") or "")
    if campaign_table_source:
        segments.append(f"campaign_table_source={campaign_table_source}")
    breakdown_scope = str(
        data_sources.get("breakdown_scope")
        or data.get("breakdown_scope")
        or BREAKDOWN_SCOPE_PRODUCT_LEVEL_ONLY
    )
    gms_group_scope = str(
        data_sources.get("gms_group_scope")
        or data.get("gms_group_scope")
        or GMS_GROUP_SCOPE_AGGREGATE_ONLY
    )
    segments.append(f"breakdown_scope={breakdown_scope}")
    segments.append(f"gms_group_scope={gms_group_scope}")
    budget_source = str(data_sources.get("budget_source") or data.get("budget_source") or "")
    if budget_source:
        segments.append(f"budget_source={budget_source}")
    fallback_source = str(data_sources.get("fallback_source") or "")
    if fallback_source:
        segments.append(f"fallback_source={fallback_source}")
    fallback_rank_key = str(data_sources.get("fallback_rank_key") or "")
    if fallback_rank_key:
        segments.append(f"fallback_rank_key={fallback_rank_key}")
    fallback_latest_snapshot_at = str(data_sources.get("fallback_latest_snapshot_at") or "")
    if fallback_latest_snapshot_at:
        segments.append(f"fallback_latest_snapshot_at={fallback_latest_snapshot_at}")
    cooldown_until_utc = str(data_sources.get("campaign_breakdown_cooldown_until_utc") or "")
    if cooldown_until_utc:
        segments.append(f"cooldown_until_utc={cooldown_until_utc}")
    return f"<div><small>Nguồn dữ liệu: {' | '.join(segments)}</small></div>"


def _render_campaign_coverage_line(data: dict[str, Any]) -> str:
    coverage = _nullable_decimal(data.get("campaign_spend_coverage_pct"))
    campaign_rows_count = int(data.get("campaign_rows_count") or 0)
    if coverage is None:
        return ""
    pct_text = _fmt_pct_compact(coverage)
    # Keep this explicitly visible because campaign breakdown endpoints can be partial.
    return (
        "<div><small>"
        f"Độ phủ bằng campaign: {pct_text} tổng chi tiêu "
        f"(campaign_rows={campaign_rows_count})"
        "</small></div>"
    )


def _render_scorecard_table(
    scorecard: dict[str, Any],
    *,
    benchmark_7d: dict[str, Any] | None = None,
    kpi_evaluation: dict[str, Any] | None = None,
    report_kind: str = "final",
) -> str:
    delta_map = _scorecard_delta_map(scorecard=scorecard, benchmark_7d=benchmark_7d or {})
    eval_map = kpi_evaluation if isinstance(kpi_evaluation, dict) else {}
    items: list[tuple[str, str, str, Decimal | None, Decimal | None]] = [
        (
            "spend",
            "Chi tiêu",
            _fmt_money(scorecard.get("spend")),
            delta_map.get("spend"),
            _nullable_decimal(scorecard.get("spend")),
        ),
        (
            "gmv",
            "GMV",
            _fmt_money(scorecard.get("gmv")),
            delta_map.get("gmv"),
            _nullable_decimal(scorecard.get("gmv")),
        ),
        (
            "roas",
            "ROAS",
            _fmt_ratio(scorecard.get("roas")),
            delta_map.get("roas"),
            _nullable_decimal(scorecard.get("roas")),
        ),
        (
            "orders",
            "Đơn hàng",
            _fmt_int(scorecard.get("orders")),
            delta_map.get("orders"),
            _to_decimal(scorecard.get("orders")),
        ),
        (
            "ctr",
            "CTR",
            _fmt_pct_compact(scorecard.get("ctr")),
            delta_map.get("ctr"),
            _nullable_decimal(scorecard.get("ctr")),
        ),
        (
            "cvr",
            "CVR",
            _fmt_pct_compact(scorecard.get("cvr")),
            delta_map.get("cvr"),
            _nullable_decimal(scorecard.get("cvr")),
        ),
        (
            "cpc",
            "CPC",
            _fmt_money(scorecard.get("cpc")),
            delta_map.get("cpc"),
            _nullable_decimal(scorecard.get("cpc")),
        ),
        (
            "clicks",
            "Luot click",
            _fmt_int(scorecard.get("clicks")),
            delta_map.get("clicks"),
            _to_decimal(scorecard.get("clicks")),
        ),
        (
            "impressions",
            "Luot hien thi",
            _fmt_int(scorecard.get("impressions")),
            delta_map.get("impressions"),
            _to_decimal(scorecard.get("impressions")),
        ),
    ]
    lines = ["<div class='score-matrix'>"]
    for metric_key, label, value, delta, raw_value in items:
        metric_eval = eval_map.get(metric_key) if isinstance(eval_map.get(metric_key), dict) else {}
        metric_eval = _resolve_scorecard_metric_eval(
            metric_key=metric_key,
            metric_eval=metric_eval,
            delta=delta,
            value=raw_value,
        )
        lines.append(
            "<div class='score-metric'>"
            f"<div class='label'>{escape(label)}</div>"
            "<div class='value'>"
            f"{_render_value_with_delta(value, delta, metric_key=metric_key)}"
            f"{_render_kpi_chip(metric_eval)}"
            "</div>"
            "</div>"
        )
    lines.append("</div>")
    return "\n".join(lines)


def _render_scorecard_kpi_legend(data: dict[str, Any]) -> str:
    thresholds = data.get("kpi_thresholds") if isinstance(data.get("kpi_thresholds"), dict) else {}
    lookback_days = int(thresholds.get("lookback_days") or 180)
    kind = str(data.get("kind") or "final").strip().lower()
    benchmark_7d = data.get("benchmark_7d") if isinstance(data.get("benchmark_7d"), dict) else {}
    compare_days = int(benchmark_7d.get("days_available") or 0)
    detail = (
        "Huy hiệu KPI: Tốt / Ổn / Cảnh báo / Rủi ro"
        f" | Cửa sổ KPI: {lookback_days} ngày gần nhất"
    )
    if kind == "midday":
        detail += " | Midday: chỉ áp dụng ROAS/CTR/CVR/CPC"
        if compare_days > 0:
            detail += f" | Số ngày so sánh cùng mốc giờ: {compare_days}"
    return f"<div><small>{escape(detail)}</small></div>"


def _render_budget_progress(
    scorecard: dict[str, Any],
    *,
    budget_source: str = "",
    campaign_spend: Decimal | None = None,
    non_product_spend: Decimal | None = None,
    budget_override: Decimal | None = None,
) -> str:
    source_key = str(budget_source or "").strip().lower()
    budget_est = _nullable_decimal(scorecard.get("budget_est"))
    total_spend = _nullable_decimal(scorecard.get("spend")) or Decimal("0")
    visible_campaign_spend = campaign_spend
    if visible_campaign_spend is None:
        visible_campaign_spend = total_spend
    hidden_non_product_spend = non_product_spend
    if hidden_non_product_spend is None:
        hidden_non_product_spend = Decimal("0")

    lines: list[str] = []
    has_api_budget = budget_est is not None and source_key in {"campaign_sum", "snapshot"}
    if has_api_budget:
        consumed_visible = _safe_div(visible_campaign_spend, budget_est)
        source_label = _budget_source_label(budget_source)
        lines.append(
            "<div><strong>Tổng ngân sách (campaign có budget API):</strong> "
            f"{_fmt_money(budget_est)} | <strong>Đã dùng:</strong> {_fmt_money(visible_campaign_spend)} | "
            f"<strong>Tỷ lệ dùng:</strong> {_fmt_pct_compact(consumed_visible)}"
            + (f" | <strong>Nguồn:</strong> {escape(source_label)}" if source_label else "")
            + "</div>"
        )
        if hidden_non_product_spend > 0:
            lines.append(
                "<div><small>"
                f"Chi tiêu ngoài product-level: {_fmt_money(hidden_non_product_spend)} "
                "(Group/Shop/Auto). OpenAPI chưa có budget tương ứng cho nhóm này."
                "</small></div>"
            )

    if not lines:
        lines.append(
            "<div class='empty'><strong>Tổng ngân sách:</strong> - "
            "(không có dữ liệu API tin cậy cho ngày này).</div>"
        )
    return "\n".join(lines)


def _budget_source_label(value: str) -> str:
    key = str(value or "").strip().lower()
    if key == "campaign_sum":
        return "campaign setting API"
    if key == "snapshot":
        return "snapshot campaign budgets"
    if key == "none":
        return ""
    return key


def _scorecard_delta_map(
    *,
    scorecard: dict[str, Any],
    benchmark_7d: dict[str, Any],
) -> dict[str, Decimal | None]:
    days_available = int(benchmark_7d.get("days_available") or 0)
    if days_available <= 0:
        return {}
    precomputed = {
        "spend": _nullable_decimal(benchmark_7d.get("spend_delta_pct")),
        "gmv": _nullable_decimal(benchmark_7d.get("gmv_delta_pct")),
        "orders": _nullable_decimal(benchmark_7d.get("orders_delta_pct")),
        "roas": _nullable_decimal(benchmark_7d.get("roas_delta_pct")),
        "ctr": _nullable_decimal(benchmark_7d.get("ctr_delta_pct")),
        "cvr": _nullable_decimal(benchmark_7d.get("cvr_delta_pct")),
        "cpc": _nullable_decimal(benchmark_7d.get("cpc_delta_pct")),
        "clicks": _nullable_decimal(benchmark_7d.get("clicks_delta_pct")),
        "impressions": _nullable_decimal(benchmark_7d.get("impressions_delta_pct")),
    }
    if any(value is not None for value in precomputed.values()):
        return precomputed
    return {
        "spend": _delta_pct(
            _nullable_decimal(scorecard.get("spend")),
            _nullable_decimal(benchmark_7d.get("spend_avg")),
        ),
        "gmv": _delta_pct(
            _nullable_decimal(scorecard.get("gmv")),
            _nullable_decimal(benchmark_7d.get("gmv_avg")),
        ),
        "orders": _delta_pct(
            _to_decimal(scorecard.get("orders")),
            _nullable_decimal(benchmark_7d.get("orders_avg")),
        ),
        "roas": _delta_pct(
            _nullable_decimal(scorecard.get("roas")),
            _nullable_decimal(benchmark_7d.get("roas_avg")),
        ),
        "ctr": _delta_pct(
            _nullable_decimal(scorecard.get("ctr")),
            _nullable_decimal(benchmark_7d.get("ctr_avg")),
        ),
        "cvr": _delta_pct(
            _nullable_decimal(scorecard.get("cvr")),
            _nullable_decimal(benchmark_7d.get("cvr_avg")),
        ),
        "cpc": _delta_pct(
            _nullable_decimal(scorecard.get("cpc")),
            _nullable_decimal(benchmark_7d.get("cpc_avg")),
        ),
        "clicks": _delta_pct(
            _to_decimal(scorecard.get("clicks")),
            _nullable_decimal(benchmark_7d.get("clicks_avg")),
        ),
        "impressions": _delta_pct(
            _to_decimal(scorecard.get("impressions")),
            _nullable_decimal(benchmark_7d.get("impressions_avg")),
        ),
    }


def _select_scorecard_benchmark(
    *,
    benchmark_7d: dict[str, Any],
    benchmark_30d: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    days_7d = int(benchmark_7d.get("days_available") or 0)
    if days_7d > 0:
        return benchmark_7d, "7d"
    days_30d = int(benchmark_30d.get("days_available") or 0)
    if days_30d > 0:
        return benchmark_30d, "30d"
    return benchmark_7d, "7d"


def _render_value_with_delta(
    value: str,
    delta: Decimal | None,
    *,
    metric_key: str,
) -> str:
    safe_value = escape(value)
    if delta is None:
        return safe_value
    pct = _quantize(delta * Decimal("100"), 2)
    cls = _delta_semantic_class(metric_key=metric_key, delta=delta)
    if delta > 0:
        pct = f"+{pct}"
    elif delta == 0:
        pct = "0.00"
    return f"{safe_value}<span class='delta-suffix {cls}'>({escape(pct)}%)</span>"


def _render_kpi_chip(metric_eval: dict[str, Any]) -> str:
    if not metric_eval:
        return ""
    status = str(metric_eval.get("status") or "").strip().lower()
    if not status or status == "n/a":
        return ""
    label_map = {
        "good": "Tốt",
        "normal": "Ổn",
        "watch": "Cảnh báo",
        "risk": "Rủi ro",
        "n/a": "Không dữ liệu",
    }
    css_map = {
        "good": "kpi-good",
        "normal": "kpi-normal",
        "watch": "kpi-watch",
        "risk": "kpi-risk",
        "n/a": "kpi-na",
    }
    label = label_map.get(status, status)
    css = css_map.get(status, "kpi-na")
    return f"<span class='kpi-chip {css}'>{escape(label)}</span>"


def _resolve_scorecard_metric_eval(
    *,
    metric_key: str,
    metric_eval: dict[str, Any],
    delta: Decimal | None,
    value: Decimal | None,
) -> dict[str, Any]:
    status = str(metric_eval.get("status") or "").strip().lower()
    if status and status != "n/a":
        return metric_eval
    fallback_status = _scorecard_delta_fallback_status(metric_key=metric_key, delta=delta)
    if not fallback_status:
        fallback_status = _scorecard_absolute_fallback_status(
            metric_key=metric_key,
            value=value if value is not None else _nullable_decimal(metric_eval.get("value")),
        )
    if not fallback_status:
        return metric_eval
    merged = dict(metric_eval)
    merged["status"] = fallback_status
    merged["source"] = "delta_fallback"
    return merged


def _scorecard_delta_fallback_status(
    *,
    metric_key: str,
    delta: Decimal | None,
) -> str:
    if delta is None:
        return ""
    key = str(metric_key or "").strip().lower()
    if key == "spend":
        abs_delta = abs(delta)
        if abs_delta <= Decimal("0.10"):
            return "normal"
        if abs_delta <= Decimal("0.25"):
            return "watch"
        return "risk"
    improvement = delta
    if key in {"cpc"}:
        improvement = -delta
    if improvement >= Decimal("0.15"):
        return "good"
    if improvement >= Decimal("-0.05"):
        return "normal"
    if improvement >= Decimal("-0.15"):
        return "watch"
    return "risk"


def _scorecard_absolute_fallback_status(
    *,
    metric_key: str,
    value: Decimal | None,
) -> str:
    if value is None:
        return ""
    key = str(metric_key or "").strip().lower()
    if key == "roas":
        band = _roas_band(value)
        return {
            "excellent": "good",
            "good": "normal",
            "watch": "watch",
            "poor": "risk",
        }.get(band, "")
    if key == "cpc":
        if value <= Decimal("0"):
            return "risk"
        if value <= Decimal("0.8"):
            return "good"
        if value <= Decimal("1.2"):
            return "normal"
        if value <= Decimal("1.8"):
            return "watch"
        return "risk"
    if key == "ctr":
        if value >= Decimal("0.025"):
            return "good"
        if value >= Decimal("0.015"):
            return "normal"
        if value >= Decimal("0.008"):
            return "watch"
        return "risk"
    if key == "cvr":
        if value >= Decimal("0.030"):
            return "good"
        if value >= Decimal("0.015"):
            return "normal"
        if value >= Decimal("0.007"):
            return "watch"
        return "risk"
    if key == "orders":
        if value >= Decimal("3"):
            return "good"
        if value >= Decimal("1"):
            return "normal"
        return "risk"
    if key == "clicks":
        if value >= Decimal("200"):
            return "good"
        if value >= Decimal("50"):
            return "normal"
        if value >= Decimal("1"):
            return "watch"
        return "risk"
    if key == "impressions":
        if value >= Decimal("5000"):
            return "good"
        if value >= Decimal("1000"):
            return "normal"
        if value >= Decimal("1"):
            return "watch"
        return "risk"
    if key in {"spend", "gmv"}:
        if value > 0:
            return "normal"
        return "risk"
    return ""


def _delta_semantic_class(*, metric_key: str, delta: Decimal) -> str:
    if delta == 0:
        return "delta-flat"
    key = str(metric_key or "").strip().lower()
    neutral_keys = {"spend"}
    lower_better_keys = {"cpc"}
    if key in neutral_keys:
        return "delta-flat"
    if key in lower_better_keys:
        return "delta-up" if delta < 0 else "delta-down"
    return "delta-up" if delta > 0 else "delta-down"


def _snapshot_fallback_heading(rank_key: str) -> str:
    rank_value = rank_key.strip().lower()
    if rank_value == "budget":
        return "Top chiến dịch theo ngân sách snapshot (không có breakdown theo chiến dịch)"
    return "Top chiến dịch theo chi tiêu snapshot (không có breakdown theo chiến dịch)"


def _render_snapshot_fallback_table(
    rows: list[dict[str, Any]], *, shop_key: str | None = None
) -> str:
    if not rows:
        return "<div>Không có dữ liệu snapshot fallback.</div>"
    lines = [
        "<table>",
        "<tr><th>#</th><th>Sản phẩm / Chiến dịch</th><th>ID chiến dịch</th><th>Trạng thái</th><th>Ngân sách</th><th>Chi tiêu</th><th>Còn lại</th><th>Cập nhật lúc</th></tr>",
    ]
    for idx, row in enumerate(rows, start=1):
        campaign_id = _display_campaign_id(row.get("campaign_id"))
        display_name = resolve_campaign_display_name(
            shop_key=shop_key,
            campaign_id=campaign_id,
            campaign_name=row.get("campaign_name"),
        )
        status = str(row.get("status") or "-")
        lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{escape(display_name)}</td>"
            f"<td>{escape(campaign_id)}</td>"
            f"<td>{escape(status)}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('budget')))}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('spend')))}</td>"
            f"<td>{_fmt_money(_nullable_decimal(row.get('remaining')))}</td>"
            f"<td>{_fmt_dt(row.get('updated_at'))}</td>"
            "</tr>"
        )
    lines.append("</table>")
    return "\n".join(lines)


def _load_daily_rows(session, shop_key: str, date: date_type) -> list[dict[str, Any]]:
    rows = (
        session.query(
            AdsCampaignDaily.campaign_id,
            AdsCampaign.campaign_name,
            AdsCampaignDaily.spend,
            AdsCampaignDaily.impressions,
            AdsCampaignDaily.clicks,
            AdsCampaignDaily.orders,
            AdsCampaignDaily.gmv,
        )
        .outerjoin(
            AdsCampaign,
            and_(
                AdsCampaign.shop_key == AdsCampaignDaily.shop_key,
                AdsCampaign.campaign_id == AdsCampaignDaily.campaign_id,
            ),
        )
        .filter(AdsCampaignDaily.shop_key == shop_key, AdsCampaignDaily.date == date)
        .all()
    )
    return [_row_to_metric_dict(row) for row in rows]


def _load_campaign_budget_rows(session, shop_key: str) -> list[dict[str, Any]]:
    rows = (
        session.query(
            AdsCampaign.campaign_id,
            AdsCampaign.campaign_name,
            AdsCampaign.status,
            AdsCampaign.daily_budget,
        )
        .filter(AdsCampaign.shop_key == shop_key)
        .all()
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "campaign_id": str(row[0] or ""),
                "campaign_name": str(row[1] or row[0] or ""),
                "status": _normalize_status_label(row[2]),
                "budget": _nullable_decimal(row[3]),
            }
        )
    return out


def _load_gms_campaign_rows(
    session,
    *,
    shop_key: str,
    date: date_type,
) -> list[dict[str, Any]]:
    rows = (
        session.query(
            Phase1AdsGmsCampaignRegistry.campaign_id,
            Phase1AdsGmsCampaignRegistry.campaign_name,
            Phase1AdsGmsCampaignRegistry.campaign_type,
            Phase1AdsGmsCampaignRegistry.daily_budget,
            Phase1AdsGmsCampaignRegistry.total_budget,
            Phase1AdsGmsCampaignRegistry.spend,
        )
        .filter(
            and_(
                Phase1AdsGmsCampaignRegistry.shop_key == shop_key,
                Phase1AdsGmsCampaignRegistry.as_of_date == date,
            )
        )
        .order_by(Phase1AdsGmsCampaignRegistry.spend.desc())
        .all()
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "campaign_id": str(row[0] or "").strip(),
                "campaign_name": str(row[1] or "").strip(),
                "campaign_type": str(row[2] or "").strip(),
                "daily_budget": _nullable_decimal(row[3]),
                "total_budget": _nullable_decimal(row[4]),
                "spend": _nullable_decimal(row[5]),
            }
        )
    return out


def _active_campaign_ids(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        campaign_id = str(row.get("campaign_id") or "").strip()
        if not campaign_id:
            continue
        spend = _nullable_decimal(row.get("spend")) or Decimal("0")
        if spend > 0:
            out.add(campaign_id)
    return out


def _load_snapshot_rows(
    session, shop_key: str, date: date_type, as_of: datetime
) -> list[dict[str, Any]]:
    tzinfo = _resolve_report_timezone(as_of)
    start_dt = datetime.combine(date, time.min, tzinfo=tzinfo)
    end_dt = datetime.combine(date, time.max, tzinfo=tzinfo)
    cutoff = as_of if as_of <= end_dt else end_dt

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
            AdsCampaignSnapshot.ts >= start_dt,
            AdsCampaignSnapshot.ts <= cutoff,
        )
        .all()
    )

    latest: dict[str, tuple] = {}
    for row in rows:
        campaign_id = row[0]
        if campaign_id not in latest or row[2] > latest[campaign_id][2]:
            latest[campaign_id] = row

    return [
        {
            "campaign_id": row[0],
            "campaign_name": row[1],
            "spend": row[3],
            "impressions": row[4],
            "clicks": row[5],
            "orders": row[6],
            "gmv": row[7],
            "roas": _safe_div(row[7], row[3]),
        }
        for row in latest.values()
    ]


def get_latest_snapshot_rows(
    session,
    shop_key: str,
    date: date_type,
    as_of: datetime | None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    tzinfo = _resolve_report_timezone(as_of)
    start_dt = datetime.combine(date, time.min, tzinfo=tzinfo)
    end_dt = datetime.combine(date, time.max, tzinfo=tzinfo)
    cutoff = end_dt
    if as_of is not None and as_of <= end_dt:
        cutoff = as_of

    rows = (
        session.query(
            AdsCampaignSnapshot.campaign_id,
            AdsCampaign.campaign_name,
            AdsCampaign.status,
            AdsCampaign.daily_budget,
            AdsCampaignSnapshot.ts,
            AdsCampaignSnapshot.spend_today,
        )
        .outerjoin(
            AdsCampaign,
            and_(
                AdsCampaign.shop_key == AdsCampaignSnapshot.shop_key,
                AdsCampaign.campaign_id == AdsCampaignSnapshot.campaign_id,
            ),
        )
        .filter(
            AdsCampaignSnapshot.shop_key == shop_key,
            AdsCampaignSnapshot.ts >= start_dt,
            AdsCampaignSnapshot.ts <= cutoff,
        )
        .order_by(AdsCampaignSnapshot.ts.desc())
        .all()
    )

    latest_per_campaign: dict[str, tuple] = {}
    for row in rows:
        campaign_id = str(row[0] or "").strip()
        if not campaign_id or campaign_id in latest_per_campaign:
            continue
        latest_per_campaign[campaign_id] = row
        if len(latest_per_campaign) >= max(limit, 1):
            break

    out: list[dict[str, Any]] = []
    for row in latest_per_campaign.values():
        campaign_id = str(row[0] or "").strip()
        campaign_name = str(row[1] or campaign_id)
        status = _normalize_status_label(row[2])
        budget = _nullable_decimal(row[3])
        spend = _nullable_decimal(row[5])
        remaining = budget - spend if budget is not None and spend is not None else None
        out.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "status": status,
                "budget": budget,
                "spend": spend,
                "remaining": remaining,
                "updated_at": row[4],
            }
        )
    return out


def _row_to_metric_dict(row) -> dict[str, Any]:
    spend = row[2]
    gmv = row[6]
    campaign_id = row[0]
    campaign_name = row[1] if row[1] else campaign_id
    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "spend": spend,
        "impressions": row[3],
        "clicks": row[4],
        "orders": row[5],
        "gmv": gmv,
        "roas": _safe_div(gmv, spend),
    }


def _select_daily_rows_for_aggregation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows

    campaign_rows = [row for row in rows if not _is_shop_total_row(row)]
    if campaign_rows:
        return campaign_rows

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        campaign_id = str(row.get("campaign_id") or "").strip().upper()
        if campaign_id in seen:
            continue
        seen.add(campaign_id)
        deduped.append(row)
    return deduped


def _select_rows_for_totals(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Pick rows used for shop-level totals.

    Prefer SHOP_TOTAL when available (all-cpc total), otherwise fall back to
    campaign-sum behavior.
    """
    if not rows:
        return rows

    shop_total_rows = [row for row in rows if _is_shop_total_row(row)]
    if not shop_total_rows:
        return _select_daily_rows_for_aggregation(rows)

    # Keep one row per campaign_id (normally just SHOP_TOTAL).
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in shop_total_rows:
        campaign_id = str(row.get("campaign_id") or "").strip().upper()
        if campaign_id in seen:
            continue
        seen.add(campaign_id)
        deduped.append(row)
    return deduped


def _campaign_spend_coverage(
    campaign_rows: list[dict[str, Any]],
    totals: dict[str, Any],
) -> Decimal | None:
    if not campaign_rows:
        return None
    total_spend = _nullable_decimal(totals.get("spend"))
    if total_spend is None or total_spend <= 0:
        return None
    campaign_spend = Decimal("0")
    for row in campaign_rows:
        campaign_spend += _to_decimal(row.get("spend"))
    return _safe_div(campaign_spend, total_spend)


def _is_shop_total_row(row: dict[str, Any]) -> bool:
    return str(row.get("campaign_id") or "").strip().upper() == "SHOP_TOTAL"


def _display_campaign_id(campaign_id: object) -> str:
    value = str(campaign_id or "").strip()
    if value.upper() == "SHOP_TOTAL":
        return "SHOP_TOTAL"
    return value


def _aggregate_totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return aggregate_metric_rows(rows)


def _compute_kpis(totals: dict[str, Any]) -> dict[str, Any]:
    return compute_kpis_from_totals(totals)


def _top_by_spend(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    campaign_rows = [
        row
        for row in rows
        if not _is_shop_total_row(row) and _to_decimal(row.get("spend")) > 0
    ]
    return sorted(campaign_rows, key=lambda r: _to_decimal(r["spend"]), reverse=True)[:5]


def _campaign_performance_count(rows: list[dict[str, Any]]) -> int:
    return len(
        [
            row
            for row in rows
            if not _is_shop_total_row(row) and _to_decimal(row.get("spend")) > 0
        ]
    )


def _campaign_performance_rows(
    rows: list[dict[str, Any]],
    *,
    max_rows: int = 50,
) -> list[dict[str, Any]]:
    campaign_rows = [
        row
        for row in rows
        if not _is_shop_total_row(row) and _to_decimal(row.get("spend")) > 0
    ]
    sorted_rows = sorted(campaign_rows, key=lambda r: _to_decimal(r["spend"]), reverse=True)
    out: list[dict[str, Any]] = []
    for row in sorted_rows[: max(max_rows, 1)]:
        impressions = int(row.get("impressions") or 0)
        clicks = int(row.get("clicks") or 0)
        orders = int(row.get("orders") or 0)
        ctr = (
            _safe_div(Decimal(clicks), Decimal(impressions))
            if impressions > 0
            else None
        )
        cvr = _safe_div(Decimal(orders), Decimal(clicks)) if clicks > 0 else None
        roas = _nullable_decimal(row.get("roas"))
        roas_band = _roas_band(roas)
        funnel_band = _funnel_band(ctr=ctr, cvr=cvr)
        out.append(
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "spend": _to_decimal(row.get("spend")),
                "gmv": _to_decimal(row.get("gmv")),
                "orders": orders,
                "roas": roas,
                "ctr": ctr,
                "cvr": cvr,
                "roas_band": roas_band,
                "funnel_band": funnel_band,
                "action_hint": _campaign_action_hint(
                    roas_band=roas_band,
                    funnel_band=funnel_band,
                    orders=orders,
                ),
            }
        )
    return out


def _append_unattributed_campaign_row(
    *,
    campaign_rows: list[dict[str, Any]],
    totals: dict[str, Any],
) -> list[dict[str, Any]]:
    total_spend = _nullable_decimal(totals.get("spend"))
    total_gmv = _nullable_decimal(totals.get("gmv"))
    total_orders = int(totals.get("orders") or 0)
    if total_spend is None or total_spend <= 0:
        return campaign_rows

    campaign_spend = sum(
        (_nullable_decimal(row.get("spend")) or Decimal("0")) for row in campaign_rows
    )
    campaign_gmv = sum(
        (_nullable_decimal(row.get("gmv")) or Decimal("0")) for row in campaign_rows
    )
    campaign_orders = sum(int(row.get("orders") or 0) for row in campaign_rows)

    spend_gap = total_spend - campaign_spend
    if spend_gap <= Decimal("0"):
        return campaign_rows
    gmv_gap = (total_gmv or Decimal("0")) - campaign_gmv
    orders_gap = total_orders - campaign_orders
    gap_roas = _safe_div(gmv_gap, spend_gap) if spend_gap > 0 else None
    extra_row = {
        "campaign_id": "NON_PRODUCT_POOL",
        "campaign_name": "Tổng hợp Group/Shop/Auto (phần còn lại ngoài campaign sản phẩm)",
        "spend": spend_gap,
        "gmv": gmv_gap,
        "orders": orders_gap if orders_gap > 0 else 0,
        "roas": gap_roas,
        "ctr": None,
        "cvr": None,
    }
    return campaign_rows + [extra_row]


def _worst_by_roas(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered = [
        row
        for row in rows
        if not _is_shop_total_row(row) and _to_decimal(row["spend"]) >= Decimal("1")
    ]
    filtered.sort(key=lambda r: _to_decimal(r["roas"]) if r["roas"] else Decimal("0"))
    return filtered[:5]


def _roas_band(roas: Decimal | None) -> str:
    if roas is None:
        return "n/a"
    if roas >= Decimal("5"):
        return "excellent"
    if roas >= Decimal("3"):
        return "good"
    if roas >= Decimal("1.5"):
        return "watch"
    return "poor"


def _funnel_band(*, ctr: Decimal | None, cvr: Decimal | None) -> str:
    if ctr is None and cvr is None:
        return "n/a"
    ctr_ok = ctr is not None and ctr >= Decimal("0.01")
    cvr_ok = cvr is not None and cvr >= Decimal("0.02")
    if ctr_ok and cvr_ok:
        return "healthy"
    if ctr_ok and not cvr_ok:
        return "click-heavy"
    if not ctr_ok and cvr_ok:
        return "targeted"
    return "weak"


def _campaign_action_hint(*, roas_band: str, funnel_band: str, orders: int) -> str:
    if roas_band == "excellent" and funnel_band == "healthy":
        return "scale budget"
    if roas_band in {"good", "watch"} and orders <= 0:
        return "check conversion path"
    if funnel_band == "click-heavy":
        return "tighten targeting"
    if funnel_band == "weak":
        return "refresh creative"
    if roas_band == "poor":
        return "reduce bid / pause"
    return "monitor"


def _resolve_report_timezone(as_of: datetime | None):
    if as_of is not None and as_of.tzinfo is not None:
        return as_of.tzinfo
    return resolve_timezone(get_settings().timezone)


def _empty_snapshot_fallback() -> dict[str, Any]:
    return {
        "used": 0,
        "rows": [],
        "rank_key": None,
        "latest_snapshot_at": None,
    }


def _build_snapshot_fallback(
    rows: list[dict[str, Any]],
    top_n: int,
) -> dict[str, Any]:
    campaign_rows = [row for row in rows if not _is_shop_total_row(row)]
    if not campaign_rows:
        return _empty_snapshot_fallback()

    has_spend = any(_nullable_decimal(row.get("spend")) is not None for row in campaign_rows)
    has_budget = any(_nullable_decimal(row.get("budget")) is not None for row in campaign_rows)
    rank_key = "spend"
    if not has_spend and has_budget:
        rank_key = "budget"

    def _sort_key(row: dict[str, Any]) -> tuple[Decimal, Decimal, str]:
        spend = _nullable_decimal(row.get("spend")) or Decimal("0")
        budget = _nullable_decimal(row.get("budget")) or Decimal("0")
        key_value = budget if rank_key == "budget" else spend
        return (key_value, budget, str(row.get("campaign_id") or ""))

    ranked_rows = sorted(campaign_rows, key=_sort_key, reverse=True)[: max(top_n, 1)]
    latest_snapshot_at = None
    for row in ranked_rows:
        updated_at = row.get("updated_at")
        if not isinstance(updated_at, datetime):
            continue
        if latest_snapshot_at is None or updated_at > latest_snapshot_at:
            latest_snapshot_at = updated_at

    return {
        "used": 1,
        "rows": ranked_rows,
        "rank_key": rank_key,
        "latest_snapshot_at": latest_snapshot_at,
    }


def _estimate_budget_from_campaign_sum(
    rows: list[dict[str, Any]],
    *,
    active_campaign_ids: set[str],
) -> tuple[Decimal | None, int]:
    total = Decimal("0")
    campaigns_budgeted = 0
    for row in rows:
        if _is_shop_total_row(row):
            continue
        budget = _nullable_decimal(row.get("budget"))
        if budget is None:
            continue
        status = str(row.get("status") or "").strip().lower()
        campaign_id = str(row.get("campaign_id") or "").strip()
        include = _should_include_budget_row(
            status=status,
            campaign_id=campaign_id,
            active_campaign_ids=active_campaign_ids,
        )
        if not include:
            continue
        total += budget
        campaigns_budgeted += 1
    if campaigns_budgeted <= 0:
        return None, 0
    return total, campaigns_budgeted


def _effective_budget_estimate(
    *,
    campaign_budget_rows: list[dict[str, Any]],
    active_campaign_ids: set[str],
    snapshot_rows: list[dict[str, Any]],
    budget_override: Decimal | None,
) -> tuple[Decimal | None, int, str]:
    campaign_budget, campaigns_budgeted = _estimate_budget_from_campaign_sum(
        campaign_budget_rows,
        active_campaign_ids=active_campaign_ids,
    )
    if campaign_budget is not None and campaigns_budgeted > 0:
        return campaign_budget, campaigns_budgeted, "campaign_sum"
    snapshot_budget, campaigns_budgeted = _estimate_budget_from_campaign_sum(
        snapshot_rows,
        active_campaign_ids=active_campaign_ids,
    )
    if snapshot_budget is not None and campaigns_budgeted > 0:
        return snapshot_budget, campaigns_budgeted, "snapshot"
    return None, 0, "none"


def _should_include_budget_row(
    *,
    status: str,
    campaign_id: str,
    active_campaign_ids: set[str],
) -> bool:
    # If this campaign actually spent on the report date, include its budget even when
    # status is stale/off (status can be changed after spend is accumulated).
    if campaign_id and campaign_id in active_campaign_ids:
        return True
    if status == "on":
        return True
    if status == "off":
        return False
    return False


def _resolve_shop_budget_override(shop_key: str) -> Decimal | None:
    try:
        shops = load_shops()
    except Exception:  # noqa: BLE001
        return None
    for shop in shops:
        if shop.shop_key != shop_key:
            continue
        raw_value = getattr(shop, "daily_budget_est", None)
        return _nullable_decimal(raw_value)
    return None


def compute_scorecard(
    *,
    totals: dict[str, Any],
    kpis: dict[str, Any],
    budget_est: Decimal | None,
) -> dict[str, Any]:
    spend = _nullable_decimal(totals.get("spend")) or Decimal("0")
    remaining = None
    util_pct = None
    if budget_est is not None:
        raw_remaining = budget_est - spend
        remaining = raw_remaining if raw_remaining > 0 else Decimal("0")
        util_pct = _safe_div(spend, budget_est)
    return {
        "budget_est": budget_est,
        "spend": spend,
        "remaining": remaining,
        "util_pct": util_pct,
        "impressions": int(totals.get("impressions") or 0),
        "clicks": int(totals.get("clicks") or 0),
        "orders": int(totals.get("orders") or 0),
        "gmv": _nullable_decimal(totals.get("gmv")) or Decimal("0"),
        "roas": kpis.get("roas"),
        "ctr": kpis.get("ctr"),
        "cpc": kpis.get("cpc"),
        "cvr": kpis.get("cvr"),
    }


def _kpi_threshold_reference_date(report_date: date_type) -> date_type:
    # Freeze KPI baselines by month so thresholds update on a predictable cadence.
    return report_date.replace(day=1)


def _build_kpi_thresholds(
    *,
    session,
    shop_key: str,
    reference_date: date_type,
    lookback_days: int = 180,
    min_days: int = 45,
) -> dict[str, Any]:
    start_date = reference_date - timedelta(days=max(lookback_days, 1))
    rows = (
        session.query(
            AdsCampaignDaily.spend,
            AdsCampaignDaily.impressions,
            AdsCampaignDaily.clicks,
            AdsCampaignDaily.orders,
            AdsCampaignDaily.gmv,
        )
        .filter(
            and_(
                AdsCampaignDaily.shop_key == shop_key,
                AdsCampaignDaily.campaign_id == "SHOP_TOTAL",
                AdsCampaignDaily.date >= start_date,
                AdsCampaignDaily.date < reference_date,
            )
        )
        .order_by(AdsCampaignDaily.date.asc())
        .all()
    )
    roas_values: list[Decimal] = []
    ctr_values: list[Decimal] = []
    cvr_values: list[Decimal] = []
    cpc_values: list[Decimal] = []
    gmv_values: list[Decimal] = []
    orders_values: list[Decimal] = []
    clicks_values: list[Decimal] = []
    impressions_values: list[Decimal] = []
    active_days = 0
    for row in rows:
        spend = _to_decimal(row[0])
        if spend <= 0:
            continue
        active_days += 1
        gmv = _to_decimal(row[4])
        gmv_values.append(gmv)
        roas_values.append(gmv / spend)
        impressions = int(row[1] or 0)
        impressions_values.append(Decimal(impressions))
        clicks = int(row[2] or 0)
        clicks_values.append(Decimal(clicks))
        orders = int(row[3] or 0)
        orders_values.append(Decimal(orders))
        if impressions > 0:
            ctr_values.append(Decimal(clicks) / Decimal(impressions))
        if clicks > 0:
            cvr_values.append(Decimal(orders) / Decimal(clicks))
            cpc_values.append(spend / Decimal(clicks))

    return {
        "cadence": "monthly",
        "reference_date": reference_date.isoformat(),
        "lookback_days": max(lookback_days, 1),
        "active_days": active_days,
        "min_days": max(min_days, 1),
        "roas": _build_kpi_metric_threshold(
            values=roas_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "ctr": _build_kpi_metric_threshold(
            values=ctr_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "cvr": _build_kpi_metric_threshold(
            values=cvr_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "cpc": _build_kpi_metric_threshold(
            values=cpc_values,
            direction="low",
            min_days=max(min_days, 1),
            good_percentile=30,
            normal_percentile=50,
            watch_percentile=65,
        ),
        "gmv": _build_kpi_metric_threshold(
            values=gmv_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "orders": _build_kpi_metric_threshold(
            values=orders_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "clicks": _build_kpi_metric_threshold(
            values=clicks_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "impressions": _build_kpi_metric_threshold(
            values=impressions_values,
            direction="high",
            min_days=max(min_days, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
    }


def _build_kpi_metric_threshold(
    *,
    values: list[Decimal],
    direction: str,
    min_days: int,
    good_percentile: int,
    normal_percentile: int,
    watch_percentile: int,
) -> dict[str, Any]:
    days_available = len(values)
    if days_available < max(min_days, 1):
        return {
            "enabled": 0,
            "direction": direction,
            "days_available": days_available,
            "good_cutoff": None,
            "normal_cutoff": None,
            "watch_cutoff": None,
            "good_percentile": good_percentile,
            "normal_percentile": normal_percentile,
            "watch_percentile": watch_percentile,
        }
    good_cutoff = _percentile_decimal(values, good_percentile)
    normal_cutoff = _percentile_decimal(values, normal_percentile)
    watch_cutoff = _percentile_decimal(values, watch_percentile)
    return {
        "enabled": 1,
        "direction": direction,
        "days_available": days_available,
        "good_cutoff": good_cutoff,
        "normal_cutoff": normal_cutoff,
        "watch_cutoff": watch_cutoff,
        "good_percentile": good_percentile,
        "normal_percentile": normal_percentile,
        "watch_percentile": watch_percentile,
    }


def _percentile_decimal(values: list[Decimal], percentile: int) -> Decimal | None:
    if not values:
        return None
    materialized = sorted(values)
    if len(materialized) == 1:
        return materialized[0]
    p = Decimal(max(0, min(percentile, 100))) / Decimal("100")
    raw_index = (Decimal(len(materialized) - 1) * p)
    lower_index = int(raw_index.to_integral_value(rounding=ROUND_HALF_UP))
    lower_index = min(max(lower_index, 0), len(materialized) - 1)
    # Use a deterministic nearest-rank style for stable thresholds.
    return materialized[lower_index]


def _evaluate_scorecard_kpis(
    *,
    scorecard: dict[str, Any],
    kpi_thresholds: dict[str, Any],
    intraday: bool = False,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    intraday_skip_metrics = {"gmv", "orders", "clicks", "impressions"} if intraday else set()
    for key in ("roas", "ctr", "cvr", "cpc", "gmv", "orders", "clicks", "impressions"):
        value = _nullable_decimal(scorecard.get(key))
        metric_cfg = (
            kpi_thresholds.get(key)
            if isinstance(kpi_thresholds.get(key), dict)
            else {}
        )
        if key in intraday_skip_metrics:
            status = "n/a"
        else:
            status = _evaluate_metric_status(value=value, metric_cfg=metric_cfg)
        out[key] = {
            "status": status,
            "value": value,
            "direction": str(metric_cfg.get("direction") or ""),
            "days_available": int(metric_cfg.get("days_available") or 0),
            "good_cutoff": _nullable_decimal(metric_cfg.get("good_cutoff")),
            "normal_cutoff": _nullable_decimal(metric_cfg.get("normal_cutoff")),
            "watch_cutoff": _nullable_decimal(metric_cfg.get("watch_cutoff")),
        }
    return out


def _evaluate_metric_status(
    *,
    value: Decimal | None,
    metric_cfg: dict[str, Any],
) -> str:
    if value is None:
        return "n/a"
    if int(metric_cfg.get("enabled") or 0) != 1:
        return "n/a"
    direction = str(metric_cfg.get("direction") or "").strip().lower()
    good_cutoff = _nullable_decimal(metric_cfg.get("good_cutoff"))
    normal_cutoff = _nullable_decimal(metric_cfg.get("normal_cutoff"))
    watch_cutoff = _nullable_decimal(metric_cfg.get("watch_cutoff"))
    if good_cutoff is None or normal_cutoff is None or watch_cutoff is None:
        return "n/a"
    if direction == "low":
        if value <= good_cutoff:
            return "good"
        if value <= normal_cutoff:
            return "normal"
        if value <= watch_cutoff:
            return "watch"
        return "risk"
    if value >= good_cutoff:
        return "good"
    if value >= normal_cutoff:
        return "normal"
    if value >= watch_cutoff:
        return "watch"
    return "risk"


def _normalize_status_label(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return "-"
    lowered = text.lower()
    if lowered in {
        "active",
        "enabled",
        "enable",
        "on",
        "running",
        "ongoing",
        "serving",
        "in_progress",
        "open",
        "1",
        "true",
    }:
        return "on"
    if lowered in {
        "inactive",
        "disabled",
        "disable",
        "off",
        "paused",
        "ended",
        "end",
        "closed",
        "close",
        "stopped",
        "stop",
        "suspended",
        "suspend",
        "terminated",
        "0",
        "false",
    }:
        return "off"
    return lowered


def _load_campaign_breakdown_status_meta(
    session,
    shop_key: str,
    date: date_type,
) -> dict[str, Any] | None:
    date_iso = date.isoformat()
    rows = (
        session.query(EventLog.meta_json)
        .filter(EventLog.message == "ads_campaign_breakdown_status")
        .order_by(EventLog.id.desc())
        .limit(200)
        .all()
    )
    for row in rows:
        try:
            meta_json = row[0]
        except Exception:  # noqa: BLE001
            meta_json = getattr(row, "meta_json", None)
        if not meta_json:
            continue
        try:
            payload = json.loads(meta_json)
        except Exception:  # noqa: BLE001
            continue
        if str(payload.get("shop_key") or "") != shop_key:
            continue
        if str(payload.get("date") or "") != date_iso:
            continue
        return payload
    return None


def _campaign_breakdown_status_value(
    status_meta: dict[str, Any] | None,
    *,
    has_campaign_rows: bool,
) -> str:
    if status_meta:
        status = str(status_meta.get("status") or "").strip().lower()
        blocked_403 = int(status_meta.get("blocked_403") or 0) == 1
        if status == "cooldown_skip":
            return "cooldown_skip"
        if blocked_403:
            return "blocked_403"
        if status == "applied":
            return "supported"
        if status:
            return status
    return "supported" if has_campaign_rows else "unknown"


def _campaign_breakdown_fallback_note(
    *,
    campaign_breakdown_status: str,
    status_meta: dict[str, Any] | None,
) -> str | None:
    if campaign_breakdown_status == "blocked_403":
        return "Breakdown theo chiến dịch bị chặn bởi API (403 Forbidden). Chỉ hiển thị tổng shop."
    if campaign_breakdown_status == "cooldown_skip":
        cooldown_until_utc = (
            str(status_meta.get("cooldown_until_utc") or "").strip()
            if status_meta
            else ""
        )
        if cooldown_until_utc:
            return (
                "Breakdown theo chien dich tam thoi bi bo qua do cooldown sau loi 403 "
                f"(den {cooldown_until_utc})."
            )
        return "Breakdown theo chien dich tam thoi bi bo qua do cooldown sau loi 403."
    return None


def _build_historical_benchmark(
    *,
    session,
    shop_key: str,
    date: date_type,
    totals: dict[str, Any],
    kpis: dict[str, Any],
    as_of: datetime | None,
) -> dict[str, Any]:
    intraday_mode = as_of is not None
    basis = "intraday_snapshot" if intraday_mode else "daily_final"
    cutoff_local = None
    cutoff_time = None
    if intraday_mode and as_of is not None:
        tzinfo = _resolve_report_timezone(as_of)
        as_of_local = as_of.astimezone(tzinfo) if as_of.tzinfo is not None else as_of.replace(tzinfo=tzinfo)
        cutoff_time = as_of_local.timetz().replace(microsecond=0)
        cutoff_local = cutoff_time.strftime("%H:%M:%S")

    windows_out: list[dict[str, Any]] = []
    current_spend = _nullable_decimal(totals.get("spend"))
    current_gmv = _nullable_decimal(totals.get("gmv"))
    current_orders = Decimal(int(totals.get("orders") or 0))
    current_roas = _nullable_decimal(kpis.get("roas"))
    current_ctr = _nullable_decimal(kpis.get("ctr"))
    current_cpc = _nullable_decimal(kpis.get("cpc"))
    current_cvr = _nullable_decimal(kpis.get("cvr"))
    current_clicks = Decimal(int(totals.get("clicks") or 0))
    current_impressions = Decimal(int(totals.get("impressions") or 0))

    for window_days, label in ((7, "7d"), (30, "30d")):
        daily_totals: list[dict[str, Any]] = []
        for idx in range(1, window_days + 1):
            prev_date = date - timedelta(days=idx)
            prev_rows: list[dict[str, Any]]
            if intraday_mode and cutoff_time is not None:
                prev_cutoff = datetime.combine(
                    prev_date,
                    cutoff_time,
                    tzinfo=cutoff_time.tzinfo,
                )
                prev_rows = _select_rows_for_totals(
                    _load_snapshot_rows(session, shop_key, prev_date, prev_cutoff)
                )
                if not prev_rows:
                    # Fallback to daily totals when same-time snapshots are unavailable.
                    prev_rows = _select_rows_for_totals(
                        _load_daily_rows(session, shop_key, prev_date)
                    )
            else:
                prev_rows = _select_rows_for_totals(_load_daily_rows(session, shop_key, prev_date))
            if not prev_rows:
                continue
            prev_totals = _aggregate_totals(prev_rows)
            prev_kpis = _compute_kpis(prev_totals)
            daily_totals.append(
                {
                    "spend": _nullable_decimal(prev_totals.get("spend")),
                    "gmv": _nullable_decimal(prev_totals.get("gmv")),
                    "orders": Decimal(int(prev_totals.get("orders") or 0)),
                    "roas": _nullable_decimal(prev_kpis.get("roas")),
                    "ctr": _nullable_decimal(prev_kpis.get("ctr")),
                    "cpc": _nullable_decimal(prev_kpis.get("cpc")),
                    "cvr": _nullable_decimal(prev_kpis.get("cvr")),
                    "clicks": Decimal(int(prev_totals.get("clicks") or 0)),
                    "impressions": Decimal(int(prev_totals.get("impressions") or 0)),
                }
            )

        if not daily_totals:
            windows_out.append(
                {
                    "label": label,
                    "days_available": 0,
                    "spend_avg": None,
                    "spend_delta_pct": None,
                    "gmv_avg": None,
                    "gmv_delta_pct": None,
                    "orders_avg": None,
                    "orders_delta_pct": None,
                    "roas_avg": None,
                    "roas_delta_pct": None,
                    "ctr_avg": None,
                    "ctr_delta_pct": None,
                    "cpc_avg": None,
                    "cpc_delta_pct": None,
                    "cvr_avg": None,
                    "cvr_delta_pct": None,
                    "clicks_avg": None,
                    "clicks_delta_pct": None,
                    "impressions_avg": None,
                    "impressions_delta_pct": None,
                }
            )
            continue

        days = len(daily_totals)
        spend_avg = _avg_decimal([row.get("spend") for row in daily_totals])
        gmv_avg = _avg_decimal([row.get("gmv") for row in daily_totals])
        orders_avg = _avg_decimal([row.get("orders") for row in daily_totals])
        roas_avg = _avg_decimal([row.get("roas") for row in daily_totals])
        ctr_avg = _avg_decimal([row.get("ctr") for row in daily_totals])
        cpc_avg = _avg_decimal([row.get("cpc") for row in daily_totals])
        cvr_avg = _avg_decimal([row.get("cvr") for row in daily_totals])
        clicks_avg = _avg_decimal([row.get("clicks") for row in daily_totals])
        impressions_avg = _avg_decimal([row.get("impressions") for row in daily_totals])
        windows_out.append(
            {
                "label": label,
                "days_available": days,
                "spend_avg": spend_avg,
                "spend_delta_pct": _delta_pct(
                    current_spend,
                    spend_avg,
                    min_base=Decimal("10000"),
                ),
                "gmv_avg": gmv_avg,
                "gmv_delta_pct": _delta_pct(
                    current_gmv,
                    gmv_avg,
                    min_base=Decimal("10000"),
                ),
                "orders_avg": orders_avg,
                "orders_delta_pct": _delta_pct(current_orders, orders_avg),
                "roas_avg": roas_avg,
                "roas_delta_pct": _delta_pct(
                    current_roas,
                    roas_avg,
                    min_base=Decimal("0.2"),
                ),
                "ctr_avg": ctr_avg,
                "ctr_delta_pct": _delta_pct(
                    current_ctr,
                    ctr_avg,
                    min_base=Decimal("0.001"),
                ),
                "cpc_avg": cpc_avg,
                "cpc_delta_pct": _delta_pct(
                    current_cpc,
                    cpc_avg,
                    min_base=Decimal("1000"),
                ),
                "cvr_avg": cvr_avg,
                "cvr_delta_pct": _delta_pct(
                    current_cvr,
                    cvr_avg,
                    min_base=Decimal("0.001"),
                ),
                "clicks_avg": clicks_avg,
                "clicks_delta_pct": _delta_pct(
                    current_clicks,
                    clicks_avg,
                    min_base=Decimal("10"),
                ),
                "impressions_avg": impressions_avg,
                "impressions_delta_pct": _delta_pct(
                    current_impressions,
                    impressions_avg,
                    min_base=Decimal("100"),
                ),
            }
        )

    return {
        "windows": windows_out,
        "basis": basis,
        "cutoff_local": cutoff_local,
    }


def _benchmark_window(benchmark: dict[str, Any], label: str) -> dict[str, Any] | None:
    windows = benchmark.get("windows")
    if not isinstance(windows, list):
        return None
    for window in windows:
        if not isinstance(window, dict):
            continue
        if str(window.get("label") or "") == label:
            return window
    return None


def _delta_vs_previous_day(
    session, shop_key: str, date: date_type, totals: dict[str, Any]
) -> dict[str, Any] | None:
    prev_date = date - timedelta(days=1)
    prev_rows = _select_rows_for_totals(_load_daily_rows(session, shop_key, prev_date))
    if not prev_rows:
        return None
    prev_totals = _aggregate_totals(prev_rows)
    prev_kpis = _compute_kpis(prev_totals)
    current_roas = (
        _to_decimal(totals.get("gmv")) / _to_decimal(totals.get("spend"))
        if _to_decimal(totals.get("spend")) > 0
        else None
    )

    return {
        "prev_date": prev_date.isoformat(),
        "spend_prev": prev_totals["spend"],
        "spend_curr": totals["spend"],
        "orders_prev": int(prev_totals["orders"]),
        "orders_curr": int(totals["orders"]),
        "gmv_prev": prev_totals["gmv"],
        "gmv_curr": totals["gmv"],
        "roas_prev": prev_kpis["roas"],
        "roas_curr": current_roas,
        "spend_pct": _delta_pct(
            totals["spend"],
            prev_totals["spend"],
            min_base=Decimal("10000"),
        ),
        "orders_pct": _delta_pct(Decimal(totals["orders"]), Decimal(prev_totals["orders"])),
        "gmv_pct": _delta_pct(
            totals["gmv"],
            prev_totals["gmv"],
            min_base=Decimal("10000"),
        ),
        "roas_pct": _delta_pct(
            current_roas,
            prev_kpis["roas"],
            min_base=Decimal("0.2"),
        ),
    }


def _delta_pct(
    current: Decimal | None,
    previous: Decimal | None,
    *,
    min_base: Decimal | None = None,
) -> Decimal | None:
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    if min_base is not None and abs(previous) < min_base:
        return None
    return (current - previous) / previous


def _avg_decimal(values: list[Decimal | None]) -> Decimal | None:
    materialized = [value for value in values if value is not None]
    if not materialized:
        return None
    return sum(materialized, Decimal("0")) / Decimal(len(materialized))


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator is None:
        return None
    return metrics_safe_div(numerator, denominator)


def _to_decimal(value: Decimal | int | float | str | None) -> Decimal:
    return metrics_to_decimal(value)


def _nullable_decimal(value: Decimal | int | float | str | None) -> Decimal | None:
    return metrics_nullable_decimal(value)


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "-"
    rounded = _to_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    return f"{sign}{abs(rounded):,}₫"


def _fmt_money_h(value: Decimal | None) -> str:
    if value is None:
        return "-"
    rounded = _to_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    return f"{sign}VND {abs(rounded):,}"


def _fmt_int(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{int(value)}"


def _fmt_int_h(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{int(value):,}"


def _fmt_ratio(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return _quantize(value, 2)


def _fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return _quantize(value * Decimal("100"), 2) + "%"


def _fmt_pct_compact(value: Decimal | None) -> str:
    if value is None:
        return "-"
    pct = value * Decimal("100")
    text = _quantize(pct, 2)
    if text.endswith("00"):
        text = text[:-3]
    elif text.endswith("0"):
        text = text[:-1]
    return text + "%"


def _quantize(value: Decimal, places: int) -> str:
    quant = Decimal("1").scaleb(-places)
    return f"{value.quantize(quant, rounding=ROUND_HALF_UP)}"


def _fmt_dt(value: datetime | str | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return "-"
        parsed = _parse_datetime(raw)
        if parsed is None:
            return raw
        value = parsed
    tz = resolve_timezone(get_settings().timezone)
    if value.tzinfo is None:
        localized = value.replace(tzinfo=tz)
    else:
        localized = value.astimezone(tz)
    return localized.strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime(raw: str) -> datetime | None:
    text = raw.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None

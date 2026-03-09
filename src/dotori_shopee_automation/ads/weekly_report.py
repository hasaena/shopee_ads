from __future__ import annotations

from datetime import date as date_type
from datetime import datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from html import escape
import json
from pathlib import Path
from typing import Any

from sqlalchemy import func

from .incidents import AdsIncident
from .models import AdsCampaign, AdsCampaignDaily
from ..config import get_settings, load_shops, resolve_timezone


def get_last_week_range(now: datetime, tz) -> tuple[date_type, date_type]:
    now_local = now
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=tz)
    else:
        now_local = now_local.astimezone(tz)
    current_monday = now_local.date() - timedelta(days=now_local.weekday())
    start_date = current_monday - timedelta(days=7)
    end_date = start_date + timedelta(days=6)
    return start_date, end_date


def week_id(start_date: date_type) -> str:
    iso = start_date.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def compute_weekly_metrics(
    session, shop_key: str, start_date: date_type, end_date: date_type
) -> dict[str, Any]:
    rows = (
        session.query(
            AdsCampaignDaily.campaign_id,
            func.coalesce(func.sum(AdsCampaignDaily.spend), 0),
            func.coalesce(func.sum(AdsCampaignDaily.impressions), 0),
            func.coalesce(func.sum(AdsCampaignDaily.clicks), 0),
            func.coalesce(func.sum(AdsCampaignDaily.orders), 0),
            func.coalesce(func.sum(AdsCampaignDaily.gmv), 0),
        )
        .filter(
            AdsCampaignDaily.shop_key == shop_key,
            AdsCampaignDaily.date >= start_date,
            AdsCampaignDaily.date <= end_date,
        )
        .group_by(AdsCampaignDaily.campaign_id)
        .all()
    )
    totals = {
        "spend": Decimal("0"),
        "impressions": 0,
        "clicks": 0,
        "orders": 0,
        "gmv": Decimal("0"),
    }
    if rows:
        shop_total_row = None
        for row in rows:
            if _is_shop_total_campaign_id(row[0]):
                shop_total_row = row
                break
        target_rows = [shop_total_row] if shop_total_row is not None else list(rows)
        for row in target_rows:
            totals["spend"] += _to_decimal(row[1])
            totals["impressions"] += int(row[2] or 0)
            totals["clicks"] += int(row[3] or 0)
            totals["orders"] += int(row[4] or 0)
            totals["gmv"] += _to_decimal(row[5])
    kpis = _compute_kpis(totals)
    return {"totals": totals, "kpis": kpis}


def compute_wow_delta(
    session, shop_key: str, start_date: date_type, end_date: date_type
) -> dict[str, Any] | None:
    prev_start = start_date - timedelta(days=7)
    prev_end = end_date - timedelta(days=7)
    prev_count = (
        session.query(func.count(AdsCampaignDaily.id))
        .filter(
            AdsCampaignDaily.shop_key == shop_key,
            AdsCampaignDaily.date >= prev_start,
            AdsCampaignDaily.date <= prev_end,
        )
        .scalar()
    )
    if not prev_count:
        return None

    current = compute_weekly_metrics(session, shop_key, start_date, end_date)
    previous = compute_weekly_metrics(session, shop_key, prev_start, prev_end)

    current_totals = current["totals"]
    previous_totals = previous["totals"]
    current_kpis = current["kpis"]
    previous_kpis = previous["kpis"]

    return {
        "prev_start": prev_start.isoformat(),
        "prev_end": prev_end.isoformat(),
        "spend_pct": _delta_pct(current_totals["spend"], previous_totals["spend"]),
        "orders_pct": _delta_pct(
            Decimal(current_totals["orders"]), Decimal(previous_totals["orders"])
        ),
        "gmv_pct": _delta_pct(current_totals["gmv"], previous_totals["gmv"]),
        "roas_pct": _delta_pct(current_kpis["roas"], previous_kpis["roas"]),
        "clicks_pct": _delta_pct(
            Decimal(current_totals["clicks"]), Decimal(previous_totals["clicks"])
        ),
        "impressions_pct": _delta_pct(
            Decimal(current_totals["impressions"]),
            Decimal(previous_totals["impressions"]),
        ),
        "ctr_pct": _delta_pct(current_kpis["ctr"], previous_kpis["ctr"]),
        "cpc_pct": _delta_pct(current_kpis["cpc"], previous_kpis["cpc"]),
        "cvr_pct": _delta_pct(current_kpis["cvr"], previous_kpis["cvr"]),
    }


def compute_weekly_campaign_table(
    session, shop_key: str, start_date: date_type, end_date: date_type
) -> list[dict[str, Any]]:
    rows = (
        session.query(
            AdsCampaignDaily.campaign_id,
            AdsCampaign.campaign_name,
            func.coalesce(func.sum(AdsCampaignDaily.spend), 0),
            func.coalesce(func.sum(AdsCampaignDaily.impressions), 0),
            func.coalesce(func.sum(AdsCampaignDaily.clicks), 0),
            func.coalesce(func.sum(AdsCampaignDaily.orders), 0),
            func.coalesce(func.sum(AdsCampaignDaily.gmv), 0),
        )
        .join(
            AdsCampaign,
            (AdsCampaign.shop_key == AdsCampaignDaily.shop_key)
            & (AdsCampaign.campaign_id == AdsCampaignDaily.campaign_id),
        )
        .filter(
            AdsCampaignDaily.shop_key == shop_key,
            AdsCampaignDaily.date >= start_date,
            AdsCampaignDaily.date <= end_date,
        )
        .group_by(AdsCampaignDaily.campaign_id, AdsCampaign.campaign_name)
        .all()
    )
    table: list[dict[str, Any]] = []
    for row in rows:
        spend = _to_decimal(row[2])
        gmv = _to_decimal(row[6])
        roas = _safe_div(gmv, spend)
        table.append(
            {
                "campaign_id": row[0],
                "campaign_name": row[1],
                "spend": spend,
                "impressions": int(row[3]),
                "clicks": int(row[4]),
                "orders": int(row[5]),
                "gmv": gmv,
                "roas": roas,
            }
        )
    return table


def compute_weekly_incident_summary(
    session, shop_key: str, start_date: date_type, end_date: date_type
) -> dict[str, Any]:
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    rows = (
        session.query(AdsIncident.incident_type, func.count(AdsIncident.id))
        .filter(
            AdsIncident.shop_key == shop_key,
            AdsIncident.last_seen_at >= start_dt,
            AdsIncident.last_seen_at <= end_dt,
        )
        .group_by(AdsIncident.incident_type)
        .all()
    )
    by_type = {row[0]: int(row[1]) for row in rows}

    top_rows = (
        session.query(
            AdsIncident.entity_id,
            AdsCampaign.campaign_name,
            func.count(AdsIncident.id),
        )
        .outerjoin(
            AdsCampaign,
            (AdsCampaign.shop_key == AdsIncident.shop_key)
            & (AdsCampaign.campaign_id == AdsIncident.entity_id),
        )
        .filter(
            AdsIncident.shop_key == shop_key,
            AdsIncident.entity_type == "campaign",
            AdsIncident.entity_id.isnot(None),
            AdsIncident.last_seen_at >= start_dt,
            AdsIncident.last_seen_at <= end_dt,
        )
        .group_by(AdsIncident.entity_id, AdsCampaign.campaign_name)
        .order_by(func.count(AdsIncident.id).desc())
        .limit(5)
        .all()
    )
    top_campaigns = [
        {
            "campaign_id": row[0],
            "campaign_name": row[1] or "-",
            "count": int(row[2]),
        }
        for row in top_rows
    ]

    return {
        "by_type": by_type,
        "top_campaigns": top_campaigns,
        "total": sum(by_type.values()),
    }


def generate_insights(
    metrics: dict[str, Any],
    campaign_table: list[dict[str, Any]],
    incidents: dict[str, Any],
    wow_delta: dict[str, Any] | None,
    kpi_evaluation: dict[str, Any] | None = None,
) -> list[str]:
    insights: list[str] = []
    totals = metrics["totals"]
    kpis = metrics["kpis"]
    kpi_eval = kpi_evaluation if isinstance(kpi_evaluation, dict) else {}
    spend_total = _to_decimal(totals.get("spend"))

    if spend_total == 0:
        insights.append("Không ghi nhận chi tiêu trong tuần này; kiểm tra ingest hoặc trạng thái campaign.")

    if wow_delta:
        spend_pct = wow_delta.get("spend_pct")
        roas_pct = wow_delta.get("roas_pct")
        if spend_pct and roas_pct and spend_pct > 0 and roas_pct < 0:
            insights.append(
                "Cấp shop: chi tiêu WoW tăng nhưng ROAS WoW giảm - cần siết giá thầu ở nhóm hiệu quả thấp."
            )
        if spend_pct and roas_pct and spend_pct < 0 and roas_pct > 0:
            insights.append(
                "Cấp shop: chi tiêu WoW giảm nhưng ROAS cải thiện - chiến lược cắt lọc hiện tại đang hiệu quả."
            )
        clicks_pct = wow_delta.get("clicks_pct")
        orders_pct = wow_delta.get("orders_pct")
        if clicks_pct and orders_pct and clicks_pct > 0 and orders_pct < 0:
            insights.append(
                "Tín hiệu WoW: clicks tăng nhưng đơn hàng giảm - chất lượng chuyển đổi giảm trong tuần này."
            )
        gmv_pct = wow_delta.get("gmv_pct")
        if gmv_pct and orders_pct and gmv_pct < 0 and orders_pct > 0:
            insights.append(
                "Tín hiệu WoW: đơn hàng giữ được nhưng GMV giảm - cần kiểm tra AOV và cơ cấu sản phẩm."
            )

    high_roas = [
        row
        for row in campaign_table
        if row["roas"] and row["roas"] >= Decimal("2") and row["spend"] < Decimal("100")
    ]
    if high_roas:
        top = sorted(high_roas, key=lambda r: r["roas"], reverse=True)[0]
        insights.append(
            f"{top['campaign_name']}: ROAS cao với chi tiêu thấp -> cân nhắc tăng quy mô sau khi kiểm tra tồn kho."
        )

    no_order = [
        row
        for row in campaign_table
        if row["orders"] == 0 and row["spend"] >= Decimal("20")
    ]
    if no_order:
        worst = sorted(no_order, key=lambda r: r["spend"], reverse=True)[0]
        insights.append(
            f"{worst['campaign_name']}: chi tiêu {_fmt_money(worst['spend'])} nhưng 0 đơn hàng -> cần xem lại target/keyword."
        )
        no_order_spend = sum((_to_decimal(row["spend"]) for row in no_order), Decimal("0"))
        if spend_total > 0:
            no_order_share = _safe_div(no_order_spend, spend_total)
            if no_order_share is not None and no_order_share >= Decimal("0.20"):
                insights.append(
                    f"Tỷ trọng chi tiêu không có đơn hàng đang cao ({_fmt_pct(no_order_share)})."
                )

    no_impr_count = int(incidents.get("by_type", {}).get("health_no_impressions") or 0)
    if no_impr_count:
        insights.append(
            f"Cảnh báo không có impression: {no_impr_count} lan -> kiểm tra giá thầu, target hiển thị và trạng thái."
        )
    overspend_count = int(incidents.get("by_type", {}).get("pacing_overspend") or 0)
    if overspend_count:
        insights.append(
            f"Cảnh báo vượt chi tiêu: {overspend_count} lần -> cần điều chỉnh ngân sách và quy tắc pacing."
        )
    spend_no_orders_count = int(incidents.get("by_type", {}).get("health_spend_no_orders") or 0)
    if spend_no_orders_count:
        insights.append(
            f"Cảnh báo có chi tiêu nhưng không có đơn: {spend_no_orders_count} lần -> ưu tiên xử lý nhóm hiệu quả thấp."
        )

    if kpis["ctr"] is not None and kpis["ctr"] < Decimal("0.005"):
        insights.append("CTR thấp (<0.5%); cần làm mới creative hoặc tối ưu target.")

    if kpis["cvr"] is not None and kpis["cvr"] < Decimal("0.02"):
        insights.append("CVR thấp (<2%); cần kiểm tra trang sản phẩm và mức giá.")

    quality_bits: list[str] = []
    volume_bits: list[str] = []
    risk_metrics: list[str] = []
    for key, label, group in (
        ("roas", "ROAS", "quality"),
        ("ctr", "CTR", "quality"),
        ("cvr", "CVR", "quality"),
        ("cpc", "CPC", "quality"),
        ("gmv", "GMV", "volume"),
        ("orders", "Đơn hàng", "volume"),
        ("clicks", "Clicks", "volume"),
        ("impressions", "Impressions", "volume"),
    ):
        metric_eval = (
            kpi_eval.get(key)
            if isinstance(kpi_eval.get(key), dict)
            else {}
        )
        status = str(metric_eval.get("status") or "").strip().lower()
        if not status:
            continue
        pretty = {
            "good": "tot",
            "normal": "on",
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
        insights.append("KPI chất lượng theo tuần: " + ", ".join(quality_bits) + ".")
    if volume_bits:
        insights.append("KPI quy mô theo tuần: " + ", ".join(volume_bits) + ".")
    quality_risk_count = sum(
        1 for key in ("roas", "ctr", "cvr", "cpc") if _status_of(kpi_eval, key) == "risk"
    )
    volume_risk_count = sum(
        1
        for key in ("spend", "gmv", "orders", "clicks", "impressions")
        if _status_of(kpi_eval, key) == "risk"
    )
    if quality_risk_count >= 2:
        insights.append("Rủi ro chất lượng theo tuần mang tính hệ thống (>=2 KPI chất lượng đang rủi ro).")
    elif quality_risk_count == 1:
        insights.append("Co 1 KPI chat luong dang rủi ro; can uu tien toi uu diem nay.")
    if volume_risk_count >= 2:
        insights.append("Quy mô nhu cầu theo tuần đang dưới baseline trên nhiều KPI.")
    if risk_metrics:
        insights.append("Trọng tâm theo tuần: " + ", ".join(risk_metrics) + " đang dưới baseline.")

    # Concentration check for top spend dependency.
    spend_rows = [row for row in campaign_table if _to_decimal(row.get("spend")) > 0]
    if spend_rows and spend_total > 0:
        top_spend = max((_to_decimal(row.get("spend")) for row in spend_rows), default=Decimal("0"))
        top_share = _safe_div(top_spend, spend_total)
        if top_share is not None and top_share >= Decimal("0.45"):
            insights.append(
                f"Mức độ tập trung chi tiêu cao (campaign top chiếm {_fmt_pct(top_share)} chi tiêu tuần)."
            )

    close_lines: list[str] = []
    if risk_metrics:
        close_lines.append("Uu tien: on dinh " + ", ".join(risk_metrics) + ".")
    if _status_of(kpi_eval, "roas") == "good" and _status_of(kpi_eval, "orders") in {"good", "normal"}:
        close_lines.append("Hành động: tiếp tục mở rộng nhóm tốt, có giới hạn tồn kho/biên lợi nhuận.")
    if _status_of(kpi_eval, "cvr") in {"watch", "risk"}:
        close_lines.append("Hành động: cải thiện luồng chuyển đổi trước khi tăng ngân sách.")
    if _status_of(kpi_eval, "ctr") in {"watch", "risk"}:
        close_lines.append("Hành động: làm mới creative và target audience cho tuần tới.")
    if not close_lines:
        close_lines.append("Hành động: duy trì vận hành nền và theo dõi biến động WoW.")
    insights.extend("Tổng kết tuần - " + line for line in close_lines[:2])

    while len(insights) < 5:
        insights.append("Xem lại campaign ROAS cao và tăng quy mô thận trọng theo tồn kho.")

    return dedupe_keep_order(insights)[:10]


def render_weekly_html(
    shop_label: str,
    week_id_value: str,
    start_date: date_type,
    end_date: date_type,
    payload: dict[str, Any],
) -> str:
    totals = payload["metrics"]["totals"]
    kpis = payload["metrics"]["kpis"]
    wow_delta = payload.get("wow_delta")
    generated_at = payload.get("generated_at")

    lines = [
        "<!doctype html>",
        "<html lang='vi'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>Báo cáo quảng cáo theo tuần - {shop_label} - {week_id_value}</title>",
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
        ".score-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:8px 0 18px 0}",
        ".score-card{border:1px solid var(--line);border-radius:12px;padding:12px;background:var(--brand-soft)}",
        ".score-card .k{font-size:12px;color:var(--muted);margin-bottom:4px}",
        ".score-card .v{font-size:18px;font-weight:700;color:#0b3f3a}",
        ".kpi-chip{display:inline-flex;align-items:center;padding:1px 7px;border-radius:999px;border:1px solid transparent;font-size:10px;font-weight:700;line-height:1.2;margin-left:6px;vertical-align:middle}",
        ".kpi-good{background:#ecfdf5;color:#047857;border-color:#6ee7b7}",
        ".kpi-normal{background:#eff6ff;color:#1d4ed8;border-color:#93c5fd}",
        ".kpi-watch{background:#fffbeb;color:#b45309;border-color:#fcd34d}",
        ".kpi-risk{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}",
        ".kpi-na{background:#f8fafc;color:#64748b;border-color:#cbd5e1}",
        ".wow-note{border-left:4px solid var(--warn);padding:10px 12px;background:#fffaf0;color:#7c2d12;border-radius:10px;margin:8px 0 16px 0}",
        ".wow-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:16px}",
        ".wow-card{border:1px solid var(--line);border-radius:10px;padding:10px;background:#fff}",
        ".wow-card .k{font-size:12px;color:var(--muted);margin-bottom:4px}",
        ".wow-card .v{font-size:16px;font-weight:700;color:#0f172a}",
        "table{width:100%;border-collapse:separate;border-spacing:0;margin-bottom:16px;border:1px solid var(--line);border-radius:12px;overflow:hidden;background:#fff}",
        "th,td{padding:10px 12px;border-bottom:1px solid #e9eef5;text-align:right;vertical-align:top}",
        "th{background:#f8fafc;font-weight:700;font-size:13px;color:#334155}",
        "tr:last-child td{border-bottom:none}",
        "th:first-child,td:first-child{text-align:left}",
        ".table-scroll{overflow-x:auto;margin-bottom:16px}",
        ".table-scroll table{min-width:760px;margin-bottom:0}",
        ".table-scroll.compact table{min-width:0}",
        ".campaign-col{min-width:220px;max-width:320px;text-align:left}",
        ".campaign-name{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;line-height:1.25;max-height:2.5em}",
        ".campaign-id{display:block;font-size:11px;color:var(--muted);margin-top:2px}",
        ".incident-campaign-col{min-width:160px;max-width:220px;text-align:left}",
        ".incident-campaign-name{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;line-height:1.25;max-height:2.5em}",
        ".insights-list{margin:0;padding-left:18px}",
        ".insights-list li{margin:6px 0}",
        ".empty{padding:10px 12px;border:1px dashed var(--line);border-radius:10px;background:#fafcff;color:var(--muted)}",
        "@media (max-width:900px){.score-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.wow-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}",
        "@media (max-width:640px){body{padding:12px}.report-shell{padding:14px;border-radius:12px}h1{font-size:22px}th,td{padding:8px 9px;font-size:12px}.campaign-col{min-width:180px;max-width:230px}.table-scroll table{min-width:700px}}",
        "</style>",
        "</head>",
        "<body>",
        "<main class='report-shell'>",
        f"<h1>{escape(str(shop_label))} — Báo cáo quảng cáo theo tuần</h1>",
        "<div class='meta-line'>"
        f"<div><strong>Tuần:</strong> {escape(str(week_id_value))}</div>"
        f"<div><strong>Khoảng ngày:</strong> {start_date.isoformat()} → {end_date.isoformat()}</div>"
        f"<div><strong>Tạo lúc:</strong> {_fmt_dt(generated_at)}</div>"
        "</div>",
        _render_report_navigation(
            shop_key=str(payload.get("shop_key") or ""),
            fallback_shop_label=shop_label,
            current_week_id=week_id_value,
        ),
        "<h2>Bảng chỉ số <span class='section-note'>(% so với tuần trước)</span></h2>",
        _render_weekly_scorecard(
            metrics=payload["metrics"],
            wow_delta=wow_delta,
            kpi_evaluation=(
                payload.get("kpi_evaluation")
                if isinstance(payload.get("kpi_evaluation"), dict)
                else {}
            ),
        ),
        _render_weekly_kpi_legend(payload),
    ]

    lines.extend(
        [
            "<h2>Phân bổ chi tiêu</h2>",
            _render_campaign_table(
                payload["top_spend"],
                include_orders=True,
                lead_rows=[payload["non_product_pool"]]
                if isinstance(payload.get("non_product_pool"), dict)
                else None,
            ),
            "<h2>ROAS cao nhất</h2>",
            _render_campaign_table(
                payload["top_roas_ranked"],
                include_orders=True,
            ),
            "<h2>10 campaign sản phẩm chi tiêu cao nhưng không có đơn</h2>",
            _render_campaign_table(payload["worst_no_orders"], include_orders=True),
            "<h2>Tổng hợp sự cố</h2>",
            _render_incident_summary(payload["incidents"]),
            "<h2>Nhận định</h2>",
            _render_insights(payload["insights"]),
            "</main>",
            "</body>",
            "</html>",
        ]
    )

    return "\n".join(lines)


def write_weekly_report_file(shop_key: str, week_id_value: str, html: str) -> Path:
    settings = get_settings()
    base_dir = Path(settings.reports_dir)
    target_dir = base_dir / shop_key / "weekly"
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{week_id_value}.html"
    path.write_text(html, encoding="utf-8")
    return path


def build_weekly_discord_message(
    shop_label: str,
    week_id_value: str,
    metrics: dict[str, Any],
    wow_delta: dict[str, Any] | None,
    report_url: str | None,
) -> str:
    totals = metrics["totals"]
    kpis = metrics["kpis"]
    lines = [f"Báo cáo Ads theo tuần {week_id_value}"]
    lines.append(
        f"spend={_fmt_money(totals['spend'])} orders={totals['orders']} gmv={_fmt_money(totals['gmv'])} roas={_fmt_ratio(kpis['roas'])}"
    )
    if wow_delta:
        lines.append(
            "So với tuần trước: spend={} gmv={} roas={}".format(
                _fmt_pct(wow_delta.get("spend_pct")),
                _fmt_pct(wow_delta.get("gmv_pct")),
                _fmt_pct(wow_delta.get("roas_pct")),
            )
        )
    if report_url:
        lines.append(report_url)
    return "\n".join(lines)


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _compute_kpis(totals: dict[str, Any]) -> dict[str, Any]:
    spend = totals["spend"]
    impressions = totals["impressions"]
    clicks = totals["clicks"]
    orders = totals["orders"]
    gmv = totals["gmv"]

    return {
        "roas": _safe_div(gmv, spend),
        "ctr": _safe_div(Decimal(clicks), Decimal(impressions)) if impressions > 0 else None,
        "cpc": _safe_div(spend, Decimal(clicks)) if clicks > 0 else None,
        "cvr": _safe_div(Decimal(orders), Decimal(clicks)) if clicks > 0 else None,
    }


def _render_campaign_table(
    rows: list[dict[str, Any]],
    include_orders: bool,
    lead_rows: list[dict[str, Any]] | None = None,
) -> str:
    display_rows: list[dict[str, Any]] = []
    if lead_rows:
        display_rows.extend([row for row in lead_rows if isinstance(row, dict)])
    display_rows.extend(rows)
    if not display_rows:
        return (
            "<div class='empty'>"
            "Không có dòng campaign sản phẩm trong giai đoạn này. "
            "Hãy xem bảng chỉ số để theo dõi tổng cấp shop."
            "</div>"
        )
    lines = [
        "<div class='table-scroll'>",
        "<table>",
        "<tr><th>#</th><th>Sản phẩm / Chiến dịch</th><th>Chi tiêu</th><th>GMV</th><th>ROAS</th>",
    ]
    if include_orders:
        lines[2] = lines[2] + "<th>Đơn hàng</th>"
    lines[2] = lines[2] + "<th>Clicks</th><th>Impressions</th></tr>"
    for idx, row in enumerate(display_rows, start=1):
        orders_cell = f"<td>{row.get('orders', 0)}</td>" if include_orders else ""
        raw_campaign_id = str(row.get("campaign_id") or "-")
        raw_campaign_name = str(row.get("campaign_name") or "-")
        campaign_name = escape(_display_campaign_name(raw_campaign_id, raw_campaign_name))
        campaign_id = escape(raw_campaign_id)
        lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            "<td class='campaign-col'>"
            f"<span class='campaign-name'>{campaign_name}</span>"
            f"<span class='campaign-id'>{campaign_id}</span>"
            "</td>"
            f"<td>{_fmt_money(row['spend'])}</td>"
            f"<td>{_fmt_money(row['gmv'])}</td>"
            f"<td>{_fmt_ratio(row['roas'])}</td>"
            f"{orders_cell}"
            f"<td>{int(row.get('clicks') or 0)}</td>"
            f"<td>{int(row.get('impressions') or 0)}</td>"
            "</tr>"
        )
    lines.append("</table>")
    lines.append("</div>")
    return "\n".join(lines)


def _render_incident_summary(incidents: dict[str, Any]) -> str:
    by_type = incidents.get("by_type", {})
    if not by_type:
        return "<div class='empty'>(không có sự cố)</div>"
    summary_bits = [
        f"{_incident_type_label(str(key))}: {int(value)}"
        for key, value in sorted(by_type.items(), key=lambda item: item[1], reverse=True)
    ]
    lines = [f"<div><small>{escape(' | '.join(summary_bits))}</small></div>"]
    lines.extend(["<div class='table-scroll compact'>", "<table>", "<tr><th>Loại</th><th>Số lần</th></tr>"])
    for key, value in sorted(by_type.items(), key=lambda item: item[1], reverse=True):
        lines.append(f"<tr><td>{escape(_incident_type_label(str(key)))}</td><td>{int(value)}</td></tr>")
    lines.append("</table>")
    lines.append("</div>")

    top_campaigns = incidents.get("top_campaigns", [])
    if top_campaigns:
        lines.append("<div><strong>Campaign có nhiều cảnh báo nhất</strong></div>")
        lines.append("<div class='table-scroll compact'>")
        lines.append("<table>")
        lines.append("<tr><th>Chiến dịch</th><th>Số lần</th></tr>")
        for row in top_campaigns:
            campaign_name = escape(str(row.get("campaign_name") or "-"))
            campaign_id = escape(str(row.get("campaign_id") or "-"))
            short_name = escape(_trim_text(str(row.get("campaign_name") or "-"), max_chars=38))
            lines.append(
                "<tr>"
                "<td class='incident-campaign-col'>"
                f"<span class='incident-campaign-name' title='{campaign_name}'>{short_name}</span>"
                f"<span class='campaign-id'>{campaign_id}</span>"
                "</td>"
                f"<td>{row['count']}</td>"
                "</tr>"
            )
        lines.append("</table>")
        lines.append("</div>")
    return "\n".join(lines)


def _render_insights(insights: list[str]) -> str:
    if not insights:
        return "<div class='empty'>(không có nhận định)</div>"
    lines = ["<ul class='insights-list'>"]
    for item in insights:
        lines.append(f"<li>{escape(str(item))}</li>")
    lines.append("</ul>")
    return "\n".join(lines)


def _render_weekly_scorecard(
    *,
    metrics: dict[str, Any],
    wow_delta: dict[str, Any] | None = None,
    kpi_evaluation: dict[str, Any] | None = None,
) -> str:
    totals = metrics.get("totals") if isinstance(metrics, dict) else {}
    kpis = metrics.get("kpis") if isinstance(metrics, dict) else {}
    spend = _fmt_money(_to_decimal((totals or {}).get("spend")))
    gmv = _fmt_money(_to_decimal((totals or {}).get("gmv")))
    orders = int((totals or {}).get("orders") or 0)
    impressions = int((totals or {}).get("impressions") or 0)
    clicks = int((totals or {}).get("clicks") or 0)
    roas = _fmt_ratio((kpis or {}).get("roas"))
    ctr = _fmt_pct((kpis or {}).get("ctr"))
    cpc = _fmt_money((kpis or {}).get("cpc"))
    cvr = _fmt_pct((kpis or {}).get("cvr"))
    wow = wow_delta if isinstance(wow_delta, dict) else {}
    eval_map = kpi_evaluation if isinstance(kpi_evaluation, dict) else {}
    cards: list[tuple[str, str, str, Decimal | None, Decimal | None]] = [
        ("spend", "Chi tiêu", spend, wow.get("spend_pct"), _to_decimal((totals or {}).get("spend"))),
        ("gmv", "GMV", gmv, wow.get("gmv_pct"), _to_decimal((totals or {}).get("gmv"))),
        ("orders", "Đơn hàng", str(orders), wow.get("orders_pct"), Decimal(orders)),
        ("roas", "ROAS", roas, wow.get("roas_pct"), _to_decimal((kpis or {}).get("roas"))),
        ("clicks", "Lượt click", f"{clicks}", wow.get("clicks_pct"), Decimal(clicks)),
        ("impressions", "Lượt hiển thị", f"{impressions}", wow.get("impressions_pct"), Decimal(impressions)),
        ("ctr", "CTR", ctr, wow.get("ctr_pct"), _to_decimal((kpis or {}).get("ctr"))),
        ("cpc", "CPC", cpc, wow.get("cpc_pct"), _to_decimal((kpis or {}).get("cpc"))),
        ("cvr", "CVR", cvr, wow.get("cvr_pct"), _to_decimal((kpis or {}).get("cvr"))),
    ]
    lines = ["<div class='score-grid'>"]
    for metric_key, label, value, delta, raw_value in cards:
        metric_eval = (
            eval_map.get(metric_key)
            if isinstance(eval_map.get(metric_key), dict)
            else {}
        )
        metric_eval = _resolve_weekly_metric_eval(
            metric_key=metric_key,
            metric_eval=metric_eval,
            delta=delta,
            value=raw_value,
        )
        lines.append(
            "<div class='score-card'>"
            f"<div class='k'>{escape(label)}</div>"
            "<div class='v'>"
            f"{_render_value_with_delta(value, delta, metric_key=metric_key)}"
            f"{_render_weekly_kpi_chip(metric_eval)}"
            "</div>"
            "</div>"
        )
    lines.append("</div>")
    return "\n".join(lines)


def _render_weekly_kpi_chip(metric_eval: dict[str, Any]) -> str:
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


def _resolve_weekly_metric_eval(
    *,
    metric_key: str,
    metric_eval: dict[str, Any],
    delta: Decimal | None,
    value: Decimal | None,
) -> dict[str, Any]:
    status = str(metric_eval.get("status") or "").strip().lower()
    if status and status != "n/a":
        return metric_eval
    fallback_status = _weekly_delta_fallback_status(metric_key=metric_key, delta=delta)
    if not fallback_status:
        fallback_status = _weekly_absolute_fallback_status(
            metric_key=metric_key,
            value=value if value is not None else (_to_decimal(metric_eval.get("value")) if metric_eval.get("value") is not None else None),
        )
    if not fallback_status:
        return metric_eval
    merged = dict(metric_eval)
    merged["status"] = fallback_status
    merged["source"] = "delta_fallback"
    return merged


def _weekly_delta_fallback_status(
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


def _weekly_absolute_fallback_status(
    *,
    metric_key: str,
    value: Decimal | None,
) -> str:
    if value is None:
        return ""
    key = str(metric_key or "").strip().lower()
    if key == "roas":
        if value >= Decimal("5"):
            return "good"
        if value >= Decimal("3"):
            return "normal"
        if value >= Decimal("1.5"):
            return "watch"
        return "risk"
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


def _render_weekly_kpi_legend(payload: dict[str, Any]) -> str:
    thresholds = (
        payload.get("kpi_thresholds")
        if isinstance(payload.get("kpi_thresholds"), dict)
        else {}
    )
    lookback_days = int(thresholds.get("lookback_days") or 180)
    return (
        "<div><small>"
        "Huy hiệu KPI: Tốt / Ổn / Cảnh báo / Rủi ro"
        f" | Cửa sổ KPI: {lookback_days} ngày gần nhất (theo tuần)"
        "</small></div>"
    )


def _render_wow_table(wow_delta: dict[str, Any]) -> str:
    cards = [
        ("Chi tiêu", _fmt_pct(wow_delta.get("spend_pct"))),
        ("Đơn hàng", _fmt_pct(wow_delta.get("orders_pct"))),
        ("GMV", _fmt_pct(wow_delta.get("gmv_pct"))),
        ("ROAS", _fmt_pct(wow_delta.get("roas_pct"))),
    ]
    lines = ["<div class='wow-grid'>"]
    for label, value in cards:
        lines.append(
            "<div class='wow-card'>"
            f"<div class='k'>{escape(label)}</div>"
            f"<div class='v'>{escape(value)}</div>"
            "</div>"
        )
    lines.append("</div>")
    return "\n".join(lines)


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


def _render_report_navigation(
    *,
    shop_key: str,
    fallback_shop_label: str,
    current_week_id: str,
) -> str:
    normalized_shop = (shop_key or "").strip()
    if not normalized_shop:
        normalized_shop = _resolve_shop_key_by_label(fallback_shop_label) or ""
    if not normalized_shop:
        return ""

    catalog = _load_report_navigation_catalog(shop_key=normalized_shop)
    weekly_ids = catalog.get("weekly_ids") or []

    weekly_options: list[str] = ["<option value=''>Chọn tuần</option>"]
    for week_id in weekly_ids:
        if week_id == current_week_id:
            weekly_options.append(
                f"<option value='{escape(week_id)}' selected>{escape(week_id)}</option>"
            )
        else:
            weekly_options.append(f"<option value='{escape(week_id)}'>{escape(week_id)}</option>")

    lines = [
        "<section class='nav-box'>",
        "<div><strong>Xem nhanh báo cáo tuần</strong></div>",
        "<div class='nav-row'>",
        "<label for='report-nav-weekly-id'>Tuần</label>",
        "<select id='report-nav-weekly-id'>",
        "".join(weekly_options),
        "</select>",
        "<button id='report-nav-open-weekly' type='button'>Xem</button>",
        "</div>",
        "<div class='nav-hint' id='report-nav-hint'></div>",
        "<script>",
        f"const reportNavShopKey={json.dumps(normalized_shop, ensure_ascii=False)};",
        f"const reportNavCatalog={json.dumps(catalog, ensure_ascii=False)};",
        "function reportNavWithToken(path){const p=new URLSearchParams(window.location.search);const t=p.get('token');if(!t){return path;}return `${path}?token=${encodeURIComponent(t)}`;}",
        "function reportNavSetHint(msg){const el=document.getElementById('report-nav-hint');if(el){el.textContent=msg||'';}}",
        "function reportNavOpenWeekly(){const weekEl=document.getElementById('report-nav-weekly-id');if(!weekEl){return;}const weekId=weekEl.value;if(!weekId){reportNavSetHint('Hãy chọn tuần trước.');return;}window.location.href=reportNavWithToken(`/reports/${reportNavShopKey}/weekly/${weekId}.html`);}",
        "document.getElementById('report-nav-open-weekly')?.addEventListener('click', reportNavOpenWeekly);",
        "</script>",
        "</section>",
    ]
    return "\n".join(lines)


def _resolve_shop_key_by_label(shop_label: str) -> str | None:
    target = (shop_label or "").strip()
    if not target:
        return None
    for shop in load_shops():
        if str(shop.label or "").strip() == target:
            return str(shop.shop_key)
    return None


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


def _status_of(kpi_eval: dict[str, Any], key: str) -> str:
    metric_eval = kpi_eval.get(key) if isinstance(kpi_eval.get(key), dict) else {}
    return str(metric_eval.get("status") or "").strip().lower()


def _kpi_threshold_reference_date(start_date: date_type) -> date_type:
    # Keep weekly KPI baseline stable inside the month; refresh next month.
    return start_date.replace(day=1)


def _build_weekly_kpi_thresholds(
    *,
    session,
    shop_key: str,
    reference_date: date_type,
    lookback_days: int = 180,
    min_weeks: int = 8,
) -> dict[str, Any]:
    start_date = reference_date - timedelta(days=max(lookback_days, 1))
    rows = (
        session.query(
            AdsCampaignDaily.date,
            AdsCampaignDaily.spend,
            AdsCampaignDaily.impressions,
            AdsCampaignDaily.clicks,
            AdsCampaignDaily.orders,
            AdsCampaignDaily.gmv,
        )
        .filter(
            AdsCampaignDaily.shop_key == shop_key,
            AdsCampaignDaily.campaign_id == "SHOP_TOTAL",
            AdsCampaignDaily.date >= start_date,
            AdsCampaignDaily.date < reference_date,
        )
        .order_by(AdsCampaignDaily.date.asc())
        .all()
    )
    weekly_map: dict[date_type, dict[str, Any]] = {}
    for row in rows:
        row_date = row[0]
        week_start = row_date - timedelta(days=row_date.weekday())
        bucket = weekly_map.setdefault(
            week_start,
            {
                "spend": Decimal("0"),
                "impressions": 0,
                "clicks": 0,
                "orders": 0,
                "gmv": Decimal("0"),
            },
        )
        bucket["spend"] += _to_decimal(row[1])
        bucket["impressions"] += int(row[2] or 0)
        bucket["clicks"] += int(row[3] or 0)
        bucket["orders"] += int(row[4] or 0)
        bucket["gmv"] += _to_decimal(row[5])

    roas_values: list[Decimal] = []
    ctr_values: list[Decimal] = []
    cvr_values: list[Decimal] = []
    cpc_values: list[Decimal] = []
    spend_values: list[Decimal] = []
    gmv_values: list[Decimal] = []
    orders_values: list[Decimal] = []
    clicks_values: list[Decimal] = []
    impressions_values: list[Decimal] = []

    for week_start in sorted(weekly_map.keys()):
        bucket = weekly_map[week_start]
        spend = _to_decimal(bucket["spend"])
        if spend <= 0:
            continue
        gmv = _to_decimal(bucket["gmv"])
        impressions = int(bucket["impressions"] or 0)
        clicks = int(bucket["clicks"] or 0)
        orders = int(bucket["orders"] or 0)

        spend_values.append(spend)
        gmv_values.append(gmv)
        orders_values.append(Decimal(orders))
        clicks_values.append(Decimal(clicks))
        impressions_values.append(Decimal(impressions))
        roas_values.append(_safe_div(gmv, spend) or Decimal("0"))
        if impressions > 0:
            ctr_values.append(Decimal(clicks) / Decimal(impressions))
        if clicks > 0:
            cvr_values.append(Decimal(orders) / Decimal(clicks))
            cpc_values.append(spend / Decimal(clicks))

    return {
        "cadence": "monthly",
        "reference_date": reference_date.isoformat(),
        "lookback_days": max(lookback_days, 1),
        "active_weeks": len(spend_values),
        "min_weeks": max(min_weeks, 1),
        "spend": _build_kpi_metric_threshold(
            values=spend_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "gmv": _build_kpi_metric_threshold(
            values=gmv_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "orders": _build_kpi_metric_threshold(
            values=orders_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "clicks": _build_kpi_metric_threshold(
            values=clicks_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "impressions": _build_kpi_metric_threshold(
            values=impressions_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "roas": _build_kpi_metric_threshold(
            values=roas_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "ctr": _build_kpi_metric_threshold(
            values=ctr_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "cvr": _build_kpi_metric_threshold(
            values=cvr_values,
            direction="high",
            min_points=max(min_weeks, 1),
            good_percentile=70,
            normal_percentile=50,
            watch_percentile=35,
        ),
        "cpc": _build_kpi_metric_threshold(
            values=cpc_values,
            direction="low",
            min_points=max(min_weeks, 1),
            good_percentile=30,
            normal_percentile=50,
            watch_percentile=65,
        ),
    }


def _build_kpi_metric_threshold(
    *,
    values: list[Decimal],
    direction: str,
    min_points: int,
    good_percentile: int,
    normal_percentile: int,
    watch_percentile: int,
) -> dict[str, Any]:
    count = len(values)
    if count < max(min_points, 1):
        return {
            "enabled": 0,
            "direction": direction,
            "points": count,
            "good_cutoff": None,
            "normal_cutoff": None,
            "watch_cutoff": None,
            "good_percentile": good_percentile,
            "normal_percentile": normal_percentile,
            "watch_percentile": watch_percentile,
        }
    return {
        "enabled": 1,
        "direction": direction,
        "points": count,
        "good_cutoff": _percentile_decimal(values, good_percentile),
        "normal_cutoff": _percentile_decimal(values, normal_percentile),
        "watch_cutoff": _percentile_decimal(values, watch_percentile),
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
    rank = Decimal(max(0, min(percentile, 100))) / Decimal("100")
    raw_index = Decimal(len(materialized) - 1) * rank
    lower_index = int(raw_index.to_integral_value(rounding=ROUND_HALF_UP))
    lower_index = min(max(lower_index, 0), len(materialized) - 1)
    return materialized[lower_index]


def _evaluate_weekly_kpis(
    *,
    metrics: dict[str, Any],
    kpi_thresholds: dict[str, Any],
) -> dict[str, Any]:
    totals = metrics.get("totals") if isinstance(metrics, dict) else {}
    kpis = metrics.get("kpis") if isinstance(metrics, dict) else {}
    current_values = {
        "spend": _to_decimal((totals or {}).get("spend")),
        "gmv": _to_decimal((totals or {}).get("gmv")),
        "orders": Decimal(int((totals or {}).get("orders") or 0)),
        "clicks": Decimal(int((totals or {}).get("clicks") or 0)),
        "impressions": Decimal(int((totals or {}).get("impressions") or 0)),
        "roas": (kpis or {}).get("roas"),
        "ctr": (kpis or {}).get("ctr"),
        "cvr": (kpis or {}).get("cvr"),
        "cpc": (kpis or {}).get("cpc"),
    }
    out: dict[str, Any] = {}
    nullable_keys = {"roas", "ctr", "cvr", "cpc"}
    for key in (
        "spend",
        "gmv",
        "orders",
        "clicks",
        "impressions",
        "roas",
        "ctr",
        "cvr",
        "cpc",
    ):
        metric_cfg = (
            kpi_thresholds.get(key)
            if isinstance(kpi_thresholds.get(key), dict)
            else {}
        )
        value = current_values.get(key)
        if key in nullable_keys and value is None:
            value_decimal = None
        else:
            value_decimal = value if isinstance(value, Decimal) else _to_decimal(value)
        out[key] = {
            "status": _evaluate_metric_status(value=value_decimal, metric_cfg=metric_cfg),
            "value": value_decimal,
        }
    return out


def _evaluate_metric_status(*, value: Decimal | None, metric_cfg: dict[str, Any]) -> str:
    if value is None:
        return "n/a"
    if int(metric_cfg.get("enabled") or 0) != 1:
        return "n/a"
    direction = str(metric_cfg.get("direction") or "").strip().lower()
    good_cutoff = _to_decimal(metric_cfg.get("good_cutoff"))
    normal_cutoff = _to_decimal(metric_cfg.get("normal_cutoff"))
    watch_cutoff = _to_decimal(metric_cfg.get("watch_cutoff"))
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


def _delta_pct(current: Decimal | None, previous: Decimal | None) -> Decimal | None:
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    return (current - previous) / previous


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _to_decimal(value: Decimal | int | float | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "-"
    rounded = _to_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    return f"{sign}{abs(rounded):,}₫"


def _fmt_ratio(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return _quantize(value, 2)


def _fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return _quantize(value * Decimal("100"), 2) + "%"


def _quantize(value: Decimal, places: int) -> str:
    quant = Decimal("1").scaleb(-places)
    return f"{value.quantize(quant, rounding=ROUND_HALF_UP)}"


def _fmt_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    tz = resolve_timezone(get_settings().timezone)
    localized = value.astimezone(tz) if value.tzinfo else value.replace(tzinfo=tz)
    return localized.strftime("%Y-%m-%d %H:%M:%S")


def _display_campaign_name(campaign_id: str, campaign_name: str) -> str:
    cid = str(campaign_id or "").strip().upper()
    name = str(campaign_name or "").strip() or "-"
    if cid == "SHOP_TOTAL":
        return "SHOP_TOTAL (Tong cap shop tren tat ca loai quang cao)"
    return name


def _is_shop_total_campaign_id(campaign_id: str | None) -> bool:
    return str(campaign_id or "").strip().upper() == "SHOP_TOTAL"


def _incident_type_label(value: str) -> str:
    key = (value or "").strip().lower()
    labels = {
        "health_no_impressions": "Không có impression",
        "health_spend_no_orders": "Có chi tiêu nhưng không có đơn",
        "pacing_overspend": "Vượt pacing",
        "pacing_underdelivery": "Thiếu pacing",
    }
    return labels.get(key, value or "-")


def _trim_text(value: str, *, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1] + "..."


def build_weekly_payload(session, shop_key: str, start_date: date_type, end_date: date_type) -> dict[str, Any]:
    metrics = compute_weekly_metrics(session, shop_key, start_date, end_date)
    kpi_reference_date = _kpi_threshold_reference_date(start_date)
    kpi_thresholds = _build_weekly_kpi_thresholds(
        session=session,
        shop_key=shop_key,
        reference_date=kpi_reference_date,
        lookback_days=180,
        min_weeks=8,
    )
    kpi_evaluation = _evaluate_weekly_kpis(
        metrics=metrics,
        kpi_thresholds=kpi_thresholds,
    )
    campaign_table_all = compute_weekly_campaign_table(session, shop_key, start_date, end_date)
    campaign_table = [
        row
        for row in campaign_table_all
        if not _is_shop_total_campaign_id(str(row.get("campaign_id") or ""))
    ]
    incidents = compute_weekly_incident_summary(session, shop_key, start_date, end_date)
    wow = compute_wow_delta(session, shop_key, start_date, end_date)

    top_spend = [row for row in campaign_table if row["spend"] > 0]
    top_spend = sorted(top_spend, key=lambda r: r["spend"], reverse=True)[:10]
    top_roas = [row for row in campaign_table if row["spend"] >= Decimal("10")]
    top_roas = sorted(top_roas, key=lambda r: r["roas"] or Decimal("0"), reverse=True)[:10]
    worst_no_orders = [row for row in campaign_table if row["orders"] == 0 and row["spend"] > 0]
    worst_no_orders = sorted(worst_no_orders, key=lambda r: r["spend"], reverse=True)[:10]
    non_product_pool = _build_non_product_pool_row(
        totals=metrics.get("totals") if isinstance(metrics, dict) else None,
        product_rows=campaign_table,
    )
    top_roas_ranked = _build_top_roas_ranked_rows(
        product_rows=top_roas,
        non_product_pool=non_product_pool,
    )

    insights = generate_insights(
        metrics,
        campaign_table,
        incidents,
        wow,
        kpi_evaluation=kpi_evaluation,
    )

    return {
        "shop_key": shop_key,
        "metrics": metrics,
        "kpi_thresholds": kpi_thresholds,
        "kpi_evaluation": kpi_evaluation,
        "campaign_table": campaign_table,
        "top_spend": top_spend,
        "top_roas": top_roas,
        "top_roas_ranked": top_roas_ranked,
        "worst_no_orders": worst_no_orders,
        "non_product_pool": non_product_pool,
        "incidents": incidents,
        "wow_delta": wow,
        "insights": insights,
        "generated_at": datetime.now(resolve_timezone(get_settings().timezone)),
    }


def _build_non_product_pool_row(
    *,
    totals: dict[str, Any] | None,
    product_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not isinstance(totals, dict):
        return None
    total_spend = _to_decimal(totals.get("spend"))
    total_impr = int(totals.get("impressions") or 0)
    total_clicks = int(totals.get("clicks") or 0)
    total_orders = int(totals.get("orders") or 0)
    total_gmv = _to_decimal(totals.get("gmv"))

    product_spend = sum((_to_decimal(row.get("spend")) for row in product_rows), Decimal("0"))
    product_impr = sum((int(row.get("impressions") or 0) for row in product_rows), 0)
    product_clicks = sum((int(row.get("clicks") or 0) for row in product_rows), 0)
    product_orders = sum((int(row.get("orders") or 0) for row in product_rows), 0)
    product_gmv = sum((_to_decimal(row.get("gmv")) for row in product_rows), Decimal("0"))

    remain_spend = total_spend - product_spend
    remain_impr = total_impr - product_impr
    remain_clicks = total_clicks - product_clicks
    remain_orders = total_orders - product_orders
    remain_gmv = total_gmv - product_gmv

    if remain_spend < 0:
        remain_spend = Decimal("0")
    if remain_impr < 0:
        remain_impr = 0
    if remain_clicks < 0:
        remain_clicks = 0
    if remain_orders < 0:
        remain_orders = 0
    if remain_gmv < 0:
        remain_gmv = Decimal("0")

    if (
        remain_spend == 0
        and remain_impr == 0
        and remain_clicks == 0
        and remain_orders == 0
        and remain_gmv == 0
    ):
        return None

    return {
        "campaign_id": "NON_PRODUCT_POOL",
        "campaign_name": "Tong hợp Group/Shop/Auto (SHOP_TOTAL - campaign sản phẩm)",
        "spend": remain_spend,
        "impressions": remain_impr,
        "clicks": remain_clicks,
        "orders": remain_orders,
        "gmv": remain_gmv,
        "roas": _safe_div(remain_gmv, remain_spend),
    }


def _build_top_roas_ranked_rows(
    *,
    product_rows: list[dict[str, Any]],
    non_product_pool: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    candidates = list(product_rows)
    if isinstance(non_product_pool, dict) and _to_decimal(non_product_pool.get("spend")) >= Decimal("10"):
        candidates.append(non_product_pool)
    ranked = sorted(
        candidates,
        key=lambda row: (
            _to_decimal(row.get("roas") or Decimal("0")),
            _to_decimal(row.get("spend") or Decimal("0")),
        ),
        reverse=True,
    )
    return ranked[:10]

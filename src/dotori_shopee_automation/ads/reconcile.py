from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import json
import re
from typing import Any
import unicodedata

from .metrics import aggregate_metric_rows, build_surface_metrics_snapshot, nullable_decimal, to_decimal
from .reporting import aggregate_daily_report, report_scope_line, report_surface_metrics


METRIC_KEYS: tuple[str, ...] = (
    "spend",
    "impressions",
    "clicks",
    "orders",
    "gmv",
    "roas",
    "ctr",
    "cpc",
    "cvr",
)


@dataclass
class ReconcileResult:
    payload: dict[str, Any]
    json_path: Path
    md_path: Path


def run_report_reconcile(
    *,
    session,
    shop_key: str,
    kind: str,
    report_date: date_type,
    reports_dir: Path,
    artifacts_dir: Path,
    raw_artifacts_root: Path | None = None,
) -> ReconcileResult:
    kind_key = kind.strip().lower()
    if kind_key not in {"final", "midday"}:
        raise ValueError("kind must be final or midday")

    as_of = (
        datetime.combine(report_date, time(13, 0))
        if kind_key == "midday"
        else None
    )
    db_data = aggregate_daily_report(
        session,
        shop_key,
        report_date,
        as_of,
    )
    db_metrics = report_surface_metrics(db_data)

    report_path = reports_dir / shop_key / "daily" / f"{report_date.isoformat()}_{kind_key}.html"
    rendered_metrics, rendered_parse_mode = _extract_rendered_metrics(report_path)

    raw_root = raw_artifacts_root or (Path("collaboration") / "artifacts")
    raw_probe = _extract_raw_metrics(
        root=raw_root,
        shop_key=shop_key,
        target_date=report_date,
    )

    comparison: list[dict[str, Any]] = []
    for key in METRIC_KEYS:
        raw_value = raw_probe.get("metrics", {}).get(key)
        db_value = db_metrics.get(key)
        rendered_value = rendered_metrics.get(key) if isinstance(rendered_metrics, dict) else None
        comparison.append(
            {
                "metric": key,
                "raw_source_value": _serialize_metric_value(raw_value),
                "db_aggregated_value": _serialize_metric_value(db_value),
                "rendered_value": _serialize_metric_value(rendered_value),
                "difference": {
                    "raw_minus_db": _serialize_metric_value(_metric_diff(raw_value, db_value)),
                    "rendered_minus_db": _serialize_metric_value(
                        _metric_diff(rendered_value, db_value)
                    ),
                },
                "root_cause_guess": _metric_root_cause_guess(
                    raw_value=raw_value,
                    db_value=db_value,
                    rendered_value=rendered_value,
                ),
            }
        )

    root_cause_summary = _overall_root_cause(
        comparison=comparison,
        report_path=report_path,
        rendered_parse_mode=rendered_parse_mode,
    )
    payload = {
        "shop": shop_key,
        "kind": kind_key,
        "date": report_date.isoformat(),
        "window": {
            "report_kind": kind_key,
            "window_start": report_date.isoformat(),
            "window_end": report_date.isoformat(),
            "as_of": as_of.isoformat() if isinstance(as_of, datetime) else None,
        },
        "scope_line": report_scope_line(db_data),
        "breakdown_scope": str(db_data.get("breakdown_scope") or "product_level_only"),
        "gms_group_scope": str(db_data.get("gms_group_scope") or "aggregate_only"),
        "report_path": str(report_path),
        "report_exists": int(report_path.exists()),
        "rendered_parse_mode": rendered_parse_mode,
        "raw_source_path": raw_probe.get("path"),
        "raw_source_row_count": int(raw_probe.get("row_count") or 0),
        "comparison": comparison,
        "root_cause_summary": root_cause_summary,
        "fix_applied": "single_source_of_truth_metrics_snapshot",
    }

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    stem = f"reconcile_{shop_key}_{kind_key}_{report_date.isoformat()}"
    json_path = artifacts_dir / f"{stem}.json"
    md_path = artifacts_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_reconcile_markdown(payload), encoding="utf-8")
    return ReconcileResult(payload=payload, json_path=json_path, md_path=md_path)


def _extract_rendered_metrics(report_path: Path) -> tuple[dict[str, Any], str]:
    if not report_path.exists():
        return {}, "missing"
    text = report_path.read_text(encoding="utf-8", errors="ignore")
    parsed = _extract_metrics_from_embedded_json(text)
    if parsed:
        return parsed, "embedded_json"
    parsed = _extract_metrics_from_scorecard_table(text)
    if parsed:
        return parsed, "scorecard_table"
    parsed = _extract_metrics_from_score_matrix_cards(text)
    if parsed:
        return parsed, "score_matrix_cards"
    return {}, "unparsable"


def _extract_metrics_from_embedded_json(text: str) -> dict[str, Any]:
    pattern = re.compile(
        r"<script[^>]*id=['\"]dotori-report-metrics['\"][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )
    matched = pattern.search(text)
    if not matched:
        return {}
    body = matched.group(1).strip()
    if not body:
        return {}
    try:
        payload = json.loads(body)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, Any] = {}
    for key in METRIC_KEYS:
        out[key] = _normalize_metric_value(key, payload.get(key))
    return out


def _extract_metrics_from_scorecard_table(text: str) -> dict[str, Any]:
    label_map = {
        "Chi tiêu": "spend",
        "Hiển thị": "impressions",
        "Clicks": "clicks",
        "Đơn hàng": "orders",
        "GMV": "gmv",
        "ROAS": "roas",
        "CTR": "ctr",
        "CPC": "cpc",
        "CVR": "cvr",
    }
    out: dict[str, Any] = {}
    for label, key in label_map.items():
        pattern = re.compile(
            rf"<tr>\s*<td[^>]*>.*?{re.escape(label)}.*?</td>\s*<td[^>]*>(.*?)</td>",
            re.IGNORECASE | re.DOTALL,
        )
        matched = pattern.search(text)
        if not matched:
            continue
        raw_cell = _strip_tags(matched.group(1))
        out[key] = _parse_metric_text(key, raw_cell)
    return out


def _extract_metrics_from_score_matrix_cards(text: str) -> dict[str, Any]:
    pair_pattern = re.compile(
        r"<div[^>]*class=['\"][^'\"]*score-metric[^'\"]*['\"][^>]*>\s*"
        r"<div[^>]*class=['\"][^'\"]*label[^'\"]*['\"][^>]*>(.*?)</div>\s*"
        r"<div[^>]*class=['\"][^'\"]*value[^'\"]*['\"][^>]*>(.*?)</div>\s*"
        r"</div>",
        re.IGNORECASE | re.DOTALL,
    )
    label_map = {
        "spend": "spend",
        "chi tieu": "spend",
        "impressions": "impressions",
        "hien thi": "impressions",
        "clicks": "clicks",
        "click": "clicks",
        "orders": "orders",
        "don hang": "orders",
        "gmv": "gmv",
        "doanh so": "gmv",
        "roas": "roas",
        "ctr": "ctr",
        "cpc": "cpc",
        "cvr": "cvr",
    }
    out: dict[str, Any] = {}
    for label_html, value_html in pair_pattern.findall(text):
        raw_label = _strip_tags(label_html)
        key = label_map.get(_normalize_label(raw_label))
        if not key:
            continue
        raw_value = _strip_tags(value_html)
        out[key] = _parse_metric_text(key, raw_value)
    return out


def _extract_raw_metrics(
    *,
    root: Path,
    shop_key: str,
    target_date: date_type,
) -> dict[str, Any]:
    if not root.exists():
        return {"path": None, "row_count": 0, "metrics": {}}
    target_text = target_date.isoformat()
    candidates: list[dict[str, Any]] = []
    scanned = 0
    for path in root.rglob("*.json"):
        scanned += 1
        if scanned > 4000:
            break
        lower_path = path.as_posix().lower()
        path_has_target_date = target_text in lower_path
        if shop_key.lower() not in lower_path:
            continue
        if "raw" not in lower_path and "artifact" not in lower_path:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        rows = _extract_metric_rows_from_payload(
            payload,
            target_text=target_text,
            allow_undated_rows=path_has_target_date,
        )
        if not rows:
            continue
        totals = aggregate_metric_rows(rows)
        kpis = build_surface_metrics_snapshot(totals=totals)
        row_count = len(rows)
        candidates.append(
            {
                "path": str(path),
                "row_count": row_count,
                "metrics": kpis,
                "mtime": path.stat().st_mtime,
            }
        )
    if not candidates:
        return {"path": None, "row_count": 0, "metrics": {}}
    candidates.sort(key=lambda row: (int(row["row_count"]), float(row["mtime"])), reverse=True)
    top = candidates[0]
    return {
        "path": str(top["path"]),
        "row_count": int(top["row_count"]),
        "metrics": top["metrics"],
    }


def _extract_metric_rows_from_payload(
    payload: Any,
    *,
    target_text: str,
    allow_undated_rows: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def walk(node: Any, inherited_date: str | None = None) -> None:
        if isinstance(node, dict):
            local_date = _extract_date(node) or inherited_date
            maybe = _dict_to_metric_row(
                node,
                local_date=local_date,
                target_text=target_text,
                allow_undated_rows=allow_undated_rows,
            )
            if maybe:
                rows.append(maybe)
            for value in node.values():
                walk(value, local_date)
        elif isinstance(node, list):
            for value in node:
                walk(value, inherited_date)

    walk(payload, None)
    return rows


def _extract_date(node: dict[str, Any]) -> str | None:
    for key in ("date", "report_date", "target_date", "data_date", "day"):
        value = str(node.get(key) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return value
        ddmmyyyy = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", value)
        if ddmmyyyy:
            return f"{ddmmyyyy.group(3)}-{ddmmyyyy.group(2)}-{ddmmyyyy.group(1)}"
    return None


def _dict_to_metric_row(
    node: dict[str, Any],
    *,
    local_date: str | None,
    target_text: str,
    allow_undated_rows: bool,
) -> dict[str, Any] | None:
    if local_date and local_date != target_text:
        return None
    if local_date is None and not allow_undated_rows:
        return None
    spend = _first_numeric(node, ("spend", "spend_today", "cost"))
    impressions = _first_numeric(node, ("impressions", "impression", "views"))
    clicks = _first_numeric(node, ("clicks", "click"))
    orders = _first_numeric(node, ("orders", "order_count", "conversions"))
    gmv = _first_numeric(node, ("gmv", "sales", "revenue", "gmv_today"))
    if (
        spend is None
        and impressions is None
        and clicks is None
        and orders is None
        and gmv is None
    ):
        return None
    return {
        "spend": spend or Decimal("0"),
        "impressions": int(impressions or 0),
        "clicks": int(clicks or 0),
        "orders": int(orders or 0),
        "gmv": gmv or Decimal("0"),
    }


def _first_numeric(node: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        if key not in node:
            continue
        parsed = _parse_decimal(node.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("₫", "").replace("VND", "").strip()
    if cleaned in {"-", "null", "None"}:
        return None
    try:
        return Decimal(cleaned)
    except Exception:
        return None


def _strip_tags(value: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", value or "")
    return " ".join(no_tags.split())


def _normalize_label(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _parse_metric_text(metric: str, text: str) -> Any:
    raw = (text or "").strip()
    if not raw or raw == "-":
        return None
    if metric in {"impressions", "clicks", "orders"}:
        parsed = _parse_decimal(raw)
        return int(parsed or 0) if parsed is not None else None
    if metric in {"ctr", "cvr"}:
        if raw.endswith("%"):
            parsed = _parse_decimal(raw[:-1])
            return (parsed / Decimal("100")) if parsed is not None else None
        return _parse_decimal(raw)
    return _parse_decimal(raw)


def _normalize_metric_value(metric: str, value: Any) -> Any:
    if metric in {"impressions", "clicks", "orders"}:
        if value is None:
            return 0
        return int(value)
    if metric in {"spend", "gmv", "roas", "ctr", "cpc", "cvr"}:
        return nullable_decimal(value)
    return value


def _metric_diff(lhs: Any, rhs: Any) -> Any:
    if lhs is None or rhs is None:
        return None
    if isinstance(lhs, int) and isinstance(rhs, int):
        return lhs - rhs
    left_dec = nullable_decimal(lhs)
    right_dec = nullable_decimal(rhs)
    if left_dec is None or right_dec is None:
        return None
    return left_dec - right_dec


def _metric_root_cause_guess(*, raw_value: Any, db_value: Any, rendered_value: Any) -> str:
    if rendered_value is None:
        return "rendered_value_missing_or_unparsable"
    rendered_diff = _metric_diff(rendered_value, db_value)
    if rendered_diff in {0, Decimal("0")}:
        if raw_value is None:
            return "aligned(raw_not_found)"
        raw_diff = _metric_diff(raw_value, db_value)
        if raw_diff in {0, Decimal("0")}:
            return "aligned"
        return "raw_window_or_payload_mismatch"
    return "rendering_mismatch_or_stale_report_file"


def _overall_root_cause(
    *,
    comparison: list[dict[str, Any]],
    report_path: Path,
    rendered_parse_mode: str,
) -> str:
    if not report_path.exists():
        return "rendered_report_missing"
    if rendered_parse_mode == "unparsable":
        return "rendered_report_unparsable"
    rendered_mismatch = [
        row
        for row in comparison
        if isinstance(row, dict)
        and row.get("difference", {}).get("rendered_minus_db") not in {0, "0", "0.00", None}
    ]
    if not rendered_mismatch:
        return "aligned_db_and_rendered"
    keys = ", ".join(str(row.get("metric")) for row in rendered_mismatch[:5])
    return f"rendered_vs_db_mismatch_on={keys}"


def _serialize_metric_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        quant = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        text = f"{quant:f}"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text
    return value


def _render_reconcile_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Reconcile — {payload.get('shop')} {payload.get('kind')} {payload.get('date')}",
        "",
        f"- window: {payload.get('window', {}).get('window_start')} ~ {payload.get('window', {}).get('window_end')}",
        f"- report_path: {payload.get('report_path')}",
        f"- report_exists: {payload.get('report_exists')}",
        f"- rendered_parse_mode: {payload.get('rendered_parse_mode')}",
        f"- raw_source_path: {payload.get('raw_source_path') or '-'}",
        f"- raw_source_row_count: {payload.get('raw_source_row_count')}",
        f"- scope: {payload.get('scope_line')}",
        "",
        "## Comparison",
        "| metric | raw_source_value | db_aggregated_value | rendered_value | raw_minus_db | rendered_minus_db | root_cause_guess |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload.get("comparison", []):
        if not isinstance(row, dict):
            continue
        diff = row.get("difference") if isinstance(row.get("difference"), dict) else {}
        lines.append(
            "| {metric} | {raw} | {db} | {rendered} | {raw_diff} | {rendered_diff} | {guess} |".format(
                metric=row.get("metric"),
                raw=row.get("raw_source_value"),
                db=row.get("db_aggregated_value"),
                rendered=row.get("rendered_value"),
                raw_diff=diff.get("raw_minus_db"),
                rendered_diff=diff.get("rendered_minus_db"),
                guess=row.get("root_cause_guess"),
            )
        )
    lines.extend(
        [
            "",
            f"- root_cause_summary: {payload.get('root_cause_summary')}",
            f"- fix_applied: {payload.get('fix_applied')}",
            "",
        ]
    )
    return "\n".join(lines)

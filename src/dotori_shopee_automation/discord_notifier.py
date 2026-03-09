from __future__ import annotations

import json
import os
import re
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from .config import get_settings

Channel = Literal["report", "alerts", "actions"]

CHANNEL_TAG = {
    "report": "REPORT",
    "alerts": "ALERT",
    "actions": "ACTION",
}


def _get_url(channel: Channel) -> str | None:
    settings = get_settings()
    report_url = settings.discord_webhook_report_url
    if channel == "report":
        return report_url
    if channel == "alerts":
        return settings.discord_webhook_alerts_url or report_url
    if channel == "actions":
        return settings.discord_webhook_actions_url or report_url
    return None


def _format_message(channel: Channel, text: str, shop_label: str | None) -> str:
    if shop_label:
        if channel == "report":
            return f"[{shop_label}] {text}"
        tag = CHANNEL_TAG[channel]
        return f"[{shop_label}][{tag}] {text}"
    return text


def _extract_first_url(lines: list[str]) -> str | None:
    for line in lines:
        candidate = line.strip()
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
        match = re.search(r"https?://\S+", candidate)
        if match:
            return match.group(0).rstrip(").,]")
    return None


def _trim_field_value(text: str, *, limit: int = 1024) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    if limit <= 3:
        return value[:limit]
    return value[: limit - 3].rstrip() + "..."


def _prettify_metric_csv(text: str) -> str:
    labels = {
        "spend": "Chi tiêu",
        "impressions": "Hiển thị",
        "clicks": "Clicks",
        "orders": "Đơn hàng",
        "gmv": "GMV",
        "roas": "ROAS",
        "ctr": "CTR",
        "cvr": "CVR",
        "cpc": "CPC",
    }
    rows: list[str] = []
    matches = list(re.finditer(r"([A-Za-z]+)=([^=]+?)(?=,\s*[A-Za-z]+=|$)", text))
    if matches:
        for match in matches:
            key = match.group(1).strip()
            value = match.group(2).strip().rstrip(",")
            pretty = labels.get(key.lower(), key)
            rows.append(f"{pretty}={value}")
    else:
        for part in [item.strip() for item in text.split(",") if item.strip()]:
            if "=" not in part:
                rows.append(part)
                continue
            key, value = part.split("=", 1)
            pretty = labels.get(key.strip().lower(), key.strip())
            rows.append(f"{pretty}={value.strip()}")
    if not rows:
        return text
    return "\n".join(f"• {row}" for row in rows)


def _detect_report_type(text: str) -> str:
    lower = (text or "").lower()
    if "weekly" in lower:
        return "Weekly"
    if "midday" in lower:
        return "Midday"
    if "final" in lower:
        return "Daily Final"
    if "daily" in lower:
        return "Daily"
    return "Report"


def _build_compact_report_title(shop_label: str | None, headline: str) -> str:
    normalized = (headline or "").strip()
    core = normalized.split(":", 1)[0].strip() if ":" in normalized else normalized
    report_type = _detect_report_type(core)
    week_match = re.search(r"\b(\d{4}-W\d{2})\b", core)
    date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", core)
    shop = (shop_label or "").strip()

    if week_match:
        title = f"{week_match.group(1)}_Weekly Ads report"
        if shop:
            return f"[{shop}] {title}"[:256]
        return title[:256]

    if date_match:
        date_raw = date_match.group(1)
        try:
            yy_mm_dd = datetime.strptime(date_raw, "%Y-%m-%d").strftime("%y%m%d")
        except ValueError:
            yy_mm_dd = date_raw.replace("-", "")[-6:]
        suffix = "Midday Ads report" if report_type == "Midday" else "Daily Ads report"
        title = f"{yy_mm_dd}_{suffix}"
        if shop:
            return f"[{shop}] {title}"[:256]
        return title[:256]

    if shop:
        return f"[{shop}] {core}"[:256]
    return core[:256]


def _build_report_embed(message: str) -> dict[str, Any] | None:
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return None

    report_url = _extract_first_url(lines)
    first_line = lines[0]
    shop_match = re.match(r"^\[([^\]]+)\]\s*(.*)$", first_line)
    shop_label = ""
    if shop_match:
        shop_label = shop_match.group(1).strip()
        headline = shop_match.group(2).strip()
    else:
        headline = first_line.strip()
    if " | http://" in headline:
        headline = headline.split(" | http://", 1)[0].strip()
    elif " | https://" in headline:
        headline = headline.split(" | https://", 1)[0].strip()
    title = _build_compact_report_title(shop_label, headline)
    detail_lines: list[str] = []
    for line in lines[1:]:
        if line == report_url:
            continue
        if line.lower().startswith("file cục bộ:"):
            continue
        detail_lines.append(line)

    fields: list[dict[str, Any]] = []
    summary_metric = ""
    if ":" in headline:
        summary_metric = headline.split(":", 1)[1].strip()
    if summary_metric:
        fields.append(
            {
                "name": "Tổng quan",
                "value": _trim_field_value(_prettify_metric_csv(summary_metric)),
                "inline": False,
            }
        )
    metric_line = next((line for line in detail_lines if "spend=" in line and "gmv=" in line), "")
    if metric_line:
        fields.append(
            {
                "name": "Tổng quan",
                "value": _trim_field_value(metric_line),
                "inline": False,
            }
        )
    kpi_line = next((line for line in detail_lines if line.startswith("KPI:")), "")
    if kpi_line:
        fields.append(
            {
                "name": "KPI",
                "value": _trim_field_value(kpi_line.replace("KPI:", "", 1).strip()),
                "inline": False,
            }
        )
    compare_line = next((line for line in detail_lines if line.startswith("So với tuần trước:")), "")
    if compare_line:
        fields.append(
            {
                "name": "So với tuần trước",
                "value": _trim_field_value(compare_line.replace("So với tuần trước:", "", 1).strip()),
                "inline": False,
            }
        )
    if report_url:
        fields.append(
            {
                "name": "Liên kết",
                "value": f"[Mở báo cáo]({report_url})",
                "inline": False,
            }
        )

    if not fields and detail_lines:
        fields.append(
            {
                "name": "Chi tiết",
                "value": _trim_field_value("\n".join(detail_lines)),
                "inline": False,
            }
        )

    embed: dict[str, Any] = {
        "title": title,
        "color": 0x0EA5A3,
        "fields": fields,
        "footer": {"text": "Dotori Shopee Automation"},
    }
    if report_url:
        embed["url"] = report_url
    return embed


def _detect_alert_severity(message: str) -> str:
    text = (message or "").upper()
    if "CRITICAL" in text:
        return "CRITICAL"
    if "WARN" in text or "CẢNH BÁO" in text:
        return "WARN"
    if "RESOLVED" in text or "PHỤC HỒI" in text:
        return "RESOLVED"
    return "INFO"


def _build_alert_embed(message: str) -> dict[str, Any] | None:
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return None

    first_line = lines[0]
    shop_match = re.match(r"^\[([^\]]+)\]\[ALERT\]\s*(.*)$", first_line)
    if shop_match:
        shop = shop_match.group(1).strip()
        title = shop_match.group(2).strip() or "Cảnh báo vận hành"
        header = f"[{shop}] {title}"
    else:
        title = first_line
        header = first_line

    severity = _detect_alert_severity("\n".join(lines))
    color_map = {
        "CRITICAL": 0xEF4444,
        "WARN": 0xF59E0B,
        "INFO": 0x0EA5A3,
        "RESOLVED": 0x22C55E,
    }
    color = color_map.get(severity, 0x0EA5A3)

    detail_lines = lines[1:] if len(lines) > 1 else []
    report_url = _extract_first_url(lines)
    fields: list[dict[str, Any]] = []
    if detail_lines:
        fields.append(
            {
                "name": "Chi tiết",
                "value": _trim_field_value("\n".join(detail_lines)),
                "inline": False,
            }
        )
    if report_url:
        fields.append(
            {
                "name": "Liên kết",
                "value": f"[Mở liên kết]({report_url})",
                "inline": False,
            }
        )

    embed: dict[str, Any] = {
        "title": header[:256],
        "description": f"Mức độ: {severity}",
        "color": color,
        "fields": fields,
        "footer": {"text": "Dotori Shopee Automation"},
    }
    if report_url:
        embed["url"] = report_url
    return embed


def build_report_url(relative_path: str) -> tuple[str | None, str | None]:
    settings = get_settings()
    base_raw = (settings.report_base_url or "").strip()
    relative_raw = (relative_path or "").strip()
    if not base_raw or not relative_raw:
        return None, None

    relative_clean = relative_raw.replace("\\", "/").lstrip("/")
    if not relative_clean:
        return None, None

    parsed = urlsplit(base_raw)
    base_path = parsed.path.rstrip("/")
    base_parts = [part for part in base_path.split("/") if part]
    relative_parts = [part for part in relative_clean.split("/") if part]
    if base_parts and relative_parts and base_parts[-1] == relative_parts[0]:
        relative_parts = relative_parts[1:]
    relative_joined = "/".join(relative_parts)
    if base_path and relative_joined:
        full_path = f"{base_path}/{relative_joined}"
    elif base_path:
        full_path = base_path
    else:
        full_path = f"/{relative_joined}" if relative_joined else "/"

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    token = (settings.report_access_token or "").strip()
    if token:
        query_pairs.append(("token", token))

    query = urlencode(query_pairs, doseq=True)
    masked_pairs = [(key, "***" if key == "token" else value) for key, value in query_pairs]
    query_masked = urlencode(masked_pairs, doseq=True, safe="*")

    url = urlunsplit((parsed.scheme, parsed.netloc, full_path, query, parsed.fragment))
    url_for_log = urlunsplit(
        (parsed.scheme, parsed.netloc, full_path, query_masked, parsed.fragment)
    )
    return url, url_for_log


def send(
    channel: Channel,
    text: str,
    shop_label: str | None = None,
    webhook_url: str | None = None,
    attachment_path: str | Path | None = None,
    attachment_filename: str | None = None,
    zip_attachment_path: str | Path | None = None,
    zip_attachment_filename: str | None = None,
    md_attachment_path: str | Path | None = None,
    md_attachment_filename: str | None = None,
) -> None:
    url = webhook_url or _get_url(channel)
    message = _format_message(channel, text, shop_label)
    dry_run = os.environ.get("DISCORD_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    outbox_path = os.environ.get("DISCORD_OUTBOX_PATH", "").strip()
    attachment_file: Path | None = None
    attach_name = ""
    attach_size = 0
    zip_file: Path | None = None
    zip_name = ""
    zip_size = 0
    md_file: Path | None = None
    md_name = ""
    md_size = 0
    if attachment_path:
        candidate = Path(attachment_path)
        if candidate.exists() and candidate.is_file():
            attachment_file = candidate
            attach_name = attachment_filename or candidate.name
            attach_size = candidate.stat().st_size
            print(
                f"report_attach_planned=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={attach_name} size={attach_size}"
            )
        else:
            print(
                f"report_attach_skipped=1 reason=file_missing channel={channel} "
                f"shop_label={shop_label or '-'} path={candidate}"
            )
    if zip_attachment_path:
        candidate_zip = Path(zip_attachment_path)
        if candidate_zip.exists() and candidate_zip.is_file():
            zip_file = candidate_zip
            zip_name = zip_attachment_filename or candidate_zip.name
            zip_size = candidate_zip.stat().st_size
            print(
                f"report_zip_attach_planned=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={zip_name} size={zip_size}"
            )
        else:
            print(
                f"report_zip_attach_skipped=1 reason=file_missing channel={channel} "
                f"shop_label={shop_label or '-'} path={candidate_zip}"
            )
    if md_attachment_path:
        candidate_md = Path(md_attachment_path)
        if candidate_md.exists() and candidate_md.is_file():
            md_file = candidate_md
            md_name = md_attachment_filename or candidate_md.name
            md_size = candidate_md.stat().st_size
            print(
                f"report_md_attach_planned=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={md_name} size={md_size}"
            )
        else:
            print(
                f"report_md_attach_skipped=1 reason=file_missing channel={channel} "
                f"shop_label={shop_label or '-'} path={candidate_md}"
            )
    if dry_run:
        if outbox_path:
            path = Path(outbox_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            safe_line = message.replace("\n", "\\n")
            with path.open("a", encoding="utf-8") as f:
                f.write(safe_line + "\n")
        print(f"discord_dry_run=1 channel={channel} shop_label={shop_label or '-'}")
        return
    if not url:
        print(f"Discord webhook URL not set for channel '{channel}'. Skipping.")
        print(
            f"discord_send_ok=0 channel={channel} shop_label={shop_label or '-'} "
            "http_status=- reason=webhook_missing"
        )
        return

    try:
        use_report_embed = (
            channel == "report"
            and os.environ.get("DISCORD_REPORT_EMBED_DISABLED", "").strip().lower()
            not in {"1", "true", "yes"}
        )
        use_alert_embed = (
            channel in {"alerts", "actions"}
            and os.environ.get("DISCORD_ALERTS_EMBED_DISABLED", "").strip().lower()
            not in {"1", "true", "yes"}
        )
        report_embed = _build_report_embed(message) if use_report_embed else None
        alert_embed = _build_alert_embed(message) if use_alert_embed else None
        payload_content = message
        if report_embed is not None or alert_embed is not None:
            payload_content = ""
        payload: dict[str, Any] = {"content": payload_content}
        if report_embed is not None:
            payload["embeds"] = [report_embed]
        elif alert_embed is not None:
            payload["embeds"] = [alert_embed]
        sent_html = False
        sent_zip = False
        sent_md = False
        if attachment_file is None and zip_file is None and md_file is None:
            response = httpx.post(url, json=payload, timeout=10)
        else:
            payload_json = json.dumps(payload, ensure_ascii=False)
            files_payload: dict[str, tuple[str, object, str]] = {}
            with ExitStack() as stack:
                index = 0
                if attachment_file is not None:
                    try:
                        fp_html = stack.enter_context(attachment_file.open("rb"))
                    except OSError:
                        print(
                            f"report_attach_skipped=1 reason=open_failed channel={channel} "
                            f"shop_label={shop_label or '-'} file={attach_name}"
                        )
                    else:
                        files_payload[f"files[{index}]"] = (
                            attach_name,
                            fp_html,
                            "text/html; charset=utf-8",
                        )
                        sent_html = True
                        index += 1
                if zip_file is not None:
                    try:
                        fp_zip = stack.enter_context(zip_file.open("rb"))
                    except OSError:
                        print(
                            f"report_zip_attach_skipped=1 reason=open_failed channel={channel} "
                            f"shop_label={shop_label or '-'} file={zip_name}"
                        )
                    else:
                        files_payload[f"files[{index}]"] = (
                            zip_name,
                            fp_zip,
                            "application/zip",
                        )
                        sent_zip = True
                        index += 1
                if md_file is not None:
                    try:
                        fp_md = stack.enter_context(md_file.open("rb"))
                    except OSError:
                        print(
                            f"report_md_attach_skipped=1 reason=open_failed channel={channel} "
                            f"shop_label={shop_label or '-'} file={md_name}"
                        )
                    else:
                        files_payload[f"files[{index}]"] = (
                            md_name,
                            fp_md,
                            "text/markdown; charset=utf-8",
                        )
                        sent_md = True
                if files_payload:
                    response = httpx.post(
                        url,
                        data={"payload_json": payload_json},
                        files=files_payload,
                        timeout=20,
                    )
                else:
                    response = httpx.post(url, json=payload, timeout=10)
        if response.status_code >= 400:
            print(
                f"discord_send_ok=0 channel={channel} shop_label={shop_label or '-'} "
                f"http_status={response.status_code}"
            )
            if sent_html:
                print(
                    f"report_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                    f"file={attach_name} size={attach_size} http_status={response.status_code}"
                )
            if sent_zip:
                print(
                    f"report_zip_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                    f"file={zip_name} size={zip_size} http_status={response.status_code}"
                )
            if sent_md:
                print(
                    f"report_md_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                    f"file={md_name} size={md_size} http_status={response.status_code}"
                )
            print(f"Discord webhook returned {response.status_code}: {response.text}")
            return
        print(
            f"discord_send_ok=1 channel={channel} shop_label={shop_label or '-'} "
            f"http_status={response.status_code}"
        )
        if sent_html:
            print(
                f"report_attach_sent=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={attach_name} size={attach_size} http_status={response.status_code}"
            )
        if sent_zip:
            print(
                f"report_zip_attach_sent=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={zip_name} size={zip_size} http_status={response.status_code}"
            )
        if sent_md:
            print(
                f"report_md_attach_sent=1 channel={channel} shop_label={shop_label or '-'} "
                f"file={md_name} size={md_size} http_status={response.status_code}"
            )
    except httpx.HTTPError as exc:
        print(
            f"discord_send_ok=0 channel={channel} shop_label={shop_label or '-'} "
            "http_status=- reason=http_error"
        )
        if attachment_file is not None:
            print(
                f"report_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                f"file={attach_name} size={attach_size} http_status=- reason=http_error"
            )
        if zip_file is not None:
            print(
                f"report_zip_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                f"file={zip_name} size={zip_size} http_status=- reason=http_error"
            )
        if md_file is not None:
            print(
                f"report_md_attach_sent=0 channel={channel} shop_label={shop_label or '-'} "
                f"file={md_name} size={md_size} http_status=- reason=http_error"
            )
        print(f"Discord webhook request failed: {exc}")

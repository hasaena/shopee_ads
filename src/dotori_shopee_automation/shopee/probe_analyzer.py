from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import json
import re
from typing import Any, Iterable


SENSITIVE_TOKENS = [
    "token",
    "sign",
    "authorization",
    "cookie",
    "secret",
    "client_secret",
    "partner_key",
    "access_token",
    "refresh_token",
]


@dataclass(frozen=True)
class ProbeRecord:
    shop_key: str
    call_name: str
    ok: bool
    status_code: int | None
    shopee_error: str | None
    message: str
    path: str | None
    saved_file: str
    top_keys: list[str]
    response_keys: list[str]


def analyze_artifacts(
    save_root: Path,
    date_value: str,
    only_shops: Iterable[str] | None = None,
) -> list[ProbeRecord]:
    records: list[ProbeRecord] = []
    shops = _resolve_shops(save_root, only_shops)
    for shop_key in shops:
        folder = save_root / shop_key / date_value
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json")):
            records.append(_parse_artifact(path, shop_key, save_root))
    return records


def write_markdown_summary(
    records: list[ProbeRecord],
    output_path: Path,
    *,
    date_value: str,
    save_root: Path,
    include_schema_hints: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"# Probe Summary — {date_value}")
    lines.append("")
    lines.append(f"- save_root: {save_root}")
    lines.append("")

    sorted_records = _sort_records(records)
    by_shop = _group_by_shop(sorted_records)
    for shop_key, items in by_shop.items():
        lines.append(f"## {shop_key}")
        lines.append("")
        lines.append(
            "| shop_key | call_name | ok | status_code | shopee_error | message | path | saved_file |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for record in items:
            ok_mark = "✅" if record.ok else "❌"
            lines.append(
                "| "
                + " | ".join(
                    [
                        record.shop_key,
                        record.call_name,
                        ok_mark,
                        _fmt_status(record.status_code),
                        record.shopee_error or "-",
                        _short_text(record.message),
                        record.path or "-",
                        record.saved_file,
                    ]
                )
                + " |"
            )
        lines.append("")
        lines.append("### Next actions")
        ok_calls, denied_calls, param_calls, other_calls = _categorize_calls(items)
        lines.append(f"- ✅ OK calls: {_fmt_call_list(ok_calls)}")
        lines.append(f"- ⛔ denied calls: {_fmt_call_list(denied_calls)}")
        lines.append(f"- 🧩 param/format: {_fmt_call_list(param_calls)}")
        if other_calls:
            lines.append(f"- ⚠️ other failures: {_fmt_call_list(other_calls)}")
        lines.append("")

        hints = _schema_hints(items) if include_schema_hints else []
        if include_schema_hints and hints:
            lines.append("### Schema hints")
            for line in hints:
                lines.append(f"- {line}")
            lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_csv_summary(records: list[ProbeRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "shop_key",
                "call_name",
                "ok",
                "status_code",
                "shopee_error",
                "message",
                "path",
                "saved_file",
            ]
        )
        for record in _sort_records(records):
            writer.writerow(
                [
                    record.shop_key,
                    record.call_name,
                    "1" if record.ok else "0",
                    _fmt_status(record.status_code),
                    record.shopee_error or "",
                    _short_text(record.message),
                    record.path or "",
                    record.saved_file,
                ]
            )


def render_console_list(records: list[ProbeRecord]) -> list[str]:
    lines = []
    header = "shop_key\tcall_name\tok\tstatus\tshopee_error\tmessage\tpath\tsaved_file"
    lines.append(header)
    for record in _sort_records(records):
        lines.append(
            "\t".join(
                [
                    record.shop_key,
                    record.call_name,
                    "1" if record.ok else "0",
                    _fmt_status(record.status_code),
                    record.shopee_error or "-",
                    _short_text(record.message),
                    record.path or "-",
                    record.saved_file,
                ]
            )
        )
    return lines


def build_discord_summary(
    records: list[ProbeRecord],
    date_value: str,
    save_root: Path,
    shops: list[str] | None,
    summary_ref: str,
) -> str:
    sorted_records = _sort_records(records)
    shop_list = shops or sorted({record.shop_key for record in sorted_records})
    shop_label = ",".join(shop_list) if shop_list else "-"
    total = len(sorted_records)
    ok_count = sum(1 for record in sorted_records if record.ok)
    fail_count = total - ok_count

    lines: list[str] = []
    lines.append(f"[PROBE] {date_value} save_root={save_root} shops={shop_label}")
    lines.append(f"totals: {total} / ok {ok_count} / fail {fail_count}")

    grouped = _group_by_shop(sorted_records)
    for shop_key in shop_list:
        items = grouped.get(shop_key, [])
        ok_calls, denied_calls, param_calls, other_calls = _categorize_calls(items)
        ok_text = _fmt_call_list(ok_calls, max_items=10)
        denied_text = _fmt_call_list(denied_calls, max_items=10)

        denied_set = set(denied_calls)
        fail_items = [
            record
            for record in items
            if not record.ok and record.call_name not in denied_set
        ]
        fail_parts: list[str] = []
        for record in fail_items[:5]:
            msg = _short_text(record.message, limit=60)
            msg = _scrub_text(msg)
            fail_parts.append(f"{record.call_name}({msg})")
        fail_text = ", ".join(fail_parts) if fail_parts else "-"

        lines.append(f"{shop_key} OK: {ok_text}")
        lines.append(f"{shop_key} DENIED: {denied_text}")
        lines.append(f"{shop_key} FAIL: {fail_text}")

    lines.append(f"summary: {summary_ref}")
    return _scrub_text("\n".join(lines))


def _resolve_shops(save_root: Path, only_shops: Iterable[str] | None) -> list[str]:
    if only_shops:
        return [item for item in only_shops if item]
    if not save_root.exists():
        return []
    return sorted([path.name for path in save_root.iterdir() if path.is_dir()])


def _parse_artifact(path: Path, shop_key: str, save_root: Path) -> ProbeRecord:
    call_name = _call_name_from_filename(path.name)
    payload: dict[str, Any] | None = None
    error_message = ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        error_message = f"invalid_json: {exc}"

    meta = payload.get("__meta") if isinstance(payload, dict) else {}
    if isinstance(meta, dict):
        call_name = meta.get("call_name") or call_name
    call_name = call_name or "unknown"

    api_path = meta.get("path") if isinstance(meta, dict) else None
    status_code = _parse_status(meta if isinstance(meta, dict) else {})

    shopee_error_value = None
    if isinstance(meta, dict) and meta.get("shopee_error") is not None:
        shopee_error_value = meta.get("shopee_error")
    elif isinstance(payload, dict) and payload.get("error") is not None:
        shopee_error_value = payload.get("error")

    error_str, error_numeric = _normalize_error(shopee_error_value)
    ok = _is_ok(status_code, error_str, error_numeric, payload)

    message = _extract_message(payload, meta, error_message)
    message = _scrub_text(message)

    top_keys, response_keys = _extract_schema_keys(payload)

    saved_file = str(path.relative_to(save_root))
    shopee_error = None
    if error_numeric is not None:
        shopee_error = str(error_numeric)
    elif error_str:
        shopee_error = _scrub_text(error_str)

    return ProbeRecord(
        shop_key=shop_key,
        call_name=str(call_name),
        ok=ok,
        status_code=status_code,
        shopee_error=shopee_error,
        message=message,
        path=api_path if api_path else None,
        saved_file=saved_file,
        top_keys=top_keys,
        response_keys=response_keys,
    )


def _call_name_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    if not stem:
        return "unknown"
    match = re.match(r"^\d+_(.+)$", stem)
    if not match:
        return "unknown"
    rest = match.group(1)
    if "_api_" in rest:
        call_name = rest.split("_api_", 1)[0]
        return call_name or "unknown"
    if "_" in rest:
        return rest.split("_", 1)[0]
    return rest or "unknown"


def _parse_status(meta: dict[str, Any]) -> int | None:
    raw = meta.get("http_status") or meta.get("status_code")
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def _normalize_error(value: Any) -> tuple[str | None, int | None]:
    if value is None:
        return None, None
    if isinstance(value, bool):
        return str(int(value)), int(value)
    if isinstance(value, int):
        return str(value), value
    if isinstance(value, str):
        if value.isdigit():
            return value, int(value)
        return value, None
    return str(value), None


def _is_ok(
    status_code: int | None,
    error_str: str | None,
    error_numeric: int | None,
    payload: dict[str, Any] | None,
) -> bool:
    if error_str and error_numeric is None:
        return False
    if error_numeric not in (None, 0):
        return False
    if status_code is None:
        return True
    return status_code == 200


def _extract_message(
    payload: dict[str, Any] | None,
    meta: dict[str, Any] | None,
    fallback: str,
) -> str:
    if meta and isinstance(meta, dict):
        meta_error = meta.get("error")
        if meta_error:
            return str(meta_error)
    if isinstance(payload, dict):
        if isinstance(payload.get("message"), str):
            return payload.get("message", "")
        if isinstance(payload.get("msg"), str):
            return payload.get("msg", "")
        if isinstance(payload.get("error"), str):
            return payload.get("error", "")
    return fallback or "-"


def _extract_schema_keys(payload: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    if not isinstance(payload, dict):
        return [], []
    top_keys = sorted([key for key in payload.keys() if key != "__meta"])
    response_keys: list[str] = []
    response = payload.get("response")
    if isinstance(response, dict):
        response_keys = sorted(response.keys())
    return top_keys, response_keys


def _short_text(value: str, limit: int = 120) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text or "-"
    return text[: limit - 3] + "..."


def _fmt_status(status_code: int | None) -> str:
    return str(status_code) if status_code is not None else "-"


def _group_by_shop(records: list[ProbeRecord]) -> dict[str, list[ProbeRecord]]:
    grouped: dict[str, list[ProbeRecord]] = {}
    for record in records:
        grouped.setdefault(record.shop_key, []).append(record)
    return grouped


def _categorize_calls(
    records: list[ProbeRecord],
) -> tuple[list[str], list[str], list[str], list[str]]:
    ok_calls: list[str] = []
    denied_calls: list[str] = []
    param_calls: list[str] = []
    other_calls: list[str] = []
    for record in records:
        if record.ok:
            ok_calls.append(record.call_name)
            continue
        message = record.message.lower()
        if any(term in message for term in ["denied", "permission", "unauthorized"]):
            denied_calls.append(record.call_name)
            continue
        if record.shopee_error == "1" and any(
            term in message for term in ["denied", "permission", "unauthorized"]
        ):
            denied_calls.append(record.call_name)
            continue
        if any(
            term in message
            for term in ["param", "invalid", "missing", "format", "required"]
        ):
            param_calls.append(record.call_name)
            continue
        other_calls.append(record.call_name)
    return ok_calls, denied_calls, param_calls, other_calls


def _fmt_call_list(values: list[str], max_items: int = 20) -> str:
    if not values:
        return "-"
    shown = values[:max_items]
    return ", ".join(shown)


def _schema_hints(records: list[ProbeRecord]) -> list[str]:
    hints: list[str] = []
    for record in records:
        if not record.ok:
            continue
        top_keys = ",".join(record.top_keys) if record.top_keys else "-"
        response_keys = ",".join(record.response_keys) if record.response_keys else "-"
        hints.append(
            f"{record.call_name}: top=[{top_keys}] response=[{response_keys}]"
        )
    return hints


def _scrub_text(text: str) -> str:
    cleaned = text
    for key in SENSITIVE_TOKENS:
        pattern = re.compile(rf"({re.escape(key)}\s*[:=]\s*)([^\s,;]+)", re.I)
        cleaned = pattern.sub(r"\1***", cleaned)
    return cleaned


def _sort_records(records: list[ProbeRecord]) -> list[ProbeRecord]:
    return sorted(records, key=lambda item: (item.shop_key, item.call_name, item.saved_file))

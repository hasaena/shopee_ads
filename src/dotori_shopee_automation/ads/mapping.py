from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class FieldMapping:
    path: str
    cast: str
    default: Any


@dataclass(frozen=True)
class CallMapping:
    name: str
    type: str
    campaign_id_path: str | None
    campaign_name_path: str | None
    records_path: str | None
    fields: dict[str, FieldMapping]
    timestamp_path: str | None
    date_path: str | None
    status_path: str | None
    daily_budget_path: str | None
    notes: str | None


@dataclass(frozen=True)
class MappingConfig:
    version: int
    calls: dict[str, CallMapping]


def coverage_for_plan(mapping: MappingConfig, plan_call_names: list[str]) -> tuple[int, list[str]]:
    unmapped: list[str] = []
    mapped = 0
    seen: set[str] = set()
    for name in plan_call_names:
        if name in seen:
            continue
        seen.add(name)
        if name in mapping.calls:
            mapped += 1
        else:
            unmapped.append(name)
    return mapped, unmapped


def load_mapping(path: Path) -> MappingConfig:
    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Mapping file must be a YAML mapping")
    version = data.get("version")
    if version != 1:
        raise ValueError("Mapping version must be 1")
    calls_raw = data.get("calls")
    if not isinstance(calls_raw, dict) or not calls_raw:
        raise ValueError("Mapping calls must be a non-empty mapping")

    calls: dict[str, CallMapping] = {}
    for call_name, call_raw in calls_raw.items():
        if not isinstance(call_name, str) or not call_name.strip():
            raise ValueError("Mapping call name must be a non-empty string")
        if not isinstance(call_raw, dict):
            raise ValueError(f"Mapping for {call_name} must be a mapping")
        mapping_type = call_raw.get("type")
        if mapping_type not in {"daily", "snapshot", "campaign", "meta"}:
            raise ValueError(
                f"Mapping type for {call_name} must be daily, snapshot, campaign, or meta"
            )
        if mapping_type == "meta":
            calls[call_name] = CallMapping(
                name=call_name,
                type=mapping_type,
                campaign_id_path=None,
                campaign_name_path=None,
                records_path=None,
                fields={},
                timestamp_path=None,
                date_path=None,
                status_path=None,
                daily_budget_path=None,
                notes=_optional(call_raw, "notes"),
            )
            continue

        if mapping_type == "campaign":
            campaign_id_path = _require(call_raw, "campaign_id_path")
            campaign_name_path = _require(call_raw, "campaign_name_path")
            records_path = call_raw.get("records_path")
            if records_path is not None and not isinstance(records_path, str):
                raise ValueError(f"records_path for {call_name} must be string or null")
            fields_raw = call_raw.get("fields") or {}
            if not isinstance(fields_raw, dict):
                raise ValueError(f"fields for {call_name} must be a mapping")
            # Campaign-only mappings may omit metric fields entirely.
            fields: dict[str, FieldMapping] = {}
            for field_name, field_raw in fields_raw.items():
                if not isinstance(field_raw, dict):
                    raise ValueError(f"field {field_name} for {call_name} must be a mapping")
                fields[field_name] = FieldMapping(
                    path=_require(field_raw, "path"),
                    cast=str(field_raw.get("cast") or "int"),
                    default=field_raw.get("default", 0),
                )
            calls[call_name] = CallMapping(
                name=call_name,
                type=mapping_type,
                campaign_id_path=campaign_id_path,
                campaign_name_path=campaign_name_path,
                records_path=records_path,
                fields=fields,
                timestamp_path=None,
                date_path=None,
                status_path=_optional(call_raw, "status_path"),
                daily_budget_path=_optional(call_raw, "daily_budget_path"),
                notes=_optional(call_raw, "notes"),
            )
            continue

        campaign_id_path = _require(call_raw, "campaign_id_path")
        campaign_name_path = _require(call_raw, "campaign_name_path")
        records_path = call_raw.get("records_path")
        if records_path is not None and not isinstance(records_path, str):
            raise ValueError(f"records_path for {call_name} must be string or null")
        fields_raw = call_raw.get("fields")
        if not isinstance(fields_raw, dict) or not fields_raw:
            raise ValueError(f"fields for {call_name} must be a mapping")
        fields: dict[str, FieldMapping] = {}
        for field_name, field_raw in fields_raw.items():
            if not isinstance(field_raw, dict):
                raise ValueError(f"field {field_name} for {call_name} must be a mapping")
            fields[field_name] = FieldMapping(
                path=_require(field_raw, "path"),
                cast=str(field_raw.get("cast") or "int"),
                default=field_raw.get("default", 0),
            )
        calls[call_name] = CallMapping(
            name=call_name,
            type=mapping_type,
            campaign_id_path=campaign_id_path,
            campaign_name_path=campaign_name_path,
            records_path=records_path,
            fields=fields,
            timestamp_path=_optional(call_raw, "timestamp_path"),
            date_path=_optional(call_raw, "date_path"),
            status_path=_optional(call_raw, "status_path"),
            daily_budget_path=_optional(call_raw, "daily_budget_path"),
            notes=_optional(call_raw, "notes"),
        )

    return MappingConfig(version=version, calls=calls)


def extract_path(obj: Any, path: str | None) -> Any:
    if path is None:
        return None
    if path == "":
        return obj
    current = obj
    for part in path.split("."):
        if current is None:
            return None
        if isinstance(current, list):
            if not part.isdigit():
                return None
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue
        if isinstance(current, dict):
            current = current.get(part)
            continue
        return None
    return current


def cast_value(value: Any, cast: str, default: Any) -> Any:
    if value is None or value == "":
        return default
    try:
        if cast == "int":
            return int(value)
        if cast == "decimal":
            return Decimal(str(value))
        if cast == "str":
            return str(value)
    except (ValueError, TypeError):
        return default
    return value


def parse_date_value(value: Any, fallback: date) -> date:
    if value is None or value == "":
        return fallback
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return fallback
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
        for fmt in ("%d-%m-%Y", "%Y%m%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        return fallback
    return fallback


def parse_datetime_value(value: Any, fallback: datetime) -> datetime:
    if value is None or value == "":
        return fallback
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return fallback
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return fallback


def _require(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing required mapping key: {key}")
    return value


def _optional(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    return value or None

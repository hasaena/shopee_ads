from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import os
from pathlib import Path
import re
from typing import Any

import yaml


@dataclass(frozen=True)
class PlanCall:
    name: str
    method: str
    path: str
    params: dict[str, Any]
    body: dict[str, Any] | None
    save: bool


@dataclass(frozen=True)
class PlanDefinition:
    version: int
    name: str
    defaults: dict[str, Any]
    calls: list[PlanCall]


_VAR_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")


def load_plan(path: Path) -> PlanDefinition:
    if not path.exists():
        raise FileNotFoundError(f"Plan file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Plan file must be a YAML mapping")
    version = data.get("version")
    if version != 1:
        raise ValueError("Plan version must be 1")
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Plan name is required")

    defaults = data.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError("Plan defaults must be a mapping")

    calls_raw = data.get("calls")
    if not isinstance(calls_raw, list) or not calls_raw:
        raise ValueError("Plan calls must be a non-empty list")

    calls: list[PlanCall] = []
    for item in calls_raw:
        if not isinstance(item, dict):
            raise ValueError("Each call must be a mapping")
        call_name = item.get("name")
        if not isinstance(call_name, str) or not call_name.strip():
            raise ValueError("Call name is required")
        path_value = item.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            raise ValueError(f"Call '{call_name}' missing path")
        method = item.get("method") or defaults.get("method") or "GET"
        method = str(method).upper()
        if method not in {"GET", "POST"}:
            raise ValueError(f"Call '{call_name}' has invalid method '{method}'")
        params = item.get("params") or {}
        if not isinstance(params, dict):
            raise ValueError(f"Call '{call_name}' params must be a mapping")
        body = item.get("body")
        if body is not None and not isinstance(body, dict):
            raise ValueError(f"Call '{call_name}' body must be a mapping")
        save = item.get("save")
        if save is None:
            save = defaults.get("save", True)
        if not isinstance(save, bool):
            raise ValueError(f"Call '{call_name}' save must be boolean")
        calls.append(
            PlanCall(
                name=call_name,
                method=method,
                path=path_value,
                params=params,
                body=body,
                save=save,
            )
        )

    return PlanDefinition(version=version, name=name, defaults=defaults, calls=calls)


def build_builtin_vars(shop_key: str, shop_id: int) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    ads_daily_path = os.environ.get("ADS_DAILY_PATH") or "/api/v2/marketing/TODO_REPLACE_ME"
    ads_snapshot_path = os.environ.get("ADS_SNAPSHOT_PATH") or "/api/v2/marketing/TODO_REPLACE_ME"
    ads_campaign_list_path = (
        os.environ.get("ADS_CAMPAIGN_LIST_PATH") or "/api/v2/marketing/TODO_REPLACE_ME"
    )
    return {
        "shop_key": shop_key,
        "shop_id": str(shop_id),
        "now_iso": now.isoformat(),
        "today": date.today().isoformat(),
        "ads_daily_path": ads_daily_path,
        "ads_snapshot_path": ads_snapshot_path,
        "ads_campaign_list_path": ads_campaign_list_path,
    }


def interpolate_data(value: Any, variables: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: interpolate_data(val, variables) for key, val in value.items()}
    if isinstance(value, list):
        return [interpolate_data(item, variables) for item in value]
    if isinstance(value, str):
        return _interpolate_string(value, variables)
    return value


def safe_path(api_path: str) -> str:
    sanitized = api_path.strip("/").replace("/", "_")
    return sanitized or "root"


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned or "call"


def build_artifact_path(
    save_root: Path,
    shop_key: str,
    call_name: str,
    api_path: str,
    requested_at: datetime,
) -> Path:
    date_folder = requested_at.strftime("%Y%m%d")
    ts_ms = int(requested_at.timestamp() * 1000)
    safe_call = safe_name(call_name)
    safe_api = safe_path(api_path)
    return save_root / shop_key / date_folder / f"{ts_ms}_{safe_call}_{safe_api}.json"


def _interpolate_string(value: str, variables: dict[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in variables:
            raise ValueError(f"Missing template variable '{key}'")
        return str(variables[key])

    return _VAR_PATTERN.sub(replace, value)

from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
from typing import Any
from datetime import timedelta, timezone, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    env: str = "local"
    timezone: str = "Asia/Ho_Chi_Minh"
    database_url: str = "sqlite:///./dotori.db"

    discord_webhook_report_url: str | None = None
    discord_webhook_alerts_url: str | None = None
    discord_webhook_actions_url: str | None = None

    shops_config_path: str = "./config/shops.yaml"

    report_base_url: str | None = None
    report_access_token: str | None = None
    reports_token: str | None = None
    ops_token: str | None = None
    dotori_ops_token: str | None = None
    reports_dir: str = "./reports"

    alert_cooldown_minutes: int = 120

    scheduler_enabled: bool = False
    scheduler_timezone: str | None = None
    detect_interval_minutes: int = 15
    daily_final_time: str = "00:00"
    daily_midday_time: str = "13:00"
    weekly_report_dow: str = "MON"
    weekly_report_time: str = "09:00"
    scheduler_send_discord: bool = True

    shopee_partner_id: int | None = None
    shopee_partner_key: str | None = None
    shopee_api_host: str = "https://partner.shopeemobile.com"
    shopee_redirect_url: str | None = None
    shopee_samord_shop_id: int | None = None
    shopee_minmin_shop_id: int | None = None

    web_host: str = "127.0.0.1"
    web_port: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    env_file = None if os.environ.get("PYTEST_CURRENT_TEST") else ".env"
    settings = Settings(_env_file=env_file)
    resolved_db = _resolve_sqlite_url(settings.database_url)
    resolved_reports = _resolve_path(settings.reports_dir)
    resolved_shops = _resolve_path(settings.shops_config_path)
    return settings.model_copy(
        update={
            "database_url": resolved_db,
            "reports_dir": str(resolved_reports),
            "shops_config_path": str(resolved_shops),
        }
    )


def _resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_absolute():
        return (PROJECT_ROOT / path).resolve()
    return path


def _resolve_sqlite_url(database_url: str) -> str:
    if not database_url.startswith("sqlite:///"):
        return database_url
    path = database_url[len("sqlite:///") :]
    if path in {":memory:", ""}:
        return database_url
    if path.startswith("/") or (len(path) >= 3 and path[1] == ":" and path[2] in {"/", "\\"}):
        return database_url
    abs_path = (PROJECT_ROOT / Path(path)).resolve()
    return f"sqlite:///{abs_path.as_posix()}"


def resolve_timezone(tz_name: str) -> tzinfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        if tz_name == "Asia/Ho_Chi_Minh":
            return timezone(timedelta(hours=7))
        return timezone.utc


class ShopConfig(BaseModel):
    shop_key: str
    label: str
    enabled: bool = True
    timezone: str | None = None
    discord_webhook_url: str | None = None
    targets: dict[str, Any] | None = None
    shopee_shop_id: int | None = None
    daily_budget_est: Decimal | None = None


class ShopsConfigFile(BaseModel):
    shops: list[ShopConfig]


def _normalize_shops_raw(raw: object) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        if "shops" in raw:
            raw = raw["shops"]
        else:
            raise ValueError("Shops config must be a YAML list or a dict with 'shops'.")
    if not isinstance(raw, list):
        raise ValueError("Shops config must be a YAML list.")
    return raw


def load_shops() -> list[ShopConfig]:
    settings = get_settings()
    path = Path(settings.shops_config_path)
    if not path.exists():
        raise FileNotFoundError(f"Shops config not found at '{path}'.")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in '{path}': {exc}") from exc

    raw_list = _normalize_shops_raw(raw)
    try:
        shops = [ShopConfig.model_validate(item) for item in raw_list]
    except ValidationError as exc:
        raise ValueError(f"Invalid shop config in '{path}': {exc}") from exc

    seen: set[str] = set()
    duplicates: set[str] = set()
    for shop in shops:
        if shop.shop_key in seen:
            duplicates.add(shop.shop_key)
        seen.add(shop.shop_key)
    if duplicates:
        raise ValueError(f"Duplicate shop_key(s): {', '.join(sorted(duplicates))}")

    default_tz = settings.timezone
    return [
        shop.model_copy(update={"timezone": shop.timezone or default_tz}) for shop in shops
    ]

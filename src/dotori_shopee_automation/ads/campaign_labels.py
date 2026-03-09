from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_PRODUCT_ALIAS_PATH = Path("collaboration/mappings/campaign_product_aliases.yaml")


def clear_campaign_product_alias_cache() -> None:
    _load_campaign_product_aliases.cache_clear()


def resolve_campaign_display_name(
    *,
    shop_key: str | None,
    campaign_id: object,
    campaign_name: object | None,
) -> str:
    normalized_id = _normalize_campaign_id(campaign_id)
    if normalized_id == "SHOP_TOTAL":
        alias = _lookup_campaign_alias(shop_key=shop_key, campaign_id=normalized_id)
        if alias:
            return f"{alias} (SHOP_TOTAL)"
        return "SHOP_TOTAL"

    alias = _lookup_campaign_alias(shop_key=shop_key, campaign_id=normalized_id)
    name = _normalize_text(campaign_name)
    if alias:
        if name and name != normalized_id and alias.lower() not in name.lower():
            return f"{alias} / {name}"
        return alias

    if name and name != normalized_id:
        return name
    return normalized_id


def _lookup_campaign_alias(*, shop_key: str | None, campaign_id: str) -> str | None:
    normalized_shop = _normalize_shop_key(shop_key)
    alias_map = _load_campaign_product_aliases()
    if not normalized_shop:
        return None
    shop_aliases = alias_map.get(normalized_shop) or {}
    value = shop_aliases.get(campaign_id)
    if value:
        return value
    return None


@lru_cache(maxsize=1)
def _load_campaign_product_aliases() -> dict[str, dict[str, str]]:
    path = _resolve_campaign_alias_path()
    if path is None or not path.exists():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001
        return {}
    shops_raw = raw.get("shops") if isinstance(raw, dict) else {}
    if not isinstance(shops_raw, dict):
        return {}

    out: dict[str, dict[str, str]] = {}
    for shop_key, value in shops_raw.items():
        normalized_shop = _normalize_shop_key(shop_key)
        if not normalized_shop:
            continue
        parsed = _parse_shop_aliases(value)
        if parsed:
            out[normalized_shop] = parsed
    return out


def _parse_shop_aliases(value: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(value, dict):
        for raw_campaign_id, raw_alias in value.items():
            campaign_id = _normalize_campaign_id(raw_campaign_id)
            alias = _normalize_text(raw_alias)
            if campaign_id and alias:
                out[campaign_id] = alias
        return out
    if isinstance(value, list):
        for row in value:
            if not isinstance(row, dict):
                continue
            raw_campaign_id = row.get("campaign_id") or row.get("id") or row.get("campaign")
            if raw_campaign_id is None:
                continue
            campaign_id = _normalize_campaign_id(raw_campaign_id)
            alias = _normalize_text(
                row.get("product_name") or row.get("alias") or row.get("label")
            )
            if campaign_id and alias:
                out[campaign_id] = alias
    return out


def _resolve_campaign_alias_path() -> Path | None:
    raw = os.environ.get("CAMPAIGN_PRODUCT_MAP_PATH", "").strip()
    if not raw:
        raw = str(DEFAULT_PRODUCT_ALIAS_PATH)
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def _normalize_campaign_id(value: object) -> str:
    text = _normalize_text(value)
    if not text:
        return "SHOP_TOTAL"
    if text.upper() == "SHOP_TOTAL":
        return "SHOP_TOTAL"
    return text


def _normalize_shop_key(value: object) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    return text.lower()


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()

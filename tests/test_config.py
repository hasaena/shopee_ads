from pathlib import Path
from decimal import Decimal

import pytest

from dotori_shopee_automation.config import get_settings, load_shops


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_shops_ok(tmp_path, monkeypatch) -> None:
    yaml_path = tmp_path / "shops.yaml"
    _write_yaml(
        yaml_path,
        """
- shop_key: shop_a
  label: SHOP_A
  enabled: true
- shop_key: shop_b
  label: SHOP_B
  enabled: false
  timezone: UTC
  daily_budget_est: 123456
""".strip(),
    )
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(yaml_path))
    get_settings.cache_clear()

    shops = load_shops()
    assert len(shops) == 2
    assert shops[0].shop_key == "shop_a"
    assert shops[0].timezone == "Asia/Ho_Chi_Minh"
    assert shops[0].daily_budget_est is None
    assert shops[1].shop_key == "shop_b"
    assert shops[1].timezone == "UTC"
    assert shops[1].daily_budget_est == Decimal("123456")


def test_load_shops_missing_file(monkeypatch, tmp_path) -> None:
    missing_path = tmp_path / "missing.yaml"
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(missing_path))
    get_settings.cache_clear()

    with pytest.raises(FileNotFoundError) as exc:
        load_shops()
    assert "Shops config not found" in str(exc.value)

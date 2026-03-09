from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.scheduler import _build_report_md_attachment


def test_build_report_md_attachment_contains_key_sections(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    html_path = tmp_path / "report.html"
    html_path.write_text("<html><head><meta charset='utf-8'></head><body>ok</body></html>", encoding="utf-8")
    data = {
        "totals": {
            "spend": Decimal("123.45"),
            "impressions": 1000,
            "clicks": 12,
            "orders": 3,
            "gmv": Decimal("456.78"),
        },
        "kpis": {"roas": Decimal("3.70"), "ctr": Decimal("0.012")},
        "top_spend": [
            {"campaign_id": "C1", "campaign_name": "Campaign One", "spend": Decimal("99.99"), "roas": Decimal("2.10")}
        ],
    }

    md_path, md_name = _build_report_md_attachment(
        output_path=html_path,
        shop_key="samord",
        shop_label="SAMORD",
        report_kind="midday",
        report_date=date(2026, 2, 25),
        window_start=date(2026, 2, 25),
        window_end=date(2026, 2, 25),
        report_url="http://localhost:8000/reports/samord/daily/2026-02-25_midday.html",
        data=data,
    )

    assert md_path is not None
    assert md_name == "SAMORD_2026-02-25_midday.md"
    content = Path(md_path).read_text(encoding="utf-8")
    assert "# SAMORD Báo cáo hằng ngày (midday)" in content
    assert "Chỉ số chính:" in content
    assert "Campaign One" in content
    assert "Huong dan mo file local:" in content
    assert "localhost" in content

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token


runner = CliRunner()


def _write_phase1_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
            ]
        ),
        encoding="utf-8",
    )


def _seed_tokens() -> None:
    init_db()
    session = SessionLocal()
    try:
        expires = datetime.now(timezone.utc) + timedelta(hours=4)
        refresh_expires = datetime.now(timezone.utc) + timedelta(days=20)
        upsert_token(
            session,
            "samord",
            497412318,
            "A" * 105,
            "R" * 106,
            expires,
            refresh_expires,
        )
        upsert_token(
            session,
            "minmin",
            567655304,
            "B" * 105,
            "S" * 106,
            expires,
            refresh_expires,
        )
        session.commit()
    finally:
        session.close()


def _setup_env(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "task125.db"
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("TIMEZONE", "Asia/Ho_Chi_Minh")
    monkeypatch.setenv("ALLOW_NETWORK", "0")
    monkeypatch.setenv("DISCORD_DRY_RUN", "1")
    monkeypatch.setenv("DISCORD_OUTBOX_PATH", str(tmp_path / "discord_outbox.log"))
    get_settings.cache_clear()
    init_db()
    _seed_tokens()


def test_render_from_db_requires_seed_data(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)
    render_dir = tmp_path / "reports_render"
    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "report",
            "render-from-db",
            "--date",
            "2026-02-25",
            "--job",
            "daily-midday",
            "--shops",
            "samord,minmin",
            "--reports-dir",
            str(render_dir),
            "--discord-mode",
            "dry-run",
        ],
    )
    assert result.exit_code == 2
    assert "render_from_db_ok=0 reason=missing_db_data" in result.output


def test_render_from_db_renders_without_ingest(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)
    seed_reports = tmp_path / "reports_seed"
    seed_artifacts = tmp_path / "artifacts_seed"
    seed_result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "schedule",
            "run-once",
            "--date",
            "2026-02-25",
            "--job",
            "daily-midday",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--token-mode",
            "passive",
            "--reports-dir",
            str(seed_reports),
            "--artifacts-root",
            str(seed_artifacts),
            "--no-send-discord",
        ],
    )
    assert seed_result.exit_code == 0, seed_result.output

    def _fail_ingest(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("ingest_ads_live should not be called in render-from-db")

    monkeypatch.setattr("dotori_shopee_automation.scheduler.ingest_ads_live", _fail_ingest)

    render_dir = tmp_path / "reports_render"
    bundle_path = tmp_path / "render_bundle.zip"
    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "report",
            "render-from-db",
            "--date",
            "2026-02-25",
            "--job",
            "daily-midday",
            "--shops",
            "samord,minmin",
            "--reports-dir",
            str(render_dir),
            "--discord-mode",
            "dry-run",
            "--discord-attach-report-zip",
            "--discord-attach-report-md",
            "--bundle-out",
            str(bundle_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "render_from_db_start" in result.output
    assert "report_path shop=samord" in result.output
    assert "report_path shop=minmin" in result.output
    assert "render_from_db_ok=1" in result.output
    assert "allow_network_required" not in result.output
    assert "discord_dry_run=1 channel=report" in result.output

    samord_report = render_dir / "samord" / "daily" / "2026-02-25_midday.html"
    minmin_report = render_dir / "minmin" / "daily" / "2026-02-25_midday.html"
    assert samord_report.exists()
    assert minmin_report.exists()
    assert samord_report.stat().st_size > 0
    assert minmin_report.stat().st_size > 0
    assert bundle_path.exists()
    assert bundle_path.stat().st_size > 0

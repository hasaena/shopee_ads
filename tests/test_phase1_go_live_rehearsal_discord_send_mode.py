from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

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


def _setup_env(monkeypatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "go_live_send.db"
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.setenv("DISCORD_WEBHOOK_ALERTS_URL", "https://discord.local/alerts")
    monkeypatch.delenv("DISCORD_DRY_RUN", raising=False)
    monkeypatch.delenv("DISCORD_ALLOW_SEND_IN_FIXTURES", raising=False)
    monkeypatch.delenv("DISCORD_ATTACH_REPORT_HTML", raising=False)
    monkeypatch.delenv("DISCORD_ATTACH_REPORT_ZIP", raising=False)
    monkeypatch.delenv("DISCORD_ATTACH_REPORT_MD", raising=False)
    get_settings.cache_clear()
    init_db()
    return db_path


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


def test_rehearsal_send_requires_confirm(monkeypatch, tmp_path: Path) -> None:
    db_path = _setup_env(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--allow-network",
            "--discord-mode",
            "send",
            "--reports-dir",
            str(tmp_path / "reports"),
            "--summary-out",
            str(tmp_path / "summary.json"),
            "--bundle-out",
            str(tmp_path / "bundle.zip"),
        ],
    )
    assert result.exit_code == 2
    assert "discord_send_requires_confirm=1" in result.output


def test_rehearsal_send_requires_allow_network(monkeypatch, tmp_path: Path) -> None:
    db_path = _setup_env(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--discord-mode",
            "send",
            "--confirm-discord-send",
            "--reports-dir",
            str(tmp_path / "reports"),
            "--summary-out",
            str(tmp_path / "summary.json"),
            "--bundle-out",
            str(tmp_path / "bundle.zip"),
        ],
    )
    assert result.exit_code == 2
    assert "discord_send_requires_allow_network=1" in result.output


def test_rehearsal_send_posts_to_discord_when_confirmed(monkeypatch, tmp_path: Path) -> None:
    db_path = _setup_env(monkeypatch, tmp_path)
    _seed_tokens()

    sent: list[dict[str, object]] = []

    def fake_post(url: str, json=None, timeout=None):  # noqa: A002
        sent.append({"url": url, "json": json, "timeout": timeout})
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--allow-network",
            "--discord-mode",
            "send",
            "--confirm-discord-send",
            "--reports-dir",
            str(tmp_path / "reports"),
            "--summary-out",
            str(tmp_path / "summary.json"),
            "--bundle-out",
            str(tmp_path / "bundle.zip"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert sent, "expected discord posts in send mode"
    assert "discord_send_ok=1 channel=alerts" in result.output
    assert "discord_send_ok=1 channel=report" in result.output
    assert any(
        str(item.get("json", {}).get("content", "")).startswith("[SAMORD][ALERT]")
        or str(item.get("json", {}).get("content", "")).startswith("[MINMIN][ALERT]")
        or (
            isinstance(item.get("json", {}).get("embeds"), list)
            and any(
                "[SAMORD]" in str(embed.get("title", ""))
                or "[MINMIN]" in str(embed.get("title", ""))
                for embed in item.get("json", {}).get("embeds", [])
                if isinstance(embed, dict)
            )
        )
        for item in sent
    )
    assert any(
        str(item.get("url", "")).endswith("/report")
        and isinstance(item.get("json", {}).get("embeds"), list)
        for item in sent
    )


def test_rehearsal_dry_run_does_not_post(monkeypatch, tmp_path: Path) -> None:
    db_path = _setup_env(monkeypatch, tmp_path)
    _seed_tokens()

    calls = {"post": 0}

    def fake_post(url: str, json=None, timeout=None):  # noqa: A002
        calls["post"] += 1
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "go-live",
            "rehearsal",
            "--date",
            "2026-02-03",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--db",
            str(db_path),
            "--discord-mode",
            "dry-run",
            "--reports-dir",
            str(tmp_path / "reports"),
            "--summary-out",
            str(tmp_path / "summary.json"),
            "--bundle-out",
            str(tmp_path / "bundle.zip"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls["post"] == 0
    assert "discord_dry_run=1 channel=alerts" in result.output
    assert "discord_dry_run=1 channel=report" in result.output

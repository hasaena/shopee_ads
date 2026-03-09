from __future__ import annotations

import copy
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation import cli
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.discord_notifier import _format_message
from dotori_shopee_automation.ops.doctor_notify import OpsDoctorNotifyState


runner = CliRunner()


def _write_shops(path: Path) -> None:
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


def _alert_payload() -> dict:
    return {
        "ok": True,
        "phase": "phase1",
        "shops": ["minmin", "samord"],
        "issues": [
            {
                "shop": "samord",
                "code": "SNAPSHOT_STALE",
                "severity": "warn",
                "hint": "check",
            },
            {
                "shop": "minmin",
                "code": "SNAPSHOT_STALE",
                "severity": "warn",
                "hint": "check",
            },
        ],
        "token": {},
        "db": {"latest_ingest": {}},
        "reports": {
            "latest": {
                "samord": {
                    "daily_midday": {
                        "url": "http://localhost:8000/reports/samord/daily/2026-02-25_midday.html?token=***",
                        "is_stale": False,
                    },
                    "daily_final": None,
                    "weekly": None,
                },
                "minmin": {
                    "daily_midday": {
                        "url": "http://localhost:8000/reports/minmin/daily/2026-02-25_midday.html?token=***",
                        "is_stale": False,
                    },
                    "daily_final": None,
                    "weekly": None,
                },
            }
        },
        "freshness": {"per_shop": {}},
    }


def _clean_payload() -> dict:
    payload = _alert_payload()
    payload["issues"] = []
    return payload


def _base_args(tmp_path: Path) -> list[str]:
    return [
        "ops",
        "phase1",
        "doctor",
        "notify",
        "--shops",
        "samord,minmin",
        "--reports-dir",
        str(tmp_path / "reports"),
        "--artifacts-dir",
        str(tmp_path / "artifacts"),
        "--min-severity",
        "warn",
        "--cooldown-sec",
        "3600",
        "--resolved-cooldown-sec",
        "21600",
        "--max-issues",
        "20",
    ]


def _setup_env(monkeypatch, tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'notify.db').as_posix()}")
    monkeypatch.setenv("TIMEZONE", "Asia/Ho_Chi_Minh")
    monkeypatch.setenv("ALLOW_NETWORK", "0")
    monkeypatch.setenv("REPORT_BASE_URL", "http://localhost:8000")
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "TEST_TOKEN_DO_NOT_USE")
    get_settings.cache_clear()


def test_doctor_notify_dry_run_default_does_not_persist_state(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "_build_phase1_status_payload_for_cli",
        lambda **_: copy.deepcopy(_alert_payload()),
    )
    sent_calls: list[dict] = []
    monkeypatch.setattr(
        cli,
        "send",
        lambda *args, **kwargs: sent_calls.append({"args": args, "kwargs": kwargs}),
    )

    args = _base_args(tmp_path) + ["--discord-mode", "dry-run"]
    first = runner.invoke(cli.app, args)
    assert first.exit_code == 0, first.stdout
    assert "would_send=1" in first.stdout
    assert "doctor_notify_preview" in first.stdout
    assert "\\nReports:" in first.stdout
    assert "\\nNext: ops phase1 doctor --shops samord" in first.stdout
    assert "doctor_summary_path=" in first.stdout
    assert sent_calls == []
    assert (tmp_path / "artifacts" / "doctor_status.json").exists()
    assert (tmp_path / "artifacts" / "doctor_summary.md").exists()

    second = runner.invoke(cli.app, args)
    assert second.exit_code == 0, second.stdout
    assert "cooldown_skip=1" not in second.stdout
    assert second.stdout.count("would_send=1") >= 2
    assert sent_calls == []
    init_db()
    session = SessionLocal()
    try:
        assert session.query(OpsDoctorNotifyState).count() == 0
    finally:
        session.close()
    get_settings.cache_clear()


def test_doctor_notify_dry_run_persist_state_has_cooldown(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "_build_phase1_status_payload_for_cli",
        lambda **_: copy.deepcopy(_alert_payload()),
    )
    sent_calls: list[dict] = []
    monkeypatch.setattr(
        cli,
        "send",
        lambda *args, **kwargs: sent_calls.append({"args": args, "kwargs": kwargs}),
    )

    args = _base_args(tmp_path) + ["--discord-mode", "dry-run", "--persist-state"]
    first = runner.invoke(cli.app, args)
    assert first.exit_code == 0, first.stdout
    assert "would_send=1" in first.stdout

    second = runner.invoke(cli.app, args)
    assert second.exit_code == 0, second.stdout
    assert "cooldown_skip=1" in second.stdout
    assert sent_calls == []
    init_db()
    session = SessionLocal()
    try:
        rows = session.query(OpsDoctorNotifyState).all()
        assert len(rows) == 2
        assert all(str(row.last_action or "").strip() == "alert" for row in rows)
        assert all(str(row.last_alert_at or "").strip() == "" for row in rows)
        assert all(str(row.last_sent_at or "").strip() == "" for row in rows)
        assert all(str(row.cooldown_until or "").strip() for row in rows)
    finally:
        session.close()
    get_settings.cache_clear()


def test_doctor_notify_dry_run_then_send_is_not_blocked(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "_build_phase1_status_payload_for_cli",
        lambda **_: copy.deepcopy(_alert_payload()),
    )
    sent_messages: list[dict] = []
    monkeypatch.setattr(
        cli,
        "send",
        lambda channel, text, shop_label=None, **kwargs: sent_messages.append(
            {
                "channel": channel,
                "text": text,
                "shop_label": shop_label,
                "kwargs": kwargs,
            }
        ),
    )

    dry_run_args = _base_args(tmp_path) + ["--discord-mode", "dry-run"]
    dry = runner.invoke(cli.app, dry_run_args)
    assert dry.exit_code == 0, dry.stdout
    assert "would_send=1" in dry.stdout

    send_args = _base_args(tmp_path) + [
        "--discord-mode",
        "send",
        "--confirm-discord-send",
    ]
    send_run = runner.invoke(cli.app, send_args)
    assert send_run.exit_code == 0, send_run.stdout
    assert "cooldown_skip=1" not in send_run.stdout
    assert len(sent_messages) == 2
    get_settings.cache_clear()


def test_doctor_notify_dry_run_persist_state_keeps_last_sent_at(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "_build_phase1_status_payload_for_cli",
        lambda **_: copy.deepcopy(_alert_payload()),
    )
    monkeypatch.setattr(cli, "send", lambda *args, **kwargs: None)

    init_db()
    session = SessionLocal()
    try:
        session.add(
            OpsDoctorNotifyState(
                shop_label="SAMORD",
                last_action="alert",
                last_alert_at="2026-02-27T02:00:00Z",
                last_sent_at="2026-02-27T02:00:00Z",
                cooldown_until="2026-02-27T02:30:00Z",
            )
        )
        session.add(
            OpsDoctorNotifyState(
                shop_label="MINMIN",
                last_action="alert",
                last_alert_at="2026-02-27T02:00:00Z",
                last_sent_at="2026-02-27T02:00:00Z",
                cooldown_until="2026-02-27T02:30:00Z",
            )
        )
        session.commit()
    finally:
        session.close()

    args = _base_args(tmp_path) + ["--discord-mode", "dry-run", "--persist-state"]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.stdout
    assert "would_send=1" in result.stdout

    init_db()
    session = SessionLocal()
    try:
        rows = {
            str(row.shop_label): row
            for row in session.query(OpsDoctorNotifyState).all()
        }
        assert rows["SAMORD"].last_sent_at == "2026-02-27T02:00:00Z"
        assert rows["MINMIN"].last_sent_at == "2026-02-27T02:00:00Z"
        assert str(rows["SAMORD"].last_action or "") == "alert"
        assert str(rows["MINMIN"].last_action or "") == "alert"
    finally:
        session.close()
    get_settings.cache_clear()


def test_doctor_notify_send_prefix_and_resolved(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)

    payloads = [copy.deepcopy(_alert_payload()), copy.deepcopy(_clean_payload()), copy.deepcopy(_clean_payload())]

    def _next_payload(**_kwargs):
        if payloads:
            return payloads.pop(0)
        return copy.deepcopy(_clean_payload())

    monkeypatch.setattr(cli, "_build_phase1_status_payload_for_cli", _next_payload)

    sent_messages: list[dict] = []

    def _fake_send(channel, text, shop_label=None, **kwargs):
        sent_messages.append(
            {
                "channel": channel,
                "shop_label": shop_label,
                "text": text,
                "formatted": _format_message(channel, text, shop_label),
                "kwargs": kwargs,
            }
        )

    monkeypatch.setattr(cli, "send", _fake_send)

    base = _base_args(tmp_path) + [
        "--discord-mode",
        "send",
        "--confirm-discord-send",
    ]

    first = runner.invoke(cli.app, base)
    assert first.exit_code == 0, first.stdout
    assert len(sent_messages) == 2
    formatted_first = [row["formatted"] for row in sent_messages]
    assert any(msg.startswith("[SAMORD][ALERT] OPS_DOCTOR WARN") for msg in formatted_first)
    assert any(msg.startswith("[MINMIN][ALERT] OPS_DOCTOR WARN") for msg in formatted_first)
    assert all("\nReports:" in msg for msg in formatted_first)
    assert all("\nNext: ops phase1 doctor --shops " in msg for msg in formatted_first)
    assert all(row["channel"] == "alerts" for row in sent_messages)
    assert all(row["kwargs"].get("md_attachment_path") is not None for row in sent_messages)
    init_db()
    session = SessionLocal()
    try:
        first_rows = session.query(OpsDoctorNotifyState).all()
        assert len(first_rows) == 2
        assert all(str(row.last_action or "") == "alert" for row in first_rows)
        assert all(str(row.last_sent_at or "").strip() != "" for row in first_rows)
    finally:
        session.close()

    second = runner.invoke(cli.app, base)
    assert second.exit_code == 0, second.stdout
    assert len(sent_messages) == 4
    formatted_second = [row["formatted"] for row in sent_messages[2:]]
    assert any(msg.startswith("[SAMORD][ALERT] OPS_DOCTOR RESOLVED") for msg in formatted_second)
    assert any(msg.startswith("[MINMIN][ALERT] OPS_DOCTOR RESOLVED") for msg in formatted_second)
    init_db()
    session = SessionLocal()
    try:
        second_rows = session.query(OpsDoctorNotifyState).all()
        assert len(second_rows) == 2
        assert all(str(row.last_action or "") == "resolved" for row in second_rows)
        assert all(str(row.last_sent_at or "").strip() != "" for row in second_rows)
    finally:
        session.close()

    third = runner.invoke(cli.app, base)
    assert third.exit_code == 0, third.stdout
    assert len(sent_messages) == 4
    init_db()
    session = SessionLocal()
    try:
        third_rows = session.query(OpsDoctorNotifyState).all()
        assert len(third_rows) == 2
        assert all(str(row.last_action or "") == "ok" for row in third_rows)
    finally:
        session.close()
    get_settings.cache_clear()


def test_doctor_notify_send_aggregate(monkeypatch, tmp_path: Path) -> None:
    _setup_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "_build_phase1_status_payload_for_cli",
        lambda **_: copy.deepcopy(_alert_payload()),
    )

    sent_messages: list[dict] = []

    def _fake_send(channel, text, shop_label=None, **kwargs):
        sent_messages.append(
            {
                "channel": channel,
                "shop_label": shop_label,
                "text": text,
                "formatted": _format_message(channel, text, shop_label),
                "kwargs": kwargs,
            }
        )

    monkeypatch.setattr(cli, "send", _fake_send)

    args = _base_args(tmp_path) + [
        "--discord-mode",
        "send",
        "--confirm-discord-send",
        "--aggregate",
    ]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.stdout
    assert "aggregate=1" in result.stdout
    assert len(sent_messages) == 1
    text = sent_messages[0]["formatted"]
    assert "[SAMORD][ALERT] OPS_DOCTOR WARN" in text
    assert "[MINMIN][ALERT] OPS_DOCTOR WARN" in text
    assert sent_messages[0]["kwargs"].get("md_attachment_path") is not None
    get_settings.cache_clear()

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation import cli
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
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


class _WebhookRecorder:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        self._lock = threading.Lock()
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> str:
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # type: ignore[override]
                size = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(size)
                with recorder._lock:
                    recorder.records.append(
                        {
                            "path": self.path,
                            "headers": dict(self.headers.items()),
                            "body": body,
                        }
                    )
                self.send_response(204)
                self.end_headers()

            def log_message(self, _format: str, *args) -> None:  # noqa: A003
                return

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address
        return f"http://{host}:{port}/webhook"

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=3)

    def snapshot(self) -> list[dict[str, object]]:
        with self._lock:
            return list(self.records)


def _extract_content(record: dict[str, object]) -> str:
    raw = record.get("body")
    if not isinstance(raw, (bytes, bytearray)):
        return ""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return ""
    return str(payload.get("content") or "")


def test_doctor_notify_send_local_webhook_smoke(monkeypatch, tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "task109.db"
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    token_file = Path("tests/fixtures/appsscript_tokens/shopee_tokens_export_example.json")

    recorder = _WebhookRecorder()
    webhook_url = recorder.start()
    try:
        monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
        monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
        monkeypatch.setenv("TIMEZONE", "Asia/Ho_Chi_Minh")
        monkeypatch.setenv("ALLOW_NETWORK", "1")
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        monkeypatch.setenv("DISCORD_ALERTS_WEBHOOK_URL", webhook_url)
        monkeypatch.setenv("DISCORD_REPORT_WEBHOOK_URL", "https://example.invalid/report")
        monkeypatch.setenv("DISCORD_WEBHOOK_ALERTS_URL", webhook_url)
        monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://example.invalid/report")
        monkeypatch.delenv("DISCORD_DRY_RUN", raising=False)
        monkeypatch.delenv("DISCORD_OUTBOX_PATH", raising=False)
        monkeypatch.setenv("REPORT_BASE_URL", "http://localhost:8000")
        monkeypatch.setenv("REPORT_ACCESS_TOKEN", "TEST_TOKEN_DO_NOT_USE")
        get_settings.cache_clear()

        sync = runner.invoke(
            cli.app,
            [
                "ops",
                "phase1",
                "token",
                "appsscript",
                "sync",
                "--token-file",
                str(token_file),
                "--shops",
                "samord,minmin",
            ],
        )
        assert sync.exit_code == 0, sync.stdout

        midday = runner.invoke(
            cli.app,
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
                str(reports_dir),
                "--artifacts-root",
                "collaboration/artifacts/shopee_api",
                "--no-send-discord",
            ],
        )
        assert midday.exit_code == 0, midday.stdout

        notify_args = [
            "ops",
            "phase1",
            "doctor",
            "notify",
            "--shops",
            "samord,minmin",
            "--reports-dir",
            str(reports_dir),
            "--min-severity",
            "warn",
            "--discord-mode",
            "send",
            "--confirm-discord-send",
            "--cooldown-sec",
            "3600",
            "--resolved-cooldown-sec",
            "21600",
            "--max-issues",
            "20",
        ]

        first = runner.invoke(cli.app, notify_args)
        assert first.exit_code == 0, first.stdout
        assert "doctor_notify_summary" in first.stdout
        assert "send_enabled=1" in first.stdout
        assert "suppressed=0" in first.stdout

        first_records = recorder.snapshot()
        assert len(first_records) == 2

        contents = [_extract_content(row) for row in first_records]
        assert any(text.startswith("[SAMORD][ALERT] OPS_DOCTOR") for text in contents)
        assert any(text.startswith("[MINMIN][ALERT] OPS_DOCTOR") for text in contents)
        for text in contents:
            lines = text.splitlines()
            assert len(lines) == 3
            assert "OPS_DOCTOR" in text
            assert "Reports:" in text
            assert "Next:" in text

        init_db()
        session = SessionLocal()
        try:
            rows = {
                str(row.shop_label): row for row in session.query(OpsDoctorNotifyState).all()
            }
            assert sorted(rows.keys()) == ["MINMIN", "SAMORD"]
            assert all(str(row.last_action or "") == "alert" for row in rows.values())
            assert all(str(row.last_sent_at or "").strip() != "" for row in rows.values())
        finally:
            session.close()

        second = runner.invoke(cli.app, notify_args)
        assert second.exit_code == 0, second.stdout
        assert "cooldown_skip=1" in second.stdout
        assert "suppressed=" in second.stdout
        second_records = recorder.snapshot()
        assert len(second_records) == 2
    finally:
        recorder.stop()
        get_settings.cache_clear()

from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    file_path = Path(path)
    assert file_path.exists(), f"missing template: {file_path}"
    try:
        return file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return file_path.read_text(encoding="utf-16")


def test_monitoring_systemd_templates_include_notify_flags() -> None:
    service = _read(
        "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_doctor_notify.service"
    )
    timer = _read(
        "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_doctor_notify.timer"
    )

    assert "ops phase1 doctor notify" in service
    assert "--shops samord,minmin" in service
    assert "--aggregate" in service
    assert "--min-severity warn" in service
    assert "--cooldown-sec 3600" in service
    assert "--resolved-cooldown-sec 21600" in service
    assert "--persist-state" in service
    assert ":-" not in service
    assert "Environment=DOTORI_DOCTOR_DISCORD_MODE=dry-run" in service
    assert (
        "--discord-mode $DOTORI_DOCTOR_DISCORD_MODE" in service
        or "--discord-mode ${DOTORI_DOCTOR_DISCORD_MODE}" in service
    )
    assert "--confirm-discord-send" in service

    assert "OnUnitActiveSec=15min" in timer
    assert "OnBootSec=2min" in timer
    assert "Persistent=true" in timer


def test_monitoring_windows_template_includes_notify_flags() -> None:
    xml = _read(
        "collaboration/runbooks/windows_task_scheduler_phase1_doctor_notify.xml"
    )

    assert "ops phase1 doctor notify" in xml
    assert "--shops samord,minmin" in xml
    assert "--aggregate" in xml
    assert "--min-severity warn" in xml
    assert "--cooldown-sec 3600" in xml
    assert "--resolved-cooldown-sec 21600" in xml
    assert "--persist-state" in xml
    assert "--discord-mode dry-run" in xml
    assert "--confirm-discord-send" in xml
    assert "PT15M" in xml

from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    file_path = Path(path)
    assert file_path.exists(), f"missing template: {file_path}"
    return file_path.read_text(encoding="utf-8")


def test_phase1_main_service_unit_is_systemd_safe() -> None:
    content = _read(
        "collaboration/runbooks/systemd/dotori_shopee_automation_phase1.service"
    )

    assert "WorkingDirectory=/opt/dotori_shopee_automation" in content
    assert "EnvironmentFile=/etc/dotori_shopee_automation/.env" in content
    assert "Environment=SHOPS_CONFIG_PATH=" in content
    assert "Environment=DATABASE_URL=" in content
    assert "Environment=TIMEZONE=Asia/Ho_Chi_Minh" in content
    assert "ExecStart=/opt/dotori_shopee_automation/.venv/bin/python -m dotori_shopee_automation.cli ops run" in content

    lines = [line.strip() for line in content.splitlines()]
    envfile_idx = next(
        i for i, line in enumerate(lines) if line.startswith("EnvironmentFile=")
    )
    env_idxs = [i for i, line in enumerate(lines) if line.startswith("Environment=")]
    assert env_idxs
    assert max(env_idxs) < envfile_idx
    allow_idxs = [
        i
        for i, line in enumerate(lines)
        if line.startswith("Environment=ALLOW_NETWORK=")
    ]
    assert len(allow_idxs) == 1
    assert allow_idxs[0] < envfile_idx
    assert all(
        i <= envfile_idx
        for i, line in enumerate(lines)
        if line.startswith("Environment=ALLOW_NETWORK=")
    )

    # Keep the unit shell-free and avoid bash-style default expansion.
    assert "bash -lc" not in content
    assert "sh -c" not in content
    assert ":-" not in content

    assert "Restart=on-failure" in content
    assert "RestartSec=5" in content
    assert "After=network-online.target" in content
    assert "Wants=network-online.target" in content

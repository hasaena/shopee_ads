from __future__ import annotations

from pathlib import Path


def test_windows_task_installer_script_contains_mode_and_markers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = (
        repo_root
        / "collaboration"
        / "runbooks"
        / "install_phase1_windows_tasks.ps1"
    )
    text = script_path.read_text(encoding="utf-8")

    assert "DotoriShopeePhase1OpsRun" in text
    assert "DotoriShopeePhase1DoctorNotify15m" in text
    assert "ValidateSet('Auto', 'Admin', 'User')" in text
    assert "WindowsPrincipal" in text
    assert "mode_selected=" in text
    assert "is_admin=" in text
    assert "ops_task_exists=" in text
    assert "doctor_task_exists=" in text

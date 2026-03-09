from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import zipfile


EXPECTED_MEMBERS = [
    "collaboration/runbooks/deploy_quickstart_phase1.md",
    "collaboration/runbooks/phase1_go_live_now.md",
    "collaboration/runbooks/PHASE1_STOPLINE.md",
    "collaboration/runbooks/phase1_go_live_checklist.md",
    "collaboration/runbooks/linux_phase1_server_go_live.md",
    "collaboration/runbooks/phase1_gms_probe_hourly.sh",
    "collaboration/runbooks/systemd/dotori_shopee_automation_phase1.service",
    "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_doctor_notify.service",
    "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_doctor_notify.timer",
    "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_gms_probe.service",
    "collaboration/runbooks/systemd/dotori_shopee_automation_phase1_gms_probe.timer",
    "collaboration/runbooks/env/phase1_server.env.example",
    "collaboration/runbooks/appsscript_token_push_access_only.md",
    "collaboration/runbooks/windows_task_scheduler_phase1_doctor_notify.xml",
    "collaboration/runbooks/install_phase1_systemd_units.sh",
    "collaboration/runbooks/smoke_phase1.sh",
    "collaboration/runbooks/e2e_access_only_token_push_check.sh",
]


def test_phase1_go_live_pack_bundle_contains_fixed_unit(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "collaboration" / "runbooks" / "build_phase1_go_live_pack.py"
    out_dir = tmp_path / "packs"
    out_dir.mkdir(parents=True, exist_ok=True)
    forced_date = "2026-02-27"

    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--out-dir",
            str(out_dir),
            "--date",
            forced_date,
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr

    latest_zip = out_dir / "phase1_go_live_systemd_pack_latest.zip"
    dated_zip = out_dir / f"phase1_go_live_systemd_pack_{forced_date}_v2.zip"
    assert latest_zip.exists()
    assert dated_zip.exists()

    with zipfile.ZipFile(latest_zip, "r") as archive:
        members = archive.namelist()
        assert members == EXPECTED_MEMBERS
        assert len(members) == 17
        assert "collaboration/runbooks/phase1_go_live_now.md" in members
        assert "collaboration/runbooks/PHASE1_STOPLINE.md" in members
        service_text = archive.read(
            "collaboration/runbooks/systemd/dotori_shopee_automation_phase1.service"
        ).decode("utf-8")

    assert "EnvironmentFile below overrides" in service_text
    lines = [line.strip() for line in service_text.splitlines()]
    idx_allow = next(
        i for i, line in enumerate(lines) if line.startswith("Environment=ALLOW_NETWORK=0")
    )
    idx_envfile = next(
        i
        for i, line in enumerate(lines)
        if line.startswith("EnvironmentFile=/etc/dotori_shopee_automation/.env")
    )
    assert idx_allow < idx_envfile

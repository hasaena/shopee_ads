from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_sign_parity_parser_whitespace(tmp_path: Path) -> None:
    python_file = tmp_path / "python_sign.json"
    appsscript_file = tmp_path / "appsscript_sign.txt"

    python_payload = {
        "partner_id": 2010863,
        "partner_key_sha8": "fe46f5fa",
        "timestamp": 1700000000,
        "paths": ["/api/v2/shop/get_shop_info"],
        "shops": {
            "samord": {
                "shop_id": 497412318,
                "token_len": 10,
                "token_sha8": "aaaabbbb",
                "paths": {
                    "/api/v2/shop/get_shop_info": {
                        "timestamp": 1700000000,
                        "sign_input_sha8": "ccccdddd",
                        "sign_sha8": "eeeeffff",
                    }
                },
            }
        },
    }
    python_file.write_text(json.dumps(python_payload), encoding="utf-8")

    appsscript_lines = [
        " partner_id = 2010863 ",
        " partner_key_sha8 = fe46f5fa ",
        " shop = samord   shop_id = 497412318  token_len = 10   token_sha8 = aaaabbbb ",
        " shop = samord  path = /api/v2/shop/get_shop_info  ts = 1700000000  sign_input_sha8 = ccccdddd  sign_sha8 = eeeeffff ",
    ]
    appsscript_file.write_text("\n".join(appsscript_lines), encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "auth",
        "sign-parity",
        "--python-file",
        str(python_file),
        "--appsscript-txt",
        str(appsscript_file),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "sign_parity_ok=1" in result.stdout

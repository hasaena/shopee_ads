# dotori_shopee_automation

Local bootstrap skeleton for a multi-shop Shopee automation system.

## Requirements
- Python 3.11+

## Setup
```powershell
# from project root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]

# copy env template
Copy-Item .env.example .env
```

## Commands
Health check:
```powershell
python -m dotori_shopee_automation.cli health
```

Discord webhook test:
```powershell
python -m dotori_shopee_automation.cli discord-test --channel report --text "hello"
```

Start web server:
```powershell
python -m dotori_shopee_automation.cli web
```

Endpoints:
- `GET /health` -> `{ "status": "ok" }`
- `GET /reports/` -> HTML listing of `./reports`

## Tests
```powershell
pytest -q
```

## Git Baseline Workflow

Reference:
- `docs/GIT_AND_OPERATIONS_GUARDRAILS.md`

One-time remote connect:
```powershell
.\scripts\connect_github.ps1 -RemoteUrl "https://github.com/<user>/<repo>.git"
```

Notes:
- `.env`, token export files, runtime DB/reports/artifacts are ignored by default.
- Pre-commit secret guard is configured via `.githooks/pre-commit`.

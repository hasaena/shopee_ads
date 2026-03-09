# Ops Scheduler

This project ships a built-in APScheduler runner so you can run ads alerts + daily/weekly reports without manual CLI steps.

## Quick start (manual)
```powershell
# Run scheduler (blocking)
python -m dotori_shopee_automation.cli ops scheduler

# Run scheduler + web server
python -m dotori_shopee_automation.cli ops run

# Run all jobs once (smoke test)
python -m dotori_shopee_automation.cli ops run-once --no-send-discord
```

## Environment variables
Set these in `.env` (copy from `.env.example`):
- `SCHEDULER_ENABLED=false`
- `SCHEDULER_TIMEZONE=Asia/Ho_Chi_Minh`
- `DETECT_INTERVAL_MINUTES=15`
- `DAILY_FINAL_TIME=00:00`
- `DAILY_MIDDAY_TIME=13:00`
- `WEEKLY_REPORT_DOW=MON`
- `WEEKLY_REPORT_TIME=09:00`
- `SCHEDULER_SEND_DISCORD=true`

## systemd example (Linux)
Create a service file at `/etc/systemd/system/dotori-ads.service`:
```
[Unit]
Description=Dotori Shopee Ads Scheduler
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/dotori_shopee_automation
Environment="PYTHONUNBUFFERED=1"
EnvironmentFile=/opt/dotori_shopee_automation/.env
ExecStart=/opt/dotori_shopee_automation/.venv/bin/python -m dotori_shopee_automation.cli ops scheduler
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then enable/start:
```
sudo systemctl daemon-reload
sudo systemctl enable dotori-ads
sudo systemctl start dotori-ads
sudo systemctl status dotori-ads
```

## Windows Task Scheduler (alternative)
You can run the scheduler as a long-running task:
1) Create Task → Trigger: At startup
2) Action: Start a program
   - Program: `C:\path\to\python.exe`
   - Arguments: `-m dotori_shopee_automation.cli ops scheduler`
   - Start in: `C:\path\to\dotori_shopee_automation`
3) Set the `.env` file in the project root.

## Verifying service health
- Health endpoint: `GET http://WEB_HOST:WEB_PORT/health`
- Reports: `http://WEB_HOST:WEB_PORT/reports/...` (token required if enabled)
- Check `event_log` rows for `job_start` / `job_end` messages.

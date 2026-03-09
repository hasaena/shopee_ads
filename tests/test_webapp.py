from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


def test_health_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_reports_index(monkeypatch) -> None:
    monkeypatch.delenv("REPORT_ACCESS_TOKEN", raising=False)
    get_settings.cache_clear()

    client = TestClient(app)
    response = client.get("/reports/")
    assert response.status_code == 200
    assert "Reports" in response.text
    assert "daily-open-btn" in response.text
    assert "weekly-open-btn" in response.text
    assert "조회" in response.text


def test_reports_token_protection(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "secret-token")
    get_settings.cache_clear()

    client = TestClient(app)
    response = client.get("/reports/some_report.html")
    assert response.status_code == 401
    get_settings.cache_clear()


def test_reports_redirect_preserves_token(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "secret-token")
    get_settings.cache_clear()

    client = TestClient(app)
    response = client.get("/reports?token=secret-token", follow_redirects=False)
    assert response.status_code in (302, 307)
    assert response.headers.get("location") == "/reports/?token=secret-token"
    get_settings.cache_clear()

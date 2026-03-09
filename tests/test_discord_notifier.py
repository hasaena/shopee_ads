from dotori_shopee_automation.discord_notifier import _format_message


def test_format_message_with_shop_label() -> None:
    assert _format_message("report", "hello", "SHOP_A") == "[SHOP_A] hello"
    assert _format_message("alerts", "hi", "SHOP_A") == "[SHOP_A][ALERT] hi"
    assert _format_message("actions", "go", "SHOP_A") == "[SHOP_A][ACTION] go"


def test_format_message_without_shop_label() -> None:
    assert _format_message("report", "hello", None) == "hello"

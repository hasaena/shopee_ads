from __future__ import annotations

from typing import Any


# Also redact `sign` because some HTTP client exceptions include full URLs.
REDACT_KEYS = {"access_token", "refresh_token", "sign", "partner_key"}


def redact_text(text: str) -> str:
    lower = text.lower()
    for key in REDACT_KEYS:
        if key in lower:
            text = _redact_query(text, key)
    text = _redact_token_like(text)
    return text


def redact_secrets(obj: Any, extra_keys: set[str] | None = None) -> Any:
    extra = {key.lower() for key in extra_keys} if extra_keys else set()
    if isinstance(obj, dict):
        redacted: dict[str, Any] = {}
        for key, value in obj.items():
            key_lower = str(key).lower()
            if _should_redact_key(key_lower, extra):
                redacted[key] = "***"
            else:
                redacted[key] = redact_secrets(value, extra_keys=extra_keys)
        return redacted
    if isinstance(obj, list):
        return [redact_secrets(item, extra_keys=extra_keys) for item in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


def _should_redact_key(key_lower: str, extra: set[str]) -> bool:
    if "token" in key_lower:
        return True
    return any(token in key_lower for token in extra)


def _redact_query(text: str, key: str) -> str:
    key_lower = key.lower()
    parts = text.split("&")
    for idx, part in enumerate(parts):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k.lower().endswith(key_lower):
            parts[idx] = f"{k}=***"
    return "&".join(parts)


def _redact_token_like(text: str) -> str:
    # Best-effort redaction for 'token=...'
    markers = ["access_token=", "refresh_token=", "sign=", "partner_key=", "token="]
    for marker in markers:
        if marker in text:
            prefix, rest = text.split(marker, 1)
            # keep only until next delimiter
            for sep in ["&", " ", "\n", "\t"]:
                if sep in rest:
                    token, tail = rest.split(sep, 1)
                    rest = f"***{sep}{tail}"
                    break
            else:
                rest = "***"
            text = prefix + marker + rest
    return text

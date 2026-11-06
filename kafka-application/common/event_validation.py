from datetime import datetime


class InvalidEvent(ValueError):
    """A permanent event contract or business validation failure."""


INT_MAX = 2**31 - 1
BIGINT_MAX = 2**63 - 1



def require_text(value, name, maximum):
    # 1. Kiểm tra kiểu dữ liệu, chuỗi rỗng và độ dài
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
    ):
        raise InvalidEvent(
            f"{name} must contain 1..{maximum} characters"
        )

    # 2. Không cho phép ký tự NUL (PostgreSQL không hỗ trợ)
    if "\x00" in value:
        raise InvalidEvent(
            f"{name} must not contain NUL"
        )

    # 3. Kiểm tra Unicode có thể mã hóa UTF-8 hợp lệ
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise InvalidEvent(
            f"{name} must be valid UTF-8 text"
        ) from exc

    return value



def require_quantity(value):
    if type(value) is not int or not 1 <= value <= INT_MAX:
        raise InvalidEvent("quantity must be a positive PostgreSQL INTEGER")
    return value


def require_envelope(event):
    if not isinstance(event, dict):
        raise InvalidEvent("Event must be an object")
    require_text(event.get("event_id"), "event_id", 100)
    timestamp = event.get("timestamp")
    if not isinstance(timestamp, str):
        raise InvalidEvent("timestamp must be an ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEvent("timestamp must be ISO 8601") from exc
    if parsed.utcoffset() is None:
        raise InvalidEvent("timestamp requires timezone")
    if not isinstance(event.get("data"), dict):
        raise InvalidEvent("data must be an object")
    return event["data"]

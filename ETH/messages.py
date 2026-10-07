"""Shared ETH message validation and UTC object-key construction."""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

CLASS_NAMES = {1: "person", 2: "door", 3: "fire", 4: "smoke"}
SENSORS = {
    "co2": ("ppm", 0, 5000),
    "temperature": ("C", -40, 85),
    "humidity": ("%RH", 0, 100),
    "pressure": ("hPa", None, None),  # Range awaits confirmation from ETH.
    "ch4": ("ppm", 1, 10000),
}


class ValidationError(ValueError):
    """A message does not follow the agreed ETH JSON contract."""


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be a JSON object")
    return value


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    return value


def parse_timestamp(value: Any) -> datetime:
    text = _required_text(value, "timestamp")
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)", text
    ):
        raise ValidationError(
            "timestamp must be an ISO 8601 UTC timestamp ending in Z or +00:00"
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("timestamp contains an invalid date or time") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValidationError("timestamp must use UTC")
    return parsed


def _path_id(value: Any, name: str) -> str:
    value = _required_text(value, name)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
        raise ValidationError(
            f"{name} must use 1-128 ASCII letters, digits, underscores or hyphens"
        )
    return value


def image_object_key(device_id: str, frame_id: str, timestamp: str) -> str:
    device = _path_id(device_id, "device_id")
    frame = _path_id(frame_id, "frame_id")
    captured_at = parse_timestamp(timestamp)
    return f"{device}/{captured_at:%Y/%m/%d}/{frame}.jpg"


def _validate_envelope(message: Any) -> dict[str, Any]:
    message = _object(message, "message")
    _required_text(message.get("incident_id"), "incident_id")
    _required_text(message.get("device_id"), "device_id")
    parse_timestamp(message.get("timestamp"))
    if message.get("source") != "ETH":
        raise ValidationError("source must be ETH")
    return message


def validate_environment(message: Any) -> None:
    message = _validate_envelope(message)
    data = _object(message.get("data"), "data")
    for name, (unit, lower, upper) in SENSORS.items():
        reading = _object(data.get(name), f"data.{name}")
        value = reading.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"data.{name}.value must be numeric (not boolean)")
        # JSON numbers have no float32 type; require a finite float32-representable range.
        if not -3.4028234663852886e38 <= value <= 3.4028234663852886e38:
            raise ValidationError(f"data.{name}.value must be finite and fit float32")
        if not math.isfinite(value):
            raise ValidationError(f"data.{name}.value must be finite")
        if lower is not None and not lower <= value <= upper:
            raise ValidationError(
                f"data.{name}.value must be between {lower} and {upper}"
            )
        if reading.get("unit") != unit:
            raise ValidationError(f"data.{name}.unit must be {unit}")


def _integer(value: Any, name: str, maximum: int) -> None:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValidationError(f"{name} must be an integer between 0 and {maximum}")


def validate_observation(message: Any) -> None:
    message = _validate_envelope(message)
    expected_key = image_object_key(
        message["device_id"], message.get("frame_id"), message["timestamp"]
    )
    image = _object(message.get("image"), "image")
    _required_text(image.get("bucket"), "image.bucket")
    if image.get("object_key") != expected_key:
        raise ValidationError(
            "image.object_key must match device_id/YYYY/MM/DD/frame_id.jpg in UTC"
        )
    if image.get("content_type") != "image/jpeg":
        raise ValidationError("image.content_type must be image/jpeg")
    detections = _object(message.get("detections"), "detections")
    _integer(detections.get("num_boxes"), "detections.num_boxes", 255)
    boxes = detections.get("boxes")
    if not isinstance(boxes, list):
        raise ValidationError("detections.boxes must be a JSON array")
    if detections["num_boxes"] != len(boxes):
        raise ValidationError("detections.num_boxes must equal the number of boxes")
    for index, box in enumerate(boxes):
        box = _object(box, f"boxes[{index}]")
        for coordinate in ("x", "y", "w", "h"):
            _integer(box.get(coordinate), f"boxes[{index}].{coordinate}", 512)
        _integer(box.get("cls_id"), f"boxes[{index}].cls_id", 255)

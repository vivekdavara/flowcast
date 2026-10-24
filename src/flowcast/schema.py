"""The clickstream event, schema version 1: its fields, a strict decoder and a byte-stable encoder.

DESIGN.md "The event" is the specification. The decoder is the reference for the Spark job's validation (day 2):
whatever it refuses must become a dead letter there too.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, TypeGuard

SCHEMA_VERSION = 1

EVENT_TYPES = ("page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout", "purchase")
DEVICES = ("desktop", "mobile", "tablet")

# The event types that name a product (product_id, category and price_cents set) and those that also carry a quantity.
PRODUCT_TYPES = frozenset({"product_view", "add_to_cart", "remove_from_cart", "purchase"})
QUANTITY_TYPES = frozenset({"add_to_cart", "remove_from_cart", "purchase"})

MIN_EVENT_TIME = 946_684_800_000  # 2000-01-01T00:00:00Z
MAX_EVENT_TIME = 4_102_444_800_000  # 2100-01-01T00:00:00Z, exclusive
MAX_PRICE_CENTS = 10_000_000
MAX_QUANTITY = 99

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_USER = re.compile(r"u[0-9]{1,31}")
_SESSION = re.compile(r"s[0-9]{1,31}")
_PRODUCT = re.compile(r"p[0-9]{1,31}")
_COUNTRY = re.compile(r"[A-Z]{2}")
_CATEGORY = re.compile(r"[a-z_]{1,32}")
# A path as a browser sends it: percent-encoded, so printable ASCII with no spaces.
_PAGE = re.compile(r"/[\x21-\x7e]{0,255}")


@dataclass(frozen=True, slots=True)
class Event:
    """One clickstream event. Field order is the wire order."""

    schema_version: int
    event_id: str
    event_type: str
    event_time: int
    user_id: str
    session_id: str
    device: str
    country: str
    page: str
    product_id: str | None
    category: str | None
    price_cents: int | None
    quantity: int | None

    def revenue_cents(self) -> int:
        """What this event adds to revenue: price times quantity for a purchase line, else 0."""
        if self.event_type != "purchase":
            return 0
        assert self.price_cents is not None and self.quantity is not None
        return self.price_cents * self.quantity


FIELDS: tuple[str, ...] = tuple(Event.__dataclass_fields__)


def values(event: Event) -> tuple[Any, ...]:
    """The event's field values in FIELDS order. Not dataclasses.astuple, which copies recursively: it was more than
    half of encode's time (scripts/measure_encode.py)."""
    return tuple(getattr(event, name) for name in FIELDS)


# Compact separators; one encoder made once rather than per call.
_JSON = json.JSONEncoder(separators=(",", ":"))


class InvalidEvent(ValueError):
    """A record that isn't a valid version-1 event; the message says why."""


def encode(event: Event) -> bytes:
    """The event's wire form: compact JSON, keys in FIELDS order, UTF-8. Checks the event first."""
    validate(event)
    return _JSON.encode({name: getattr(event, name) for name in FIELDS}).encode()


def decode(value: bytes | None) -> Event:
    """Parse and check one record. Raises InvalidEvent with the first problem found.

    None is a Kafka record with no value (a tombstone): not an event, so InvalidEvent too, not a TypeError.
    """
    if value is None:
        raise InvalidEvent("no value")
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidEvent("not UTF-8") from exc
    try:
        obj = json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_no_constants)
    except InvalidEvent:
        raise
    except (ValueError, RecursionError) as exc:
        raise InvalidEvent("not JSON") from exc
    if not isinstance(obj, dict):
        raise InvalidEvent("not a JSON object")
    missing = [name for name in FIELDS if name not in obj]
    if missing:
        raise InvalidEvent(f"missing field {missing[0]}")
    unknown = sorted(set(obj) - set(FIELDS))
    if unknown:
        raise InvalidEvent(f"unknown field {unknown[0]}")
    event = Event(**{name: obj[name] for name in FIELDS})
    validate(event)
    return event


def validate(event: Event) -> None:
    """Check every rule of DESIGN.md's table. Raises InvalidEvent."""
    if not _is_int(event.schema_version) or event.schema_version != SCHEMA_VERSION:
        raise InvalidEvent("schema_version must be 1")
    _string(event.event_id, "event_id", _UUID)
    if event.event_type not in EVENT_TYPES:
        raise InvalidEvent("unknown event_type")
    if not _is_int(event.event_time) or not MIN_EVENT_TIME <= event.event_time < MAX_EVENT_TIME:
        raise InvalidEvent("event_time out of range")
    _string(event.user_id, "user_id", _USER)
    _string(event.session_id, "session_id", _SESSION)
    if event.device not in DEVICES:
        raise InvalidEvent("unknown device")
    _string(event.country, "country", _COUNTRY)
    _string(event.page, "page", _PAGE)

    product = event.event_type in PRODUCT_TYPES
    for name in ("product_id", "category", "price_cents"):
        if (getattr(event, name) is not None) != product:
            raise InvalidEvent(f"{name} must be {'set' if product else 'null'} for {event.event_type}")
    if (event.quantity is not None) != (event.event_type in QUANTITY_TYPES):
        raise InvalidEvent(
            f"quantity must be {'set' if event.event_type in QUANTITY_TYPES else 'null'} for {event.event_type}"
        )
    if product:
        _string(event.product_id, "product_id", _PRODUCT)
        _string(event.category, "category", _CATEGORY)
        if not _is_int(event.price_cents) or not 1 <= event.price_cents <= MAX_PRICE_CENTS:
            raise InvalidEvent("price_cents out of range")
    if event.quantity is not None and (not _is_int(event.quantity) or not 1 <= event.quantity <= MAX_QUANTITY):
        raise InvalidEvent("quantity out of range")


def _is_int(value: object) -> TypeGuard[int]:
    # bool is a subclass of int, and JSON's true must not pass for 1.
    return isinstance(value, int) and not isinstance(value, bool)


def _string(value: Any, name: str, pattern: re.Pattern[str]) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise InvalidEvent(f"bad {name}")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # json.loads keeps the last of two equal keys silently; a record that says two things is refused instead.
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise InvalidEvent(f"duplicate field {key}")
        obj[key] = value
    return obj


def _no_constants(name: str) -> Any:
    # json.loads accepts NaN, Infinity and -Infinity, which aren't JSON.
    raise InvalidEvent(f"not JSON: {name}")

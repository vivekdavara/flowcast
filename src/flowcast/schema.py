"""The clickstream event, schema versions 1 and 2: its fields, a strict decoder and a byte-stable encoder.

DESIGN.md "The event" is the specification. The decoder is the reference for the Spark job's validation (day 2):
whatever it refuses must become a dead letter there too. Version 2 (day 3) adds one optional field, `campaign`; the
decoder reads both versions, so readers can be upgraded before the producers that write version 2.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, TypeGuard

SCHEMA_VERSIONS = (1, 2)

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
# A marketing campaign's name, as a utm_campaign tag: lowercase letters, digits, `_` and `-`.
_CAMPAIGN = re.compile(r"[a-z0-9_-]{1,64}")
# Each string field's format, matched whole. The data-quality rules (quality.py) restate them in SQL, so they are
# kept to what Python's re and DuckDB's RE2 read the same way: ASCII classes and counted repeats.
STRING_FORMATS: dict[str, re.Pattern[str]] = {
    "event_id": _UUID,
    "user_id": _USER,
    "session_id": _SESSION,
    "country": _COUNTRY,
    "page": _PAGE,
    "product_id": _PRODUCT,
    "category": _CATEGORY,
    "campaign": _CAMPAIGN,
}


@dataclass(frozen=True, slots=True)
class Event:
    """One clickstream event. Field order is the wire order. `campaign` is version 2's and None in version 1."""

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
    campaign: str | None = None

    def revenue_cents(self) -> int:
        """What this event adds to revenue: price times quantity for a purchase line, else 0."""
        if self.event_type != "purchase":
            return 0
        assert self.price_cents is not None and self.quantity is not None
        return self.price_cents * self.quantity


# Every field of the latest version, in wire order; version 1 has all but the last.
FIELDS: tuple[str, ...] = tuple(Event.__dataclass_fields__)
V1_FIELDS = FIELDS[:-1]


def wire_fields(version: int) -> tuple[str, ...]:
    """The keys of a record of this version, in wire order."""
    return V1_FIELDS if version == 1 else FIELDS


def values(event: Event) -> tuple[Any, ...]:
    """The event's field values in FIELDS order. Not dataclasses.astuple, which copies recursively: it was more than
    half of encode's time (scripts/measure_encode.py)."""
    return tuple(getattr(event, name) for name in FIELDS)


# Compact separators; one encoder made once rather than per call.
_JSON = json.JSONEncoder(separators=(",", ":"))


class InvalidEvent(ValueError):
    """A record that isn't a valid version-1 event; the message says why."""


def encode(event: Event) -> bytes:
    """The event's wire form: compact JSON, its version's keys in order (every key, null where a field doesn't apply),
    UTF-8. Checks the event first. A version-1 event's bytes are the same as before version 2 existed."""
    validate(event)
    return _JSON.encode({name: getattr(event, name) for name in wire_fields(event.schema_version)}).encode()


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
    missing = [name for name in V1_FIELDS if name not in obj]
    if missing:
        raise InvalidEvent(f"missing field {missing[0]}")
    # The version decides which keys are known; an invalid version is refused by validate, with version 1's keys.
    version = obj["schema_version"]
    allowed = FIELDS if _is_int(version) and version == 2 else V1_FIELDS
    unknown = sorted(set(obj) - set(allowed))
    if unknown:
        raise InvalidEvent(f"unknown field {unknown[0]}")
    given: dict[str, Any] = {name: obj.get(name) for name in FIELDS}  # an optional field left out is None
    event = Event(**given)
    validate(event)
    return event


def validate(event: Event) -> None:
    """Check every rule of DESIGN.md's table. Raises InvalidEvent."""
    if not _is_int(event.schema_version) or event.schema_version not in SCHEMA_VERSIONS:
        raise InvalidEvent("schema_version must be 1 or 2")
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
    if event.campaign is not None:
        if event.schema_version == 1:
            raise InvalidEvent("campaign is a version-2 field")
        _string(event.campaign, "campaign", _CAMPAIGN)


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

import dataclasses
import json
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from flowcast.schema import (
    EVENT_TYPES,
    FIELDS,
    MAX_EVENT_TIME,
    MIN_EVENT_TIME,
    PRODUCT_TYPES,
    QUANTITY_TYPES,
    Event,
    InvalidEvent,
    decode,
    encode,
)


def make(event_type: str = "purchase", **changes: Any) -> Event:
    product = event_type in PRODUCT_TYPES
    event = Event(
        schema_version=1,
        event_id="0f8fad5b-d9cb-469f-a165-70867728950e",
        event_type=event_type,
        event_time=1_767_225_600_000,
        user_id="u42",
        session_id="s7",
        device="mobile",
        country="US",
        page="/p/p17" if product else "/",
        product_id="p17" if product else None,
        category="home_garden" if product else None,
        price_cents=1999 if product else None,
        quantity=2 if event_type in QUANTITY_TYPES else None,
    )
    return dataclasses.replace(event, **changes)


def raw(event: Event, **changes: Any) -> bytes:
    obj = dict(zip(FIELDS, dataclasses.astuple(event), strict=True))
    obj.update(changes)
    return json.dumps(obj).encode()


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_every_event_type_round_trips(event_type: str) -> None:
    event = make(event_type)
    assert decode(encode(event)) == event


def test_the_wire_form_is_compact_json_in_field_order() -> None:
    event = make("page_view")
    assert encode(event) == (
        b'{"schema_version":1,"event_id":"0f8fad5b-d9cb-469f-a165-70867728950e","event_type":"page_view",'
        b'"event_time":1767225600000,"user_id":"u42","session_id":"s7","device":"mobile","country":"US",'
        b'"page":"/","product_id":null,"category":null,"price_cents":null,"quantity":null}'
    )


def test_revenue_is_price_times_quantity_for_purchases_only() -> None:
    assert make("purchase", price_cents=1999, quantity=3).revenue_cents() == 5997
    assert make("add_to_cart").revenue_cents() == 0
    assert make("page_view").revenue_cents() == 0


def test_key_order_on_the_wire_does_not_matter_to_the_decoder() -> None:
    event = make()
    obj = dict(reversed(list(zip(FIELDS, dataclasses.astuple(event), strict=True))))
    assert decode(json.dumps(obj).encode()) == event


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (b"\xff\xfe", "not UTF-8"),
        (b"{", "not JSON"),
        (b"[]", "not a JSON object"),
        (b'"text"', "not a JSON object"),
        (b"null", "not a JSON object"),
        (b"[" * 100_000, "not JSON"),
    ],
)
def test_records_that_are_not_json_objects_are_refused(value: bytes, reason: str) -> None:
    with pytest.raises(InvalidEvent, match=reason):
        decode(value)


def test_a_missing_field_is_refused_and_named() -> None:
    obj = json.loads(encode(make()))
    del obj["country"]
    with pytest.raises(InvalidEvent, match="missing field country"):
        decode(json.dumps(obj).encode())


def test_a_null_field_is_not_a_missing_one_but_still_checked() -> None:
    with pytest.raises(InvalidEvent, match="bad country"):
        decode(raw(make(), country=None))


def test_an_unknown_field_is_refused_in_version_1() -> None:
    with pytest.raises(InvalidEvent, match="unknown field referrer"):
        decode(raw(make(), referrer="/home"))


def test_a_duplicate_key_is_refused_rather_than_last_one_wins() -> None:
    value = encode(make()).replace(b'"country":"US"', b'"country":"US","country":"FR"')
    with pytest.raises(InvalidEvent, match="duplicate field country"):
        decode(value)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_non_json_constants_are_refused(constant: str) -> None:
    value = encode(make()).replace(b'"price_cents":1999', f'"price_cents":{constant}'.encode())
    with pytest.raises(InvalidEvent, match="not JSON"):
        decode(value)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"schema_version": 2}, "schema_version"),
        ({"schema_version": True}, "schema_version"),
        ({"schema_version": 1.0}, "schema_version"),
        ({"event_id": "0F8FAD5B-D9CB-469F-A165-70867728950E"}, "bad event_id"),
        ({"event_id": "0f8fad5bd9cb469fa16570867728950e"}, "bad event_id"),
        ({"event_type": "click"}, "unknown event_type"),
        ({"event_type": ["purchase"]}, "unknown event_type"),
        ({"event_time": MIN_EVENT_TIME - 1}, "event_time out of range"),
        ({"event_time": MAX_EVENT_TIME}, "event_time out of range"),
        ({"event_time": 1_767_225_600_000.0}, "event_time out of range"),
        ({"event_time": "1767225600000"}, "event_time out of range"),
        ({"user_id": "42"}, "bad user_id"),
        ({"user_id": "u" + "1" * 32}, "bad user_id"),
        ({"user_id": "u٤٢"}, "bad user_id"),  # Arabic-Indic digits match \d, not [0-9]
        ({"session_id": "u7"}, "bad session_id"),
        ({"device": "watch"}, "unknown device"),
        ({"country": "us"}, "bad country"),
        ({"country": "USA"}, "bad country"),
        ({"page": "p/p17"}, "bad page"),
        ({"page": "/a b"}, "bad page"),
        ({"page": "/" + "a" * 256}, "bad page"),
        ({"page": "/café"}, "bad page"),
        ({"product_id": "17"}, "bad product_id"),
        ({"category": "Home"}, "bad category"),
        ({"category": ""}, "bad category"),
        ({"price_cents": 0}, "price_cents out of range"),
        ({"price_cents": 10_000_001}, "price_cents out of range"),
        ({"price_cents": True}, "price_cents out of range"),
        ({"quantity": 0}, "quantity out of range"),
        ({"quantity": 100}, "quantity out of range"),
        ({"quantity": 2.0}, "quantity out of range"),
    ],
)
def test_each_rule_refuses_a_bad_value(changes: dict[str, Any], reason: str) -> None:
    with pytest.raises(InvalidEvent, match=reason):
        decode(raw(make(), **changes))


def test_the_longest_page_and_ids_are_accepted() -> None:
    event = make(page="/" + "~" * 255, user_id="u" + "9" * 31, category="z" * 32)
    assert decode(encode(event)) == event


@pytest.mark.parametrize("event_type", sorted(set(EVENT_TYPES) - PRODUCT_TYPES))
@pytest.mark.parametrize("name", ["product_id", "category", "price_cents"])
def test_product_fields_must_be_null_for_non_product_events(event_type: str, name: str) -> None:
    value = make("purchase").__getattribute__(name)
    with pytest.raises(InvalidEvent, match=f"{name} must be null for {event_type}"):
        decode(raw(make(event_type), **{name: value}))


@pytest.mark.parametrize("event_type", sorted(PRODUCT_TYPES))
@pytest.mark.parametrize("name", ["product_id", "category", "price_cents"])
def test_product_fields_must_be_set_for_product_events(event_type: str, name: str) -> None:
    with pytest.raises(InvalidEvent, match=f"{name} must be set for {event_type}"):
        decode(raw(make(event_type), **{name: None}))


def test_quantity_must_be_null_on_a_product_view_and_set_on_cart_events() -> None:
    with pytest.raises(InvalidEvent, match="quantity must be null for product_view"):
        decode(raw(make("product_view"), quantity=1))
    for event_type in sorted(QUANTITY_TYPES):
        with pytest.raises(InvalidEvent, match=f"quantity must be set for {event_type}"):
            decode(raw(make(event_type), quantity=None))


def test_encode_refuses_an_invalid_event() -> None:
    with pytest.raises(InvalidEvent, match="quantity out of range"):
        encode(make(quantity=0))


events = st.builds(
    lambda event_type, t, price, quantity, page: make(
        event_type,
        event_time=t,
        page=page,
        **({"price_cents": price} if event_type in PRODUCT_TYPES else {}),
        **({"quantity": quantity} if event_type in QUANTITY_TYPES else {}),
    ),
    st.sampled_from(EVENT_TYPES),
    st.integers(MIN_EVENT_TIME, MAX_EVENT_TIME - 1),
    st.integers(1, 10_000_000),
    st.integers(1, 99),
    st.text(st.characters(min_codepoint=0x21, max_codepoint=0x7E), max_size=255).map(lambda s: "/" + s),
)


@given(events)
def test_any_valid_event_round_trips_byte_for_byte(event: Event) -> None:
    value = encode(event)
    assert decode(value) == event
    assert encode(decode(value)) == value

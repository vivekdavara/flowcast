import hashlib
from collections import defaultdict
from itertools import islice, takewhile

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from flowcast.generator import (
    CAMPAIGNS,
    DEFAULT_START_MS,
    NETWORK_MAX_MS,
    NETWORK_MIN_MS,
    RETRY_MAX_MS,
    RETRY_MIN_MS,
    Config,
    Emitted,
    Generator,
    generate,
    sessions,
    take,
)
from flowcast.schema import decode, encode

# The SHA-256 of the first 10,000 records of Config() as "<arrival_ms>\t<event bytes>\n" lines. A deliberate change to
# the generator re-pins it in the same commit; anything else that changes it is a bug (or a platform difference).
PINNED_10K = "4087a8d31ef8ba68be98fb587028b547a05c4ac1a6006c2519ead33d355457b7"


def digest(records: list[Emitted]) -> str:
    h = hashlib.sha256()
    for record in records:
        h.update(f"{record.arrival_ms}\t".encode() + encode(record.event) + b"\n")
    return h.hexdigest()


@pytest.fixture(scope="module")
def sample() -> list[Emitted]:
    """A prefix of the endless stream, as a collector would have it: late records of its last minutes are missing."""
    return take(50_000)


@pytest.fixture(scope="module")
def complete() -> list[Emitted]:
    """Every record of 10,000 whole sessions."""
    return sessions(10_000)


def test_the_first_10k_records_match_the_pinned_hash(sample: list[Emitted]) -> None:
    assert digest(sample[:10_000]) == PINNED_10K


def test_the_same_config_gives_the_same_stream() -> None:
    assert digest(take(3_000, Config(seed=7))) == digest(take(3_000, Config(seed=7)))


def test_a_different_seed_gives_a_different_stream() -> None:
    assert digest(take(100, Config(seed=7))) != digest(take(100, Config(seed=8)))


def test_every_record_is_a_valid_event_that_round_trips(sample: list[Emitted]) -> None:
    for record in sample:
        assert decode(encode(record.event)) == record.event


def test_records_come_out_in_arrival_order(sample: list[Emitted]) -> None:
    arrivals = [record.arrival_ms for record in sample]
    assert arrivals == sorted(arrivals)


def test_arrival_delays_match_their_labels(sample: list[Emitted]) -> None:
    config = Config()
    for record in sample:
        if record.duplicate:
            continue
        delay = record.arrival_ms - record.event.event_time
        if record.delay == "on_time":
            assert NETWORK_MIN_MS <= delay <= NETWORK_MAX_MS
        elif record.delay == "late":
            assert NETWORK_MIN_MS <= delay <= NETWORK_MAX_MS + config.late_cap_ms
        else:
            low = NETWORK_MIN_MS + config.very_late_min_ms
            assert low <= delay <= NETWORK_MAX_MS + config.very_late_max_ms


def test_a_duplicate_is_the_same_bytes_sent_again_shortly_after_the_original(sample: list[Emitted]) -> None:
    originals: dict[str, Emitted] = {}
    duplicates = 0
    for record in sample:
        if not record.duplicate:
            assert record.event.event_id not in originals, "event ids are unique among originals"
            originals[record.event.event_id] = record
            continue
        duplicates += 1
        original = originals[record.event.event_id]  # the original was emitted first
        assert encode(record.event) == encode(original.event)
        assert RETRY_MIN_MS <= record.arrival_ms - original.arrival_ms <= RETRY_MAX_MS
        assert record.delay == original.delay
    assert duplicates > 0


def test_the_disorder_rates_are_close_to_the_config(complete: list[Emitted]) -> None:
    # Measured on whole sessions: a prefix in arrival order would miss the very late records still on their way.
    # A fixed seed, so this can't flake; the tolerances are about three standard deviations at this size.
    originals = [record for record in complete if not record.duplicate]
    n = len(originals)
    late = sum(record.delay == "late" for record in originals) / n
    very_late = sum(record.delay == "very_late" for record in originals) / n
    duplicates = (len(complete) - n) / n
    assert abs(late - 0.03) < 0.003
    assert abs(very_late - 0.002) < 0.0007
    assert abs(duplicates - 0.01) < 0.0015


def check_sessions(records: list[Emitted]) -> None:
    """The walk's rules, rebuilt from the events of each session in event-time order."""
    sessions: dict[str, list[Emitted]] = defaultdict(list)
    for record in records:
        if not record.duplicate:
            sessions[record.event.session_id].append(record)
    for events in sessions.values():
        events.sort(key=lambda r: (r.event.event_time, r.event.event_id))
        first = events[0].event
        assert first.event_type == "page_view"
        cart: dict[str, int] = {}
        checked_out = purchasing = False
        for record in events:
            event = record.event
            assert (event.user_id, event.device, event.country) == (first.user_id, first.device, first.country)
            if event.event_type == "add_to_cart":
                assert event.quantity is not None and event.product_id is not None
                cart[event.product_id] = cart.get(event.product_id, 0) + event.quantity
            elif event.event_type == "remove_from_cart":
                assert event.product_id is not None
                assert cart.pop(event.product_id) == event.quantity
            elif event.event_type == "checkout":
                assert cart, "checkout needs a non-empty cart"
                checked_out = True
            elif event.event_type == "purchase":
                assert checked_out and event.product_id is not None
                assert cart.pop(event.product_id) == event.quantity
                purchasing = True
                continue
            if purchasing:
                assert not cart, "a purchase buys every cart line"
                purchasing = checked_out = False
        if purchasing:
            assert not cart, "a purchase buys every cart line"


def test_sessions_follow_the_walks_rules(complete: list[Emitted]) -> None:
    check_sessions(complete)


def test_a_finite_stream_is_complete_and_in_arrival_order(complete: list[Emitted]) -> None:
    assert len({record.event.session_id for record in complete}) == 10_000
    arrivals = [record.arrival_ms for record in complete]
    assert arrivals == sorted(arrivals)
    # It is the endless stream's records of those sessions, all of them: read the endless stream up to the last
    # arrival and keep the records of the same sessions; in the same order, too.
    last = arrivals[-1]
    ids = {record.event.event_id for record in complete}
    endless = takewhile(lambda record: record.arrival_ms <= last, generate())
    assert [record for record in endless if record.event.event_id in ids] == complete


def test_zero_sessions_is_an_empty_stream() -> None:
    assert sessions(0) == []
    with pytest.raises(ValueError, match="sessions"):
        Generator(Config(), sessions=-1)


def test_one_users_events_keep_their_device_and_country_across_sessions(sample: list[Emitted]) -> None:
    seen: dict[str, tuple[str, str]] = {}
    for record in sample:
        event = record.event
        assert seen.setdefault(event.user_id, (event.device, event.country)) == (event.device, event.country)


def test_memory_stays_bounded_while_streaming() -> None:
    generator = Generator(Config())
    stream = iter(generator)
    peak = 0
    for _ in islice(stream, 100_000):
        peak = max(peak, generator.pending)
    # About 4 events a session at 20 sessions/s; very late records wait up to 30 minutes, so the heap holds about
    # 0.2% of 30 minutes of events plus the sessions in flight. Measured at the defaults: a peak of 1,855.
    assert peak < 5_000, peak


@settings(max_examples=40, deadline=None)
@given(
    seed=st.integers(0, 2**32),
    users=st.integers(1, 50),
    products=st.integers(1, 20),
    late=st.floats(0, 0.5),
    very_late=st.floats(0, 0.5),
    duplicates=st.floats(0, 1),
    rate=st.floats(0.01, 1_000),
)
def test_invariants_hold_for_any_config(
    seed: int, users: int, products: int, late: float, very_late: float, duplicates: float, rate: float
) -> None:
    config = Config(
        seed=seed,
        users=users,
        products=products,
        late_fraction=late,
        very_late_fraction=very_late,
        duplicate_fraction=duplicates,
        sessions_per_second=rate,
    )
    records = sessions(200, config)
    arrivals = [record.arrival_ms for record in records]
    assert arrivals == sorted(arrivals)
    for record in records:
        assert decode(encode(record.event)) == record.event
        assert record.arrival_ms - record.event.event_time >= NETWORK_MIN_MS
    check_sessions(records)


def test_no_disorder_means_every_record_is_on_time_and_unique() -> None:
    config = Config(late_fraction=0, very_late_fraction=0, duplicate_fraction=0)
    records = take(5_000, config)
    assert all(record.delay == "on_time" and not record.duplicate for record in records)
    assert len({record.event.event_id for record in records}) == len(records)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"users": 0}, "users and products"),
        ({"products": 0}, "users and products"),
        ({"sessions_per_second": 0}, "sessions_per_second"),
        ({"think_mean_ms": 0}, "think_mean_ms"),
        ({"late_fraction": -0.1}, "late_fraction"),
        ({"duplicate_fraction": 1.5}, "duplicate_fraction"),
        ({"late_fraction": 0.6, "very_late_fraction": 0.6}, "at most 1"),
        ({"late_mean_ms": 200_000}, "late_mean_ms"),
        ({"very_late_min_ms": 2_000_000}, "very_late_min_ms"),
        ({"max_session_events": 0}, "max_session_events"),
        ({"start_ms": 0}, "start_ms"),
        ({"start_ms": 4_070_908_800_001}, "start_ms"),  # a millisecond after 2099-01-01
        ({"seed": -1}, "seed"),
    ],
)
def test_bad_configs_are_refused(changes: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Config(**changes)  # type: ignore[arg-type]


def test_an_add_to_cart_stops_at_the_quantity_limit() -> None:
    # Reaching 99 of one product in a session is too rare to wait for, so drive one step of the walk directly.
    from flowcast.generator import _Session

    generator = Generator(Config(products=1, duplicate_fraction=0))
    product = generator._products[0]
    session = _Session("s0", generator._users[0], DEFAULT_START_MS, product=product)
    session.cart[product.product_id] = (product, 98)
    assert generator._step(session, "add_to_cart") == 1  # adds only the one that fits
    assert session.cart[product.product_id][1] == 99
    assert generator._step(session, "add_to_cart") == 0  # full: no event at all
    assert session.cart[product.product_id][1] == 99
    emitted = sorted(record.event.quantity for _, _, record in generator._pending)
    assert emitted == [1]


def test_a_weighted_draw_at_the_top_of_the_range_takes_the_last_option() -> None:
    # random() is below 1, but the running subtraction can round to exactly 0 at the top: the last option, not None.
    from flowcast.generator import _Draw

    draw = _Draw(0)
    draw._rng.random = lambda: 1.0  # type: ignore[method-assign]
    assert draw.weighted((("a", 0.5), ("b", 0.5))) == "b"
    assert draw.below(10) == 9
    assert draw.skewed(10) == 9


def test_the_latest_start_is_accepted_and_a_stream_past_2100_stops_with_an_error() -> None:
    from flowcast.generator import LATEST_START_MS

    assert sessions(5, Config(start_ms=LATEST_START_MS))
    generator = Generator(Config(start_ms=LATEST_START_MS))
    generator._clock = 365 * 24 * 3600 * 1000 - 10_000.0  # ten seconds of event time before 2100
    with pytest.raises(OverflowError, match="2100-01-01"):
        for record in generator:
            encode(record.event)  # every record before the error is valid


def test_an_event_at_exactly_2100_is_refused_too() -> None:
    # The schema's limit is exclusive (event_time < 2100-01-01): the second review's mutant (>= to >) survived.
    from flowcast.generator import _Session
    from flowcast.schema import MAX_EVENT_TIME

    generator = Generator(Config())
    session = _Session("s0", generator._users[0], MAX_EVENT_TIME)
    with pytest.raises(OverflowError, match="2100-01-01"):
        generator._emit(session, "page_view", "/", None, None)
    session.time_ms = MAX_EVENT_TIME - 1
    generator._emit(session, "page_view", "/", None, None)
    assert encode(generator._pending[0][2].event)


def test_version_2_gives_each_session_one_campaign_and_round_trips() -> None:
    records = sessions(2_000, Config(seed=5, schema_version=2))
    by_session: dict[str, set[str | None]] = {}
    for record in records:
        assert record.event.schema_version == 2
        assert decode(encode(record.event)) == record.event
        by_session.setdefault(record.event.session_id, set()).add(record.event.campaign)
    assert all(len(campaigns) == 1 for campaigns in by_session.values())
    drawn = [next(iter(c)) for c in by_session.values()]
    assert set(drawn) == {c for c, _ in CAMPAIGNS}
    # 60% of sessions have no campaign: 2,000 draws put the share within a few points of it.
    assert 0.55 < drawn.count(None) / len(drawn) < 0.65


def test_version_2_changes_only_what_comes_after_the_campaign_draws() -> None:
    # Same users and catalog (drawn before any session), the same first session start; the extra draw per session
    # moves the rest, so the version-1 stream itself is untouched (the pinned hash) rather than reproduced.
    v1 = sessions(1, Config(seed=9))
    v2 = sessions(1, Config(seed=9, schema_version=2))
    assert v1[0].event.user_id == v2[0].event.user_id and v1[0].event.event_time == v2[0].event.event_time
    with pytest.raises(ValueError, match="schema_version must be 1 or 2"):
        Config(schema_version=3)

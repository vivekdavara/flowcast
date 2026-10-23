"""Review of 943596b: generator.py's Config and session walk."""

from collections import Counter

import pytest

from flowcast.generator import Config, sessions
from flowcast.schema import MAX_EVENT_TIME, InvalidEvent, encode


def test_max_session_events_is_a_cap_on_a_sessions_events() -> None:
    # _session stops starting steps once `emitted >= max_session_events`, but a purchase step emits one event per cart
    # line, so a session that reaches checkout one event below the cap overshoots it (seen: 8 events with a cap of 7).
    cap = 7
    records = sessions(20_000, Config(max_session_events=cap))
    per_session = Counter(r.event.session_id for r in records if not r.duplicate)
    assert max(per_session.values()) <= cap


def test_a_config_it_accepts_never_makes_invalid_events() -> None:
    # Config accepts any start_ms < 2100-01-01, but events after the first are later than start_ms, so a start near
    # the limit yields events encode() and decode() refuse (11 of the first 13 records here).
    try:
        config = Config(start_ms=MAX_EVENT_TIME - 1)
    except ValueError:
        return  # refusing the setting is a fix too
    for record in sessions(3, config):
        try:
            encode(record.event)
        except InvalidEvent as exc:
            pytest.fail(f"generated an invalid event: {exc}")

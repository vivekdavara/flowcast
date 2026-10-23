"""A seeded, deterministic clickstream in arrival order, with late events and duplicates made on purpose.

DESIGN.md "The generator" is the specification. In short: sessions start as a Poisson process in event time, each walks
a Markov chain over event types, each event gets an arrival time (a small network delay, sometimes much more), some
events are sent twice, and the stream comes out sorted by arrival time with bounded memory.

Every random draw goes through `_Draw`, which uses only `random.Random.random()`: that is the one method whose sequence
Python promises to keep across versions for the same seed.
"""

from __future__ import annotations

import hashlib
import heapq
import math
import random
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from itertools import islice
from typing import Literal, TypeVar

from flowcast.schema import MAX_EVENT_TIME, MAX_QUANTITY, MIN_EVENT_TIME, Event

DEFAULT_START_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
# A year before the schema's limit, so a stream has a year of event time before an event would pass 2100-01-01 (the
# generator stops with an error if it gets there rather than make an invalid event).
LATEST_START_MS = MAX_EVENT_TIME - 365 * 24 * 3600 * 1000

CATEGORIES = ("books", "electronics", "home_garden", "toys", "clothing", "sports", "beauty", "grocery", "music", "pets")
# A category's typical price in cents; each product's price is spread around it.
BASE_PRICE_CENTS = (1800, 24900, 4500, 2900, 3900, 5900, 2400, 900, 1500, 3200)
COUNTRIES: tuple[tuple[str, float], ...] = (
    ("US", 0.35),
    ("IN", 0.15),
    ("GB", 0.1),
    ("DE", 0.08),
    ("BR", 0.08),
    ("CA", 0.07),
    ("FR", 0.06),
    ("JP", 0.05),
    ("AU", 0.03),
    ("MX", 0.03),
)
DEVICES = (("mobile", 0.6), ("desktop", 0.32), ("tablet", 0.08))
LANDING_PAGES = ("/", "/search", "/deals")

# Network delay of every event, in ms; late and very late events add to it.
NETWORK_MIN_MS = 5
NETWORK_MAX_MS = 250
# A duplicate is the same bytes sent again this long after the original (a client retry).
RETRY_MIN_MS = 100
RETRY_MAX_MS = 5_000

Delay = Literal["on_time", "late", "very_late"]
T = TypeVar("T")


@dataclass(frozen=True)
class Config:
    seed: int = 1
    start_ms: int = DEFAULT_START_MS
    users: int = 10_000
    products: int = 500
    sessions_per_second: float = 20.0
    think_mean_ms: float = 6_000.0
    late_fraction: float = 0.03
    late_mean_ms: float = 20_000.0
    late_cap_ms: int = 120_000
    very_late_fraction: float = 0.002
    very_late_min_ms: int = 300_000
    very_late_max_ms: int = 1_800_000
    duplicate_fraction: float = 0.01
    max_session_events: int = 60

    def __post_init__(self) -> None:
        if self.seed < 0:
            # random.Random uses a seed's absolute value, so -n would repeat n's stream under other event ids.
            raise ValueError("seed must not be negative")
        if not MIN_EVENT_TIME <= self.start_ms <= LATEST_START_MS:
            raise ValueError("start_ms must be between 2000-01-01 and 2099-01-01")
        if self.users < 1 or self.products < 1:
            raise ValueError("users and products must be at least 1")
        if not 0 < self.sessions_per_second <= 1_000_000:
            raise ValueError("sessions_per_second must be in (0, 1,000,000]")
        if not 0 < self.think_mean_ms <= 3_600_000:
            raise ValueError("think_mean_ms must be in (0, 3,600,000]")
        for name in ("late_fraction", "very_late_fraction", "duplicate_fraction"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.late_fraction + self.very_late_fraction > 1.0:
            raise ValueError("late_fraction + very_late_fraction must be at most 1")
        if not 0 < self.late_mean_ms <= self.late_cap_ms:
            raise ValueError("late_mean_ms must be positive and at most late_cap_ms")
        if not 0 <= self.very_late_min_ms <= self.very_late_max_ms:
            raise ValueError("very_late_min_ms must be between 0 and very_late_max_ms")
        if self.max_session_events < 1:
            raise ValueError("max_session_events must be at least 1")


@dataclass(frozen=True, slots=True)
class Emitted:
    """One record as the collector receives it. Only `event` goes to Kafka; the rest is for tests and the truth."""

    arrival_ms: int
    event: Event
    delay: Delay
    duplicate: bool


class _Draw:
    """Random draws built on `random()` alone, so a seed means the same stream on every Python version."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def uniform(self) -> float:
        return self._rng.random()

    def below(self, n: int) -> int:
        return min(int(self._rng.random() * n), n - 1)

    def between(self, low: int, high: int) -> int:
        """An integer in [low, high]."""
        return low + self.below(high - low + 1)

    def chance(self, p: float) -> bool:
        return self._rng.random() < p

    def exponential(self, mean: float) -> float:
        return -mean * math.log(1.0 - self._rng.random())

    def skewed(self, n: int) -> int:
        """An index in [0, n) where low indexes are much more likely: P(index < k) = sqrt(k / n)."""
        u = self._rng.random()
        return min(int(n * u * u), n - 1)

    def weighted(self, options: Sequence[tuple[T, float]]) -> T:
        u = self._rng.random() * sum(weight for _, weight in options)
        for value, weight in options:
            u -= weight
            if u < 0:
                return value
        return options[-1][0]


@dataclass(frozen=True, slots=True)
class _Product:
    product_id: str
    category: str
    price_cents: int


@dataclass(frozen=True, slots=True)
class _User:
    user_id: str
    device: str
    country: str


@dataclass
class _Session:
    """What one session's walk needs to remember."""

    session_id: str
    user: _User
    time_ms: int
    product: _Product | None = None
    cart: dict[str, tuple[_Product, int]] = field(default_factory=dict)


# The Markov chain: from the last event type, the next one (None ends the session). Weights add up to 1.
TRANSITIONS: dict[str, tuple[tuple[str | None, float], ...]] = {
    "page_view": (("product_view", 0.55), ("page_view", 0.2), (None, 0.25)),
    "product_view": (("add_to_cart", 0.25), ("product_view", 0.35), ("page_view", 0.15), (None, 0.25)),
    "add_to_cart": (("checkout", 0.35), ("product_view", 0.4), ("remove_from_cart", 0.05), (None, 0.2)),
    "remove_from_cart": (("product_view", 0.5), ("checkout", 0.2), (None, 0.3)),
    "checkout": (("purchase", 0.7), ("page_view", 0.1), (None, 0.2)),
    "purchase": (("page_view", 0.3), (None, 0.7)),
}
# After a removal that empties the cart, checkout isn't possible.
EMPTY_CART_AFTER_REMOVE: tuple[tuple[str | None, float], ...] = (("product_view", 0.6), (None, 0.4))


class Generator:
    """The stream for one Config. Iterate it for `Emitted` records in arrival order.

    With `sessions=None` it never ends. With a number, it generates that many whole sessions and ends once every
    record of them has arrived (very late ones included), so the stream is complete: nothing generated is left out.
    """

    def __init__(self, config: Config, sessions: int | None = None) -> None:
        if sessions is not None and sessions < 0:
            raise ValueError("sessions must not be negative")
        self.config = config
        self._limit = sessions
        self._draw = _Draw(config.seed)
        self._products = [self._product(i) for i in range(config.products)]
        self._users = [self._user(i) for i in range(config.users)]
        self._clock = 0.0  # ms after start_ms at which the next session starts
        self._sessions = 0
        self._events = 0  # generated events, duplicates not counted; numbers event ids
        self._pushed = 0  # tie-breaker for equal arrival times: the order records were made in
        self._pending: list[tuple[int, int, Emitted]] = []

    @property
    def pending(self) -> int:
        """Records generated but not yet emitted (the memory the stream holds)."""
        return len(self._pending)

    def __iter__(self) -> Iterator[Emitted]:
        while self._limit is None or self._sessions < self._limit:
            start = self.config.start_ms + int(self._clock)
            # Every event of this session or a later one happens at or after `start` and arrives at least
            # NETWORK_MIN_MS later, so anything arriving by `start` can go out now without breaking the order.
            while self._pending and self._pending[0][0] <= start:
                yield heapq.heappop(self._pending)[2]
            self._session(start)
            self._clock += self._draw.exponential(1000.0 / self.config.sessions_per_second)
        while self._pending:
            yield heapq.heappop(self._pending)[2]

    def _product(self, index: int) -> _Product:
        category = index % len(CATEGORIES)
        spread = 0.5 + 1.5 * self._draw.uniform() ** 2
        return _Product(f"p{index}", CATEGORIES[category], max(1, round(BASE_PRICE_CENTS[category] * spread)))

    def _user(self, index: int) -> _User:
        return _User(f"u{index}", self._draw.weighted(DEVICES), self._draw.weighted(COUNTRIES))

    def _session(self, start: int) -> None:
        session = _Session(f"s{self._sessions}", self._users[self._draw.skewed(len(self._users))], start)
        self._sessions += 1
        event_type: str | None = "page_view"
        emitted = 0
        while event_type is not None:
            # A purchase writes one event per cart line, so its size is known before the step: a session ends
            # rather than take a step that would put it past max_session_events.
            size = len(session.cart) if event_type == "purchase" else 1
            if emitted + size > self.config.max_session_events:
                break
            emitted += self._step(session, event_type)
            if event_type == "remove_from_cart" and not session.cart:
                event_type = self._draw.weighted(EMPTY_CART_AFTER_REMOVE)
            else:
                event_type = self._draw.weighted(TRANSITIONS[event_type])
            session.time_ms += max(1, int(self._draw.exponential(self.config.think_mean_ms)))

    def _step(self, session: _Session, event_type: str) -> int:
        """Emit the events of one step of the walk; returns how many."""
        if event_type == "page_view":
            page = self._draw.weighted([(p, 1.0) for p in LANDING_PAGES] + [(f"/c/{c}", 0.3) for c in CATEGORIES])
            self._emit(session, event_type, page, None, None)
        elif event_type == "product_view":
            session.product = self._products[self._draw.skewed(len(self._products))]
            self._emit(session, event_type, f"/p/{session.product.product_id}", session.product, None)
        elif event_type == "add_to_cart":
            assert session.product is not None
            quantity = self._draw.weighted(((1, 0.8), (2, 0.15), (3, 0.05)))
            product = session.product
            held = session.cart.get(product.product_id, (product, 0))[1]
            quantity = min(quantity, MAX_QUANTITY - held)
            if quantity == 0:
                return 0
            session.cart[product.product_id] = (product, held + quantity)
            self._emit(session, event_type, f"/p/{product.product_id}", product, quantity)
        elif event_type == "remove_from_cart":
            product_id = list(session.cart)[self._draw.below(len(session.cart))]
            product, quantity = session.cart.pop(product_id)
            self._emit(session, event_type, "/cart", product, quantity)
        elif event_type == "checkout":
            self._emit(session, event_type, "/checkout", None, None)
        else:  # purchase: one event per cart line, a millisecond apart, and the cart is empty after
            lines = list(session.cart.values())
            session.cart.clear()
            for i, (product, quantity) in enumerate(lines):
                if i:
                    session.time_ms += 1
                self._emit(session, "purchase", "/checkout/complete", product, quantity)
            return len(lines)
        return 1

    def _emit(
        self, session: _Session, event_type: str, page: str, product: _Product | None, quantity: int | None
    ) -> None:
        config = self.config
        if session.time_ms >= MAX_EVENT_TIME:
            raise OverflowError("the stream reached 2100-01-01, the schema's limit for event_time")
        digest = hashlib.blake2b(f"{config.seed}:{self._events}".encode(), digest_size=16).digest()
        self._events += 1
        event = Event(
            schema_version=1,
            event_id=str(uuid.UUID(bytes=digest, version=4)),
            event_type=event_type,
            event_time=session.time_ms,
            user_id=session.user.user_id,
            session_id=session.session_id,
            device=session.user.device,
            country=session.user.country,
            page=page,
            product_id=product.product_id if product else None,
            category=product.category if product else None,
            price_cents=product.price_cents if product else None,
            quantity=quantity,
        )
        arrival = event.event_time + self._draw.between(NETWORK_MIN_MS, NETWORK_MAX_MS)
        u = self._draw.uniform()
        delay: Delay = "on_time"
        if u < config.very_late_fraction:
            delay = "very_late"
            arrival += self._draw.between(config.very_late_min_ms, config.very_late_max_ms)
        elif u < config.very_late_fraction + config.late_fraction:
            delay = "late"
            arrival += min(config.late_cap_ms, int(self._draw.exponential(config.late_mean_ms)))
        self._push(Emitted(arrival, event, delay, duplicate=False))
        if self._draw.chance(config.duplicate_fraction):
            self._push(Emitted(arrival + self._draw.between(RETRY_MIN_MS, RETRY_MAX_MS), event, delay, duplicate=True))

    def _push(self, record: Emitted) -> None:
        heapq.heappush(self._pending, (record.arrival_ms, self._pushed, record))
        self._pushed += 1


def generate(config: Config | None = None) -> Iterator[Emitted]:
    """The endless stream for `config` (the defaults if None)."""
    return iter(Generator(config or Config()))


def take(n: int, config: Config | None = None) -> list[Emitted]:
    """The first `n` records of the endless stream: what a collector had received at that point."""
    return list(islice(generate(config), n))


def sessions(n: int, config: Config | None = None) -> list[Emitted]:
    """Every record of the first `n` sessions, in arrival order."""
    return list(Generator(config or Config(), sessions=n))

from __future__ import annotations

from decimal import Decimal

import pytest

from avee.astra import (
    DEFAULT_BASE_URL,
    FEED_IDS_PER_REQUEST,
    KNOWN_STATUSES,
    MAX_EXPO,
    MAX_HISTORICAL_FEEDS,
    MAX_IDS_PER_REQUEST,
    MAX_IDS_PER_URL,
    MAX_INTERVAL_SECONDS,
    MAX_SCALE_DECIMALS,
    MIN_EXPO,
    AstraClient,
    AstraConnectionError,
    AstraError,
    AstraHTTPError,
    AstraSubscriptionError,
    AstraTimeoutError,
    AstraValidationError,
    AsyncAstraClient,
    Candle,
    Channel,
    ConnectionState,
    Feed,
    FeedHealth,
    FeedIdEntry,
    FeedIdMap,
    FeedMetadata,
    LiveValue,
    Price,
    PriceUpdate,
    Problem,
    StatusReport,
    Subscription,
    SubscriptionStats,
    UpdateMetadata,
    normalize_feed_id,
)
from tests.astra.conftest import BTC, BTC_ASTRA, ETH, FakeAstra

from .conftest import UNKNOWN

TYPES = (Candle, Channel, ConnectionState, Feed, FeedHealth, FeedIdEntry, FeedIdMap, FeedMetadata, LiveValue, PriceUpdate, Problem, StatusReport, SubscriptionStats, UpdateMetadata)
CONSTANTS = (FEED_IDS_PER_REQUEST, MAX_EXPO, MAX_HISTORICAL_FEEDS, MAX_IDS_PER_REQUEST, MAX_IDS_PER_URL, MAX_INTERVAL_SECONDS, MAX_SCALE_DECIMALS, MIN_EXPO)
ERRORS = (AstraConnectionError, AstraHTTPError, AstraSubscriptionError, AstraTimeoutError, AstraValidationError)


def client(astra: FakeAstra) -> AstraClient:
    return AstraClient(astra.url, timeout=5.0, max_retries=1, max_retry_delay=0.05, max_response_bytes=1 << 20, headers={"x-consumer": "compat-v0.1.0"})


def test_exports_and_error_hierarchy() -> None:
    assert isinstance(DEFAULT_BASE_URL, str)
    assert all(isinstance(n, int) for n in CONSTANTS)
    assert all(issubclass(e, AstraError) for e in ERRORS)
    assert issubclass(AstraValidationError, ValueError)
    assert "trading" in KNOWN_STATUSES
    assert len(TYPES) == 14


def test_hermes_rest(astra: FakeAstra) -> None:
    with client(astra) as c:
        metas: list[FeedMetadata] = c.price_feeds(query="btc", asset_type="crypto")
        assert metas[0].symbol == "Crypto.BTC/USD" and metas[0].astra_id == BTC_ASTRA
        meta = c.price_feed("0x" + BTC)
        assert meta.display_symbol == "BTC/USD" and meta.market_open

        latest: list[PriceUpdate] = c.latest_prices(["0x" + BTC, ETH], ignore_invalid=True)
        assert [u.id for u in latest] == [BTC, ETH]
        assert latest[0].price.to_decimal() == Decimal("65000")
        assert latest[0].metadata is not None and latest[0].metadata.prev_publish_time == 1699999999
        assert len(c.prices_at(1700000000, [BTC])) == 1
        assert len(c.prices_in_interval(1700000000, MAX_INTERVAL_SECONDS, [BTC], unique=False)) == 1


def test_native_routes_feed_ids_and_status(astra: FakeAstra) -> None:
    with client(astra) as c:
        feeds: list[Feed] = c.feeds(category="crypto")
        assert feeds[0].live.status == "trading" and feeds[0].live.price is not None
        candles: list[Candle] = c.candles("Crypto.BTC/USD", "60", 0, 60)
        assert candles[0].high == 2

        ids: FeedIdMap = c.feed_ids(pyth_ids=[BTC, UNKNOWN])
        assert ids.items[0].astra_id == BTC_ASTRA and ids.missing == [UNKNOWN]
        report: StatusReport = c.status()
        assert report.ready and not report.feeds[0].stale
        assert c.status(feed=BTC_ASTRA).feeds[0].stale


def test_typed_errors(astra: FakeAstra) -> None:
    with client(astra) as c:
        with pytest.raises(AstraHTTPError) as info:
            c.price_feed(UNKNOWN)
        assert info.value.status == 404 and not info.value.retryable
        with pytest.raises(AstraValidationError):
            c.latest_prices(["not-a-feed-id"])
    assert normalize_feed_id("0x" + BTC.upper()) == BTC
    with pytest.raises(ValueError):
        normalize_feed_id("nope")


def test_price_helpers() -> None:
    p = Price("6500012345678", "1000", -8, 1)
    assert p.to_decimal() == Decimal("65000.12345678")
    assert p.to_float() == 65000.12345678
    assert p.conf_to_decimal() == Decimal("0.00001") and p.conf_to_float() == 0.00001
    assert p.scaled(2) == 6500012 and p.conf_scaled(18) == 10**13


@pytest.mark.anyio
async def test_async_client(astra: FakeAstra) -> None:
    async with AsyncAstraClient(astra.url, max_retries=1, max_retry_delay=0.05) as c:
        assert (await c.price_feed(BTC)).id == BTC
        assert len(await c.latest_prices([BTC])) == 1
        assert (await c.status()).ready
        assert (await c.feed_ids(astra_ids=[BTC_ASTRA])).items[0].pyth_id == BTC
        assert len(await c.candles("Crypto.BTC/USD", "60", 0, 60)) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["ws", "sse"])
async def test_subscribe_and_close(astra: FakeAstra, transport: str) -> None:
    states: list[ConnectionState] = []
    async with AsyncAstraClient(astra.url) as c:
        sub: Subscription = c.subscribe(
            ["0x" + BTC, ETH],
            transport=transport,
            channel="real_time",
            reconnect_base_delay=0.01,
            reconnect_max_delay=0.05,
            on_error=lambda _: None,
            on_state_change=states.append,
        )
        seen: dict[str, PriceUpdate] = {}
        async with sub:
            async for update in sub:
                seen[update.id] = update
                if len(seen) == 2:
                    break
        assert seen[BTC].price.to_decimal() == Decimal("65000")
        assert sub.ids == (BTC, ETH)
        assert sub.stats.connects >= 1
        assert sub.state == "closed" and sub.error is None
        assert "open" in states

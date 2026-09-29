from __future__ import annotations

import os
import time

import pytest

from avee.astra import AstraClient, AsyncAstraClient

BASE_URL = os.environ.get("ASTRA_BASE_URL", "https://astra.preview.avee.tech")
BTC = "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"

pytestmark = [pytest.mark.live, pytest.mark.skipif(os.environ.get("ASTRA_LIVE") != "1", reason="set ASTRA_LIVE=1")]


def test_rest() -> None:
    with AstraClient(BASE_URL) as c:
        assert any(f.id == BTC for f in c.price_feeds(query="btc", asset_type="crypto"))
        [btc] = c.latest_prices([BTC])
        assert btc.price.to_float() > 0
        assert c.status().feeds
        now = int(time.time())
        assert c.candles("Crypto.BTC/USD", "60", now - 6 * 3600, now)
        assert c.prices_at(now - 600, [BTC])[0].id == BTC


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["ws", "sse"])
async def test_stream(transport: str) -> None:
    async with AsyncAstraClient(BASE_URL) as c, c.subscribe([BTC], transport=transport) as sub:
        async for u in sub:
            assert u.id == BTC
            break

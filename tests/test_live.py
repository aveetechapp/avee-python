from __future__ import annotations

import os

import pytest

from avee import PREVIEW_BASE_URL, AveeClient

pytestmark = [pytest.mark.live, pytest.mark.skipif(os.environ.get("AVEE_LIVE") != "1", reason="set AVEE_LIVE=1")]


def test_a_few_cheap_reads() -> None:
    with AveeClient(os.environ.get("AVEE_BASE_URL", PREVIEW_BASE_URL)) as c:
        assert c.status().status
        chains = c.chains()
        assert chains.items
        page = c.pairs(chains=[chains.items[0].slug], limit=5)
        if page.items:
            first = page.items[0]
            assert c.pair(first.network.slug, first.pair_address).pair_address == first.pair_address
        info = c.last_response
        assert info is not None and info.request_id and info.rate_limit.remaining is not None

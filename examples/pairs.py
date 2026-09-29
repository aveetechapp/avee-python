import os

from avee import PREVIEW_BASE_URL, AveeAPIError, AveeClient, RequestOptions

with AveeClient(os.environ.get("AVEE_BASE_URL", PREVIEW_BASE_URL)) as client:
    try:
        chains = client.chains().items
        pairs = client.iter_pairs(chains=[chains[0].slug], sort="liquidity", limit=20, options=RequestOptions(max_pages=2))
        for n, pair in enumerate(pairs, 1):
            print(pair.network.slug, pair.pair_address, pair.liquidity_usd)
        print(f"{n} pairs; budget left: {client.last_response.rate_limit.remaining if client.last_response else None}")
    except AveeAPIError as err:
        print(err.status, err.code, err.request_id)
        raise

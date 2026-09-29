# Astra oracle client for Python

```python
from avee.astra import AstraClient
[btc] = AstraClient().latest_prices(["0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"])
print(btc.price.to_decimal())  # 83095.4425
```

Astra is avee's composite exchange index served over the routes and JSON shapes of Pyth's Hermes. No
key is needed. `avee.astra` wraps its REST, SSE and WebSocket surfaces with typed dataclasses,
retries and reconnects. The `astra` extra adds `websockets` and `certifi` to `httpx`; Python 3.10+.

```sh
pip install 'avee[astra]'
```

The default host is the preview host `https://astra.preview.avee.tech`; pass a base URL to use another
(`AstraClient(base_url)`). A base URL with a path prefix works.

## REST

`AstraClient` is synchronous, `AsyncAstraClient` has the same methods as coroutines. Both are context
managers and accept your own `httpx.Client` / `httpx.AsyncClient`.

| Method | Route |
|---|---|
| `price_feeds(query=, asset_type=)` | `GET /v2/price_feeds` |
| `price_feed(id)` | `GET /v2/price_feeds/{id}` |
| `latest_prices(ids, ignore_invalid=)` | `GET /v2/updates/price/latest`, `MAX_IDS_PER_URL` (200) ids per request, merged in request order |
| `prices_at(publish_time, ids)` | `GET /v2/updates/price/{publish_time}` |
| `prices_in_interval(publish_time, seconds, ids, unique=)` | `GET /v2/updates/price/{publish_time}/{interval}`, flattened oldest first |
| `feeds(category=)` | `GET /v1/feeds`: both ids, category, live value and status |
| `feed_ids(pyth_ids=, astra_ids=, category=)` | `GET /v1/feed-ids`: which Pyth ids Astra serves, and their Astra ids |
| `status(feed=)` | `GET /v1/status`; with `feed`, that feed's entry alone, and a `503` (not `trading`, or `stale`) is returned as the report, not raised |
| `candles(feed, resolution, from_time, to_time)` | `GET /v1/candles` as `list[Candle]` |

Ids are accepted with or without `0x`, in any case, are de-duplicated, and come back lower-case
without `0x` (`normalize_feed_id`). Times are Unix seconds.

**Prices are exact.** `Price` keeps `price` and `conf` as integer strings with `expo`, as sent:

```python
p.to_decimal()   # Decimal('83095.442500000'), exact
p.to_float()     # 83095.4425, correctly rounded
p.scaled(18)     # 83095442500000000000000, int, truncated toward zero
p.conf_to_decimal(); p.conf_scaled(8)
```

## Streaming

Streaming is asynchronous:

```python
async with AsyncAstraClient() as astra, astra.subscribe(ids, channel="fixed_rate@1000ms") as sub:
    async for update in sub:
        ...
```

Leaving `async for` does not close a subscription: it keeps reconnecting until `async with` exits or
`await sub.aclose()` is called.

- `transport`: `"ws"` (default, the Hermes WebSocket protocol) or `"sse"`
  (`/v2/updates/price/stream`, which also takes `benchmarks_only`). SSE carries the ids in the URL, so
  it takes at most `MAX_IDS_PER_URL` (200) and refuses more with `AstraValidationError`; the WebSocket
  sends them in a message and takes up to 500.
- **Reconnect** on any drop, 429 or 5xx: full-jitter backoff from 0.5 s, capped at 30 s, reset after a
  connection has been up for 60 s; `Retry-After` is a floor. The same ids are resubscribed.
- **Keepalive**: WebSocket pings every `idle_timeout / 2` (45 s / 2) and reconnects when a pong is
  late or the subscribe is not answered within `idle_timeout`; SSE reconnects after `idle_timeout`
  without a byte.
- **Exactly the new values**: an update older than the last one delivered for its feed, or an exact
  repeat of it (the replay sent on reconnect), is dropped.
- **Bounded memory**: a consumer that falls behind gets the newest update per feed, never a queue;
  `sub.stats.coalesced` counts what it skipped.
- **Fatal** (the iterator raises, no retry): 400, 404, 422, and a refused WebSocket subscription.
  Everything else, an unexpected exception included, goes to `on_error` as an `AstraError` and is
  retried. `on_state_change` sees
  `connecting → open → reconnecting → … → closed`.

## Errors

All errors derive from `AstraError`:

| Class | When |
|---|---|
| `AstraHTTPError` | non-2xx: `status`, `body`, `problem` (`application/problem+json`), `retry_after`, `retryable` |
| `AstraTimeoutError` | a network operation exceeded `timeout` (10 s), or a stream went idle |
| `AstraConnectionError` | network failure, abnormal WebSocket close |
| `AstraValidationError` | bad input, or a response that breaks the contract (also a `ValueError`) |
| `AstraSubscriptionError` | the WebSocket subscribe was refused |

REST calls are GETs and are retried `max_retries` times (2) on 408, 429, 502, 503, 504, timeouts and
network errors. `Retry-After` is honoured up to `max_retry_delay` (30 s); a longer one is raised.

## Find your feeds

Paste the Pyth ids your code already uses and see which ones Astra serves:

```python
m = client.feed_ids(pyth_ids=["0xe62df6c8…", "0xff61491a…"])
# m.items:   [FeedIdEntry(symbol="Crypto.BTC/USD", pyth_id="e62d…", astra_id="1de7…", …)]
# m.missing: Pyth ids Astra does not serve
```

Every id in `items` works as it is on the Hermes routes: nothing in your code changes for those
feeds. `feed_ids()` without a filter lists every feed Astra serves, and `missing` is then `None`. Up
to `MAX_IDS_PER_URL` (200) ids go in one request; a longer list is split, so the URL fits the
request line a proxy accepts, and merged, sorted by symbol.

## Moving from Pyth Hermes

Code calling Hermes over HTTP only changes its host to `https://astra.preview.avee.tech`; an `Authorization`
header is accepted and ignored. `pythclient`'s v1 routes (`/api/latest_price_feeds`, `/ws`) are served
too.

| Hermes | this package |
|---|---|
| `GET /v2/updates/price/latest?ids[]=…` | `latest_prices(ids)` |
| `GET /v2/updates/price/stream` | `subscribe(ids, transport="sse")` |
| `wss://…/ws` subscribe | `subscribe(ids)` |
| `int(price) * 10 ** expo` | `price.to_float()`, `to_decimal()`, `scaled(n)` |
| `binary.data` for `updatePriceFeeds` | always empty: Astra is unsigned and cannot be verified on-chain |

A reference-status feed is served over Hermes like a trading one; read `status()` or `feeds()` before
liquidating on it.

## Limits

| Limit | Value |
|---|---|
| Ids per URL | `MAX_IDS_PER_URL`, 200: the edge rejects a longer request line before it reaches Astra. `latest_prices` and `feed_ids` split a longer list and merge the answers, failing whole when any request fails; SSE refuses more |
| Ids per call / WebSocket subscription | 500 (historical routes: 100 feeds) |
| Interval window | 60 s |
| Candles per request | 5000 |
| Response body | `max_response_bytes`, 8 MiB |
| Stream message | `max_message_bytes`, 1 MiB; a larger WebSocket message ends the connection, which reconnects |
| Stream lifetime | 24 h on the server; the client reconnects |

## Development

```sh
python -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/pytest            # local fake Astra
ASTRA_LIVE=1 .venv/bin/pytest tests/astra/test_live.py
.venv/bin/mypy
```

The contract test reads `tests/astra/spec/astra.yml`, the synced copy of the Astra OpenAPI file, and fails
when a route or field this package reads changes type or disappears.

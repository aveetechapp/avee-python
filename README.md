# avee DEX data API client for Python

```python
from avee import AveeClient
page = AveeClient().pairs(chains=["base"], sort="liquidity", limit=10)
print(page.items[0].pair_address)
```

Typed access to every operation of the avee client API (`/api/v1`): pairs, tokens, trades, candles,
wallets, leaderboards, farms, perps and oracle prices across every chain avee indexes. No key is
needed. Depends on `httpx` only; Python 3.10+.

```sh
pip install avee
```

`AveeClient` is synchronous, `AsyncAveeClient` has the same methods as coroutines. Both are context
managers and accept your own `httpx.Client` / `httpx.AsyncClient` (`http_client=`). The default host
is the preview host: `DEFAULT_BASE_URL` equals `PREVIEW_BASE_URL`.

## Options

| Argument | Default | |
|---|---|---|
| `base_url` | `DEFAULT_BASE_URL` | a path prefix works |
| `api_key` | `None` | sent as `X-API-Key`; only raises the limits |
| `timeout` | 30 s | per attempt |
| `max_retries` | 2 | GETs only |
| `max_retry_delay` | 30 s | a longer `Retry-After` is raised instead |
| `max_response_bytes` | 16 MiB | a larger body is refused |
| `headers`, `http_client` | | |
| `payer` | `None` | opts into x402, see below |

## Operations

Path parameters are positional (a chain is a slug or an `int` id); everything else is keyword-only
with the API's names (`from` becomes `from_`). A batch takes its body fields as keywords
(`pair_batch(items=[{"chain": "base", "address": "0x…"}])`, dataclasses work too). Input is checked
against the specification's bounds before anything is sent (`AveeValidationError`, a `ValueError`); a
batch takes up to the `maxItems` of its request schema (pairs 50, tokens and wallet labels 200).
Responses are frozen dataclasses from `avee.models`.

Every value set in the specification has a class of constants in `avee.models`:
`sort=SortBy.VOLUME`, `timeframe=TimeFrame.H24`, `pair.status == PairStatus.SCAM`. A value that starts
with a digit leads with its unit (`"24h"` is `TimeFrame.H24`, `"30d"` is `WalletWindow.D30`). Fields
and parameters stay `str`, so an unknown value from the server decodes as it is. An omitted parameter
takes the server's default, listed in each method's docstring.

<!-- operations:start -->
| Method | Route | What it answers |
|---|---|---|
| **meta** | | |
| `x402_discovery()` | `GET /.well-known/x402` | Operations payable with x402 and their prices |
| `status()` | `GET /status` | Liveness and upstream health |
| `key()` | `GET /key` | Plan and limits of the calling key |
| `config()` | `GET /config` | Enumerations and defaults |
| `chains()` | `GET /chains` | Chains served right now |
| **dex** | | |
| `search(…)` | `GET /search` | Typeahead across tokens and pairs |
| `pairs(…), iter_pairs` | `GET /pairs` | Liquidity pair screener |
| `pair(chain, address)` | `GET /chains/{chain}/pairs/{address}` | One liquidity pair |
| `pair_trades(chain, address, …), iter_pair_trades` | `GET /chains/{chain}/pairs/{address}/trades` | Trade tape of a pair |
| `pair_candles(chain, address, …)` | `GET /chains/{chain}/pairs/{address}/candles` | OHLCV candles |
| `trending(…), iter_trending` | `GET /trending` | Trending pairs right now |
| `pairs_new(…), iter_pairs_new` | `GET /pairs/new` | Newest pairs on one chain |
| `launchpad_tokens(…), iter_launchpad_tokens` | `GET /launchpads/tokens` | Launchpad launches by stage (new, bonding or graduated) |
| `pair_batch(…)` | `POST /pairs/batch` | Many pairs in one call |
| `perps(…), iter_perps` | `GET /perps` | Perpetual markets |
| `perp_history(market, …)` | `GET /perps/{market}/history` | Open interest, funding and mark history of a perpetual |
| `perp_stats(market, …)` | `GET /perps/{market}/stats` | How positions on a perpetual opened, closed and were liquidated over the last day |
| `perp_liquidations(…)` | `GET /perps/liquidations` | Daily liquidations of a perpetual or a whole venue |
| `deployer_tokens(address, …), iter_deployer_tokens` | `GET /deployers/{address}/tokens` | Launches of one deployer, with its reputation card |
| **token** | | |
| `token_by_address(chain, address, …)` | `GET /chains/{chain}/tokens/{address}` | Canonical token by chain and address |
| `token_pairs(chain, address, …)` | `GET /chains/{chain}/tokens/{address}/pairs` | Top pairs of a token by 24h volume |
| `token_verdict(chain, address)` | `GET /chains/{chain}/tokens/{address}/verdict` | Signed token verdict, submittable to the trust oracle |
| `token_verdict_proof(chain, address)` | `GET /chains/{chain}/tokens/{address}/proof` | Merkle proof of a verdict at the last published epoch |
| `token_brief(chain, address)` | `GET /chains/{chain}/tokens/{address}/brief` | Everything needed to decide about a token, in one call |
| `token_by_id(id, …)` | `GET /tokens/{id}` | Canonical token by id |
| `token_by_slug(slug, …)` | `GET /tokens/by-slug/{slug}` | Canonical token by slug |
| `tokens(…), iter_tokens` | `GET /tokens` | Token market list, ranked by market cap |
| `token_batch(…)` | `POST /tokens/batch` | Resolve many tokens at once |
| `token_holders(chain, address, …), iter_token_holders` | `GET /chains/{chain}/tokens/{address}/holders` | Top holders of a token |
| `token_traders(chain, address, …), iter_token_traders` | `GET /chains/{chain}/tokens/{address}/traders` | Wallets that traded a token, with their PNL on it |
| **farm** | | |
| `farms(…), iter_farms` | `GET /farms` | Yield farm screener |
| `farm(chain, address)` | `GET /chains/{chain}/farms/{address}` | One yield farm |
| **wallet** | | |
| `wallets(…), iter_wallets` | `GET /wallets` | Rank traders on one chain |
| `wallet_stats(…)` | `GET /wallets/stats` | Trader population per chain |
| `wallet_labels_batch(…)` | `POST /wallets/labels/batch` | Behaviour labels for many wallets |
| `wallet_overview(address, …)` | `GET /wallets/{address}/overview` | One wallet across every chain it traded |
| `leaderboard(…)` | `GET /leaderboard` | Chain and DEX protocol boards |
| `wallet_profile(chain, address)` | `GET /chains/{chain}/wallets/{address}` | Trader profile with metrics for every window |
| `wallet_positions(chain, address, …), iter_wallet_positions` | `GET /chains/{chain}/wallets/{address}/positions` | Open and closed positions of a wallet |
| `wallet_trades(chain, address, …), iter_wallet_trades` | `GET /chains/{chain}/wallets/{address}/trades` | Raw trade history of a wallet |
| `wallet_chart(chain, address, …)` | `GET /chains/{chain}/wallets/{address}/chart` | Wallet performance series — PNL and ROI |
| `wallet_best_trades(chain, address, …)` | `GET /chains/{chain}/wallets/{address}/best-trades` | Best and worst closed trades of a wallet |
| `wallet_rounds(chain, address, …), iter_wallet_rounds` | `GET /chains/{chain}/wallets/{address}/rounds` | Position rounds — one entry and exit cycle per row |
| `wallet_funding(chain, address)` | `GET /chains/{chain}/wallets/{address}/funding` | Who funded a wallet |
| **oracle** | | |
| `oracle_prices(…)` | `GET /prices` | Latest oracle prices by feed id |
| `oracle_prices_at(…)` | `GET /prices/at` | Oracle prices at a past moment |
<!-- operations:end -->

## Pagination

Each paged operation has an `iter_…` twin (an async iterator on `AsyncAveeClient`) that fetches the
next page only when the previous one is used up; `options=RequestOptions(max_pages=5)` caps it.

```python
for trade in client.iter_pair_trades("base", pair, tx_type=["buy"]):
    ...
```

A repeated cursor ends the walk with `AveeValidationError` rather than looping. Prefer `pair_batch`,
`token_batch` and `wallet_labels_batch` to one call per address.

## Errors

| Class | When |
|---|---|
| `AveeAPIError` | non-2xx: `status`, `code` (branch on it), `detail`, `param`, `request_id`, `rate_limit`, `retry_after`, `retryable`, `paid`, `payment_required`, `payment` |
| `AveeTimeoutError` | an attempt exceeded `timeout` |
| `AveeConnectionError` | network failure |
| `AveeValidationError` | bad input, or a response that breaks the contract or is too large |
| `AveePaymentError` | the x402 flow stopped before paying: an unreadable challenge or offer, no network in common, the payer declined |

GETs are retried on 408, 429, 5xx, timeouts and network errors with full-jitter backoff; a
`Retry-After` (or, on a 429 without one, the `RateLimit` reset) is waited out when it fits
`max_retry_delay`. POSTs are never retried. Redirects are not followed, even on an `http_client` that
follows them: a 3xx is an `AveeAPIError`, so the key and a payment signature never leave the host.

## Rate limits

`client.last_response` holds the latest `status`, `request_id`, `rate_limit` (`limit`, `remaining`,
`reset_seconds`, `retry_after_seconds`, `policy`) and the x402 `payment` receipt. Errors carry their own
`rate_limit`.

## Paying past the keyless limit (x402)

Off by default: without a payer the client never pays and a spent budget is an ordinary 429. With one
it sends `Accept-Payment: x402`; a spent budget then answers 402, the client picks the first `exact`
offer on a network from `payer.networks` (in that order), calls `payer.sign(context)` once, and
repeats the request once with `PAYMENT-SIGNATURE`. A paid request is never retried; the receipt is
`last_response.payment`. The payer sees the amount, asset, network and `pay_to` in
`context.requirement` and declines by returning `None`. `sign` may be a coroutine on
`AsyncAveeClient`. An offer whose amount is not a positive integer (or exceeds
`max_amount_required`), or that lacks an asset or `pay_to`, is refused before the payer is asked.

The SDK holds no key and depends on no wallet library. With eth-account:

```python
import secrets, time
from eth_account import Account
from avee import AveeClient, PaymentSignature

class Payer:
    networks = ["eip155:84532"]

    def __init__(self, key: str) -> None:
        self.account = Account.from_key(key)

    def sign(self, ctx):
        r = ctx.requirement
        if int(r.amount) > 10_000:
            return None
        now = int(time.time())
        auth = {"from": self.account.address, "to": r.pay_to, "value": r.amount,
                "validAfter": str(now - 5), "validBefore": str(now + r.max_timeout_seconds),
                "nonce": "0x" + secrets.token_hex(32)}
        signed = self.account.sign_typed_data(
            domain_data={"name": r.extra["name"], "version": r.extra["version"], "chainId": 84532, "verifyingContract": r.asset},
            message_types={"TransferWithAuthorization": [
                {"name": "from", "type": "address"}, {"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
                {"name": "validAfter", "type": "uint256"}, {"name": "validBefore", "type": "uint256"}, {"name": "nonce", "type": "bytes32"}]},
            message_data={**auth, "value": int(auth["value"]), "validAfter": int(auth["validAfter"]),
                          "validBefore": int(auth["validBefore"]), "nonce": bytes.fromhex(auth["nonce"][2:])},
        )
        return PaymentSignature(payload={"signature": "0x" + signed.signature.hex(), "authorization": auth})

client = AveeClient(payer=Payer(KEY))
```

`sign` may instead return `PaymentSignature(header=...)`, a finished `PAYMENT-SIGNATURE` from any x402
client library.

## Astra price oracle

The package also carries the client for Astra, avee's Hermes-compatible price oracle, as
`avee.astra`, which needs the `astra` extra (`websockets`, `certifi`); `import avee` never loads it.

```sh
pip install 'avee[astra]'
```

```python
from avee.astra import AstraClient
[btc] = AstraClient().latest_prices(["0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"])
```

REST, SSE and WebSocket, exact prices, reconnecting streams: [ASTRA.md](ASTRA.md).

## Compatibility

Models are generated from the OpenAPI document: unknown fields are ignored, enum fields are plain
`str` and keep unknown values, and the one polymorphic field (`Transaction.block_info`, `event_data`)
falls back to a `dict` for an unrecognised branch. Inside a major version the package is additive
only.

## Development

`pytest` runs against a local fake server; `AVEE_LIVE=1 pytest tests/test_live.py` checks the preview
host; `mypy` checks `src` in strict mode.

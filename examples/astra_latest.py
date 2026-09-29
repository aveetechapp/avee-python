import os

from avee.astra import DEFAULT_BASE_URL, AstraClient

BTC = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
ETH = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"

with AstraClient(os.environ.get("ASTRA_BASE_URL", DEFAULT_BASE_URL)) as astra:
    for update in astra.latest_prices([BTC, ETH]):
        p = update.price
        print(update.id, p.to_decimal(), "±", p.conf_to_float(), "as 1e18:", p.scaled(18))
    print("stale:", [f.symbol for f in astra.status().feeds if f.stale])

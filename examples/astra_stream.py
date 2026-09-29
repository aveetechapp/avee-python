import asyncio
import os
from datetime import datetime, timezone

from avee.astra import DEFAULT_BASE_URL, AsyncAstraClient

BTC = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"


async def main() -> None:
    async with AsyncAstraClient(os.environ.get("ASTRA_BASE_URL", DEFAULT_BASE_URL)) as astra:
        async with astra.subscribe(
            [BTC],
            channel="fixed_rate@1000ms",
            on_error=lambda err: print("astra:", err),
            on_state_change=lambda state: print("connection", state),
        ) as sub:
            async for update in sub:
                at = datetime.fromtimestamp(update.price.publish_time, timezone.utc).isoformat()
                print(at, update.price.to_decimal())


asyncio.run(main())

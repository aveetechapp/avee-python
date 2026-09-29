from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .errors import AstraValidationError

MIN_EXPO = -32
MAX_EXPO = 32
MAX_SCALE_DECIMALS = 77


def _scale(mantissa: str, expo: int, decimals: int) -> int:
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= MAX_SCALE_DECIMALS:
        raise AstraValidationError(f"decimals must be an integer in [0, {MAX_SCALE_DECIMALS}], got {decimals!r}")
    value = int(mantissa)
    shift = expo + decimals
    if shift >= 0:
        return value * int(10**shift)
    quotient = abs(value) // int(10**-shift)
    return -quotient if value < 0 else quotient


@dataclass(frozen=True, slots=True)
class Price:
    price: str
    conf: str
    expo: int
    publish_time: int

    def to_decimal(self) -> Decimal:
        return Decimal(f"{self.price}E{self.expo}")

    def to_float(self) -> float:
        return float(f"{self.price}e{self.expo}")

    def scaled(self, decimals: int) -> int:
        return _scale(self.price, self.expo, decimals)

    def conf_to_decimal(self) -> Decimal:
        return Decimal(f"{self.conf}E{self.expo}")

    def conf_to_float(self) -> float:
        return float(f"{self.conf}e{self.expo}")

    def conf_scaled(self, decimals: int) -> int:
        return _scale(self.conf, self.expo, decimals)

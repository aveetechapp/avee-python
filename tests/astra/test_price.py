from __future__ import annotations

from decimal import Decimal

import pytest

from avee.astra import AstraValidationError, Price, normalize_feed_id
from avee.astra.ids import normalize_feed_ids

from .conftest import BTC, ETH


@pytest.mark.parametrize(
    ("mantissa", "expo", "want"),
    [
        ("83095442500000", -9, "83095.4425"),
        ("-12345", -2, "-123.45"),
        ("5", -3, "0.005"),
        ("12", 3, "1.2E+4"),
        ("0", -8, "0E-8"),
        ("1" + "0" * 70, -70, "1.0000000000000000000000000000000000000000000000000000000000000000000000"),
    ],
)
def test_to_decimal_is_exact(mantissa: str, expo: int, want: str) -> None:
    got = Price(mantissa, "0", expo, 0).to_decimal()
    assert got == Decimal(want)
    assert got.as_tuple().exponent == expo


def test_to_float_is_correctly_rounded() -> None:
    p = Price("83095442500000", "4947500000", -9, 0)
    assert p.to_float() == 83095.4425
    assert p.conf_to_float() == 4.9475
    assert p.conf_to_decimal() == Decimal("4.9475")


@pytest.mark.parametrize(
    ("mantissa", "expo", "decimals", "want"),
    [
        ("83095442500000", -9, 18, 83095442500000 * 10**9),
        ("83095442500000", -9, 2, 8309544),
        ("83095442500000", -9, 0, 83095),
        ("-12345", -2, 1, -1234),
        ("7", 3, 2, 700000),
        ("9" * 40, -8, 77, int("9" * 40) * 10**69),
    ],
)
def test_scaled_truncates_toward_zero(mantissa: str, expo: int, decimals: int, want: int) -> None:
    assert Price(mantissa, "4947500000", expo, 0).scaled(decimals) == want


def test_conf_scaled() -> None:
    assert Price("1", "4947500000", -9, 0).conf_scaled(6) == 4947500


@pytest.mark.parametrize("decimals", [-1, 78, 1.5, True])
def test_scaled_refuses_bad_decimals(decimals: object) -> None:
    with pytest.raises(AstraValidationError):
        Price("1", "0", -8, 0).scaled(decimals)  # type: ignore[arg-type]


def test_normalize_feed_id() -> None:
    assert normalize_feed_id("0x" + BTC.upper()) == BTC
    assert normalize_feed_id("0X" + BTC) == BTC
    for bad in ["", "0x", BTC[1:], BTC + "0", BTC[1:] + "g", " " + BTC[1:]]:
        with pytest.raises(AstraValidationError):
            normalize_feed_id(bad)


def test_normalize_feed_ids_dedupes_and_bounds() -> None:
    assert normalize_feed_ids([BTC, "0x" + BTC, ETH], 500) == [BTC, ETH]
    with pytest.raises(AstraValidationError, match="at least one"):
        normalize_feed_ids([], 500)
    with pytest.raises(AstraValidationError, match="at most 1"):
        normalize_feed_ids([BTC, ETH], 1)
    with pytest.raises(AstraValidationError, match="not a single string"):
        normalize_feed_ids(BTC, 500)

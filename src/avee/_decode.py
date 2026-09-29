from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from .errors import AveeValidationError

T = TypeVar("T")
Decoder = Callable[[Any, str], T]


def _obj(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise AveeValidationError(f"{where} must be an object, got {type(value).__name__}")
    return value


def _req(o: Mapping[str, Any], key: str, fn: Decoder[T], where: str) -> T:
    value = o.get(key)
    if value is None:
        raise AveeValidationError(f"{where} is missing")
    return fn(value, where)


def _opt(o: Mapping[str, Any], key: str, fn: Decoder[T], where: str) -> T | None:
    value = o.get(key)
    return None if value is None else fn(value, where)


def _str(value: Any, where: str) -> str:
    if not isinstance(value, str):
        raise AveeValidationError(f"{where} must be a string, got {type(value).__name__}")
    return value


def _int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or (isinstance(value, float) and not value.is_integer()):
        raise AveeValidationError(f"{where} must be an integer, got {value!r}")
    return int(value)


def _float(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AveeValidationError(f"{where} must be a number, got {type(value).__name__}")
    return float(value)


def _bool(value: Any, where: str) -> bool:
    if not isinstance(value, bool):
        raise AveeValidationError(f"{where} must be a boolean, got {type(value).__name__}")
    return value


def _any(value: Any, where: str) -> Any:
    return value


def _list_of(fn: Decoder[T]) -> Decoder[list[T]]:
    def decode(value: Any, where: str) -> list[T]:
        if not isinstance(value, list):
            raise AveeValidationError(f"{where} must be an array, got {type(value).__name__}")
        return [fn(v, f"{where}[{i}]") for i, v in enumerate(value)]

    return decode


def _map_of(fn: Decoder[T]) -> Decoder[dict[str, T]]:
    def decode(value: Any, where: str) -> dict[str, T]:
        return {k: fn(v, f"{where}.{k}") for k, v in _obj(value, where).items()}

    return decode


def _one_of(value: Any, where: str, branches: tuple[tuple[Decoder[Any], tuple[str, ...]], ...]) -> Any:
    o = _obj(value, where)
    for fn, required in branches:
        if all(k in o for k in required):
            return fn(value, where)
    return dict(o)

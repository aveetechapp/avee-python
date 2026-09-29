from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from avee.astra.models import (
    decode_candles,
    decode_feed,
    decode_feed_id_map,
    decode_feed_metadata,
    decode_status_report,
    decode_streamed_price_feed,
    decode_update_envelope,
)

SPEC_DIR = Path(__file__).parent / "spec"
SPEC: dict[str, Any] = yaml.safe_load((SPEC_DIR / "astra.yml").read_text())
CONTRACT: dict[str, Any] = json.loads((SPEC_DIR / "sdk-contract.json").read_text())
COMPONENTS: dict[str, dict[str, Any]] = SPEC["components"]
RENAME = Path(__file__).resolve().parents[3] / "scripts" / "rename.mjs"


def resolve(node: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in node:
        _, _, section, name = node["$ref"].split("/", 3)
        node = COMPONENTS[section][name]
    return node


def walk(schema: str, path: str) -> dict[str, Any]:
    node = resolve(COMPONENTS["schemas"][schema])
    for part in path.split("."):
        key = part.removesuffix("[]")
        node = resolve(node.get("properties", {})[key])
        if part.endswith("[]"):
            assert node["type"] == "array", f"{schema}.{path}"
            node = resolve(node["items"])
    return node


def instance(node: dict[str, Any], full: bool) -> Any:
    node = resolve(node)
    if "const" in node:
        return node["const"]
    if "enum" in node:
        return node["enum"][0]
    if "oneOf" in node:
        return instance(node["oneOf"][0], full)
    kind = node.get("type")
    if kind == "object":
        required = set(node.get("required", []))
        out: dict[str, Any] = {"x_future_field": {"nested": [1]}} if full else {}
        for key, sub in node.get("properties", {}).items():
            if full or key in required:
                out[key] = instance(sub, full)
        return out
    if kind == "array":
        return [] if node.get("maxItems") == 0 else [instance(node["items"], full)]
    if kind == "integer":
        return 1
    if kind == "number":
        return 1.5
    if kind == "boolean":
        return True
    if kind == "null":
        return None
    return "ab" * 32 if "{64}" in node.get("pattern", "") else "1"


@pytest.mark.parametrize(("route", "params"), sorted(CONTRACT["routes"].items()))
def test_route_and_parameters_exist(route: str, params: list[str]) -> None:
    op = SPEC["paths"][route]["get"]
    names = {resolve(p)["name"] for p in op.get("parameters", [])}
    assert set(params) <= names


@pytest.mark.parametrize(
    ("schema", "path", "kind"),
    [(schema, path, kind) for schema, fields in sorted(CONTRACT["fields"].items()) for path, kind in sorted(fields.items())],
)
def test_field_has_the_expected_type(schema: str, path: str, kind: str) -> None:
    assert walk(schema, path)["type"] == kind


@pytest.mark.parametrize(("ref", "values"), sorted(CONTRACT["enums"].items()))
def test_enum_still_offers_the_typed_values(ref: str, values: list[str]) -> None:
    section, name = ref.split(".")
    node = resolve(COMPONENTS[section][name])
    if section == "parameters":
        node = resolve(node["schema"])
    assert set(values) <= set(node["enum"])


DECODERS: list[tuple[str, Callable[[Any], Any]]] = [
    ("PriceUpdate", lambda v: decode_update_envelope(v, "x")),
    ("PriceFeed", lambda v: decode_streamed_price_feed(v, "x")),
    ("PriceFeedMetadata", lambda v: decode_feed_metadata(v, "x")),
    ("Feed", lambda v: decode_feed(v, "x")),
    ("FeedIDList", lambda v: decode_feed_id_map(v, "x")),
    ("StatusReport", lambda v: decode_status_report(v, "x")),
    ("Bars", lambda v: decode_candles(v, "x")),
]


@pytest.mark.parametrize("full", [False, True], ids=["required-only", "every-field-plus-unknown"])
@pytest.mark.parametrize(("schema", "decode"), DECODERS, ids=[d[0] for d in DECODERS])
def test_decoder_accepts_spec_instances(schema: str, decode: Callable[[Any], Any], full: bool) -> None:
    decode(instance(COMPONENTS["schemas"][schema], full))


def test_spec_example_decodes() -> None:
    [u] = decode_update_envelope(resolve(COMPONENTS["schemas"]["PriceUpdate"])["example"], "example")
    assert u.price.to_float() == 65123.45


@pytest.mark.skipif(not RENAME.exists(), reason="outside the monorepo")
def test_names_are_applied() -> None:
    subprocess.run(["node", str(RENAME), "--check"], check=True, capture_output=True)

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from .conftest import CONTRACT, SCHEMAS, SPEC, resolve

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def contract_type(n: dict[str, Any]) -> str:
    if "$ref" in n:
        return "#" + n["$ref"].rsplit("/", 1)[1]
    if "oneOf" in n:
        return "oneOf"
    t = n.get("type")
    if t == "array":
        return contract_type(n["items"]) + "[]"
    if t == "object":
        ap = n.get("additionalProperties")
        if isinstance(ap, dict):
            return f"map<{contract_type(ap)}>"
        return "object" if "properties" in n else "map<any>"
    if "enum" in n:
        return "string"
    return t if isinstance(t, str) else "any"


@pytest.mark.parametrize("op_id", sorted(CONTRACT["operations"]))
def test_every_operation_and_parameter_is_still_served(op_id: str) -> None:
    op = CONTRACT["operations"][op_id]
    item = SPEC["paths"].get(op["path"], {}).get(op["method"].lower())
    assert item is not None and item["operationId"] == op_id
    names = {resolve(p)["name"] for p in item.get("parameters", [])}
    assert set(op["params"]) <= names
    assert contract_type(item["responses"]["200"]["content"]["application/json"]["schema"]) == "#" + op["response"]


@pytest.mark.parametrize(("schema", "path", "want"), [(s, p, t) for s, fields in CONTRACT["fields"].items() for p, t in fields.items()])
def test_every_field_keeps_its_type(schema: str, path: str, want: str) -> None:
    node = SCHEMAS[schema]
    for part in path.split("."):
        node = resolve(node).get("properties", {})[part]
    assert contract_type(node) == ("oneOf" if want.startswith("oneOf<") else want)


@pytest.mark.skipif(not (SCRIPTS / "rename.mjs").exists() or shutil.which("node") is None, reason="outside the monorepo")
def test_names_are_applied() -> None:
    subprocess.run(["node", str(SCRIPTS / "rename.mjs"), "--check"], check=True, capture_output=True)

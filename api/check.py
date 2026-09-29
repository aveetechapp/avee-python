from __future__ import annotations

import dataclasses
import importlib
import inspect
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASELINE = HERE / "public-api.txt"
MODULE = re.search(r'packages = \["src/(\w+)"\]', (ROOT / "pyproject.toml").read_text()).group(1)
BASELINES = {MODULE: BASELINE, f"{MODULE}.astra": HERE / "astra-public-api.txt"}


def _public(names: Any) -> list[str]:
    return sorted(n for n in names if not n.startswith("_") or n == "__init__")


def _callable(owner: str, fn: Any) -> list[str]:
    sig = inspect.signature(fn)
    params = [p for p in sig.parameters.values() if p.name != "self"]
    positional = [p.name for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    out = [f"{owner} positional ({', '.join(positional)})", f"{owner} returns {sig.return_annotation}"]
    for p in params:
        need = "required" if p.default is p.empty and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD) else "optional"
        out.append(f"{owner} param {p.name}: {p.annotation} [{p.kind.name.lower()}, {need}]")
    return out


def _class(name: str, cls: type) -> list[str]:
    kind = "protocol" if getattr(cls, "_is_protocol", False) else "dataclass" if dataclasses.is_dataclass(cls) else "class"
    bases = ", ".join(b.__name__ for b in cls.__mro__[1:] if b is not object and not b.__name__.startswith("_"))
    out = [f"{kind} {name}({bases})"]
    if dataclasses.is_dataclass(cls):
        for f in dataclasses.fields(cls):
            need = "optional" if f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING else "required"
            out.append(f"{kind} {name} field {f.name}: {f.type} [{need}]")
    else:
        for attr, annotation in vars(cls).get("__annotations__", {}).items():
            if not attr.startswith("_"):
                out.append(f"{kind} {name} attribute {attr}: {annotation}")
    for member in _public(dir(cls)):
        owner = next(c for c in cls.__mro__ if member in vars(c))
        if not owner.__module__.startswith(MODULE):
            continue
        value = vars(owner)[member]
        if isinstance(value, property):
            out.append(f"{kind} {name} property {member}")
        elif inspect.isfunction(value) or isinstance(value, (staticmethod, classmethod)):
            if not (kind != "class" and member == "__init__"):
                out.extend(_callable(f"{kind} {name} def {member}", getattr(cls, member)))
    return out


def surface(module: str = MODULE) -> list[str]:
    package = importlib.import_module(module)
    lines: list[str] = []
    for name in _public(package.__all__):
        value = getattr(package, name)
        if inspect.ismodule(value):
            lines.append(f"module {name}")
            for inner in _public(value.__all__):
                obj = getattr(value, inner)
                lines.extend(_class(f"{name}.{inner}", obj) if inspect.isclass(obj) else [f"alias {name}.{inner}"])
        elif inspect.isclass(value):
            lines.extend(_class(name, value))
        elif inspect.isfunction(value):
            lines.extend(_callable(f"def {name}", value))
        elif isinstance(value, (tuple, frozenset)):
            lines.extend(f"const {name} item {item!r}" for item in (sorted(value) if isinstance(value, frozenset) else value))
        else:
            lines.append(f"const {name} = {value!r}")
    return sorted(set(lines))


def surface_of(src: Path, module: str = MODULE) -> list[str]:
    code = f"import sys; sys.path[:0] = [{str(src)!r}, {str(HERE)!r}]; import check; print(chr(10).join(check.surface({module!r})))"
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if run.returncode != 0:
        raise SystemExit(f"api: cannot import {module} from {src}:\n{run.stderr}")
    return run.stdout.splitlines()


def problems(baseline: list[str], current: list[str]) -> list[str]:
    before = set(baseline)
    owners = {line.split(" param ")[0] for line in baseline if " param " in line}
    protocols = {line.split(" ")[1].split("(")[0] for line in baseline if line.startswith("protocol ")}
    out = [f"removed or changed: {line}" for line in baseline if line not in set(current)]
    for line in current:
        if line in before:
            continue
        if " param " in line and line.endswith(", required]") and line.split(" param ")[0] in owners:
            out.append(f"new required parameter: {line}")
        elif line.startswith("protocol ") and line.split(" ")[1].split("(")[0] in protocols:
            out.append(f"new member on a protocol that callers implement: {line}")
    return out


def main(argv: list[str]) -> int:
    if argv[:1] == ["--update"]:
        src = Path(argv[1]).resolve() if len(argv) > 1 else ROOT / "src"
        for module, baseline in BASELINES.items():
            baseline.write_text("\n".join(surface_of(src, module)) + "\n")
            print(f"api: {baseline.relative_to(ROOT)} now describes the {module} package in {src}")
        return 0
    found = [f"{module}: {p}" for module, baseline in BASELINES.items() for p in problems(baseline.read_text().splitlines(), surface_of(ROOT / "src", module))]
    for line in found:
        print(line)
    if found:
        print("api: breaking changes to the public Python API; they wait for a new major version", file=sys.stderr)
        return 1
    print("api: the public Python API is compatible with its baselines")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

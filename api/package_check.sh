#!/bin/sh
set -eu

py=${PYTHON:-python3}
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

"$py" -m venv "$work/venv"
"$work/venv/bin/python" -m pip install -q --disable-pip-version-check build
"$work/venv/bin/python" -m build -q --wheel --sdist --outdir "$work/dist" "$root" >/dev/null
wheel=$(ls "$work"/dist/*.whl)
"$work/venv/bin/python" -m pip install -q --disable-pip-version-check "$wheel[astra]"
cd "$work"
"$work/venv/bin/python" - "$root/pyproject.toml" "$work/dist" <<'PY'
import importlib
import importlib.metadata as md
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
name = re.search(r'^name = "([^"]+)"', text, re.M).group(1)
version = re.search(r'^version = "([^"]+)"', text, re.M).group(1)
floor = re.search(r'^requires-python = ">=(\d+)\.(\d+)"', text, re.M)
module = name.replace("-", "_")

dist = md.distribution(name)
assert dist.version == version, f"installed {dist.version}, pyproject says {version}"
assert dist.metadata["Requires-Python"] == f">={floor.group(1)}.{floor.group(2)}", dist.metadata["Requires-Python"]
assert sys.version_info[:2] >= (int(floor.group(1)), int(floor.group(2))), f"python {sys.version_info[:2]} is below the declared floor"

pkg = importlib.import_module(module)
assert pathlib.Path(pkg.__file__).parent.joinpath("py.typed").exists(), "py.typed missing from the wheel"
for sub in (pkg, importlib.import_module(f"{module}.astra")):
    unresolved = [n for n in sub.__all__ if not hasattr(sub, n)]
    assert not unresolved, f"{sub.__name__}.__all__ names missing: {unresolved}"
files = sorted(p.name for p in pathlib.Path(sys.argv[2]).iterdir())
assert any(f.endswith(".whl") for f in files) and any(f.endswith(".tar.gz") for f in files), files
print(f"package_check: {name} {version} builds, installs and imports on python {sys.version.split()[0]} ({len(pkg.__all__)} exports)")
PY

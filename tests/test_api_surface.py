from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))

import check


def test_the_public_api_keeps_everything_the_baseline_promises() -> None:
    for module, baseline in check.BASELINES.items():
        assert check.problems(baseline.read_text().splitlines(), check.surface_of(check.ROOT / "src", module)) == [], module


def test_the_gate_reports_removals_and_new_required_inputs() -> None:
    baseline = ["class C def m param a: int [keyword_only, optional]", "protocol P attribute x: int", "protocol P(Protocol)"]
    current = ["class C def m param b: int [keyword_only, required]", "protocol P attribute x: int", "protocol P attribute y: int", "protocol P(Protocol)"]
    assert len(check.problems(baseline, current)) == 3
    current = ["class C def m param a: int [keyword_only, optional]", "class C def m param b: int [keyword_only, required]", *baseline[1:], "protocol P attribute y: int"]
    assert [p.split(":")[0] for p in check.problems(baseline, current)] == ["new required parameter", "new member on a protocol that callers implement"]

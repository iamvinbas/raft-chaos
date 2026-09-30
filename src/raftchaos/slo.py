"""Service level objectives and error-budget accounting for a run."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .metrics import Metrics


@dataclass(frozen=True)
class Slo:
    name: str
    description: str
    target: float
    unit: str
    # Returns the measured value for these metrics.
    measure: Callable[[Metrics], float]
    higher_is_better: bool = False

    def budget_used(self, actual: float) -> float:
        """Share of the error budget consumed. Above 1.0 the objective is missed."""
        if self.higher_is_better:
            allowed = 1.0 - self.target
            return (1.0 - actual) / allowed if allowed > 0 else float(actual < self.target)
        return actual / self.target if self.target > 0 else float(actual > 0)


@dataclass(frozen=True)
class SloResult:
    slo: Slo
    actual: float
    budget_used: float

    @property
    def met(self) -> bool:
        return self.budget_used <= 1.0


def default_slos() -> list[Slo]:
    return [
        Slo(
            "availability",
            "windows with at least one successful operation",
            0.90,
            "ratio",
            lambda m: m.availability,
            higher_is_better=True,
        ),
        Slo(
            "latency-p99",
            "99th percentile latency of successful operations",
            250.0,
            "ms",
            lambda m: m.percentile(99),
        ),
        Slo(
            "max-outage",
            "longest period with no successful operation",
            2000.0,
            "ms",
            lambda m: float(m.max_outage_ms),
        ),
    ]


def evaluate(metrics: Metrics, slos: list[Slo] | None = None) -> list[SloResult]:
    results = []
    for slo in slos or default_slos():
        actual = slo.measure(metrics)
        results.append(SloResult(slo, actual, slo.budget_used(actual)))
    return results


def render_report(title: str, results: list[SloResult]) -> str:
    rows = [f"{title}", f"{'SLO':14} {'target':>10} {'actual':>10} {'budget used':>12}  status"]
    for r in results:
        fmt = "{:.3f}" if r.slo.unit == "ratio" else "{:.0f}"
        target = fmt.format(r.slo.target) + ("" if r.slo.unit == "ratio" else r.slo.unit)
        actual = fmt.format(r.actual) + ("" if r.slo.unit == "ratio" else r.slo.unit)
        status = "MET" if r.met else "MISSED"
        rows.append(f"{r.slo.name:14} {target:>10} {actual:>10} {r.budget_used:>11.0%}  {status}")
    return "\n".join(rows)

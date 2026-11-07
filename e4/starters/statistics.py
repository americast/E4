"""Small, explicit statistical reconstructions used by the paper-family starters."""

from __future__ import annotations

import math
import random
from statistics import mean, variance
from typing import Sequence


def quantile(values: Sequence[float], probability: float) -> float:
    """Linearly interpolate an order statistic; requires finite nonempty input."""
    if not values:
        raise ValueError("quantile requires at least one value")
    if any(not math.isfinite(value) for value in values):
        raise ValueError("quantile input must contain only finite values")
    if not 0.0 <= probability <= 1.0:
        raise ValueError("quantile probability must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def welch_mean_test(reference: Sequence[float], alert: Sequence[float]) -> tuple[float, float]:
    """Return alert-minus-reference means and a two-sided Welch p-value."""
    if len(reference) < 2 or len(alert) < 2:
        raise ValueError("Welch test requires at least two blocks in each window")
    if any(not math.isfinite(value) for value in (*reference, *alert)):
        raise ValueError("Welch input must contain only finite values")
    scale = max(abs(value) for value in (*reference, *alert))
    if scale == 0.0:
        return 0.0, 1.0
    reference_scaled = [value / scale for value in reference]
    alert_scaled = [value / scale for value in alert]
    delta_scaled = mean(alert_scaled) - mean(reference_scaled)
    delta = delta_scaled * scale
    if not math.isfinite(delta):
        raise ValueError("Welch effect is outside the finite float range")
    term_reference = variance(reference_scaled) / len(reference_scaled)
    term_alert = variance(alert_scaled) / len(alert_scaled)
    standard_error_squared = term_reference + term_alert
    if standard_error_squared == 0.0:
        return delta, 1.0 if delta_scaled == 0.0 else 0.0
    denominator = (
        term_reference * term_reference / (len(reference) - 1)
        + term_alert * term_alert / (len(alert) - 1)
    )
    if denominator == 0.0:
        return delta, 0.0
    degrees_freedom = standard_error_squared * standard_error_squared / denominator
    statistic = abs(delta) / math.sqrt(standard_error_squared)
    # scipy is an existing package dependency; keep its import lazy for non-statistical paths.
    from scipy.stats import t as student_t

    p_value = float(2.0 * student_t.sf(statistic, degrees_freedom))
    if not math.isfinite(p_value):
        raise ValueError("Welch calculation returned a non-finite p-value")
    return delta, min(1.0, max(0.0, p_value))


def permutation_test(
    reference: Sequence[float],
    alert: Sequence[float],
    *,
    statistic: str,
    quantile_probability: float,
    resamples: int,
    seed: int,
) -> tuple[float, float]:
    """Two-sided independent-unit permutation contrast with a +1 p-value correction."""
    if not reference or not alert:
        raise ValueError("permutation test requires nonempty samples in both windows")
    if any(not math.isfinite(value) for value in (*reference, *alert)):
        raise ValueError("permutation input must contain only finite values")
    if resamples <= 0:
        raise ValueError("resamples must be positive")
    if statistic not in {"mean", "quantile"}:
        raise ValueError("statistic must be 'mean' or 'quantile'")

    scale = max(abs(value) for value in (*reference, *alert))
    if scale == 0.0:
        return 0.0, 1.0
    reference_scaled = [value / scale for value in reference]
    alert_scaled = [value / scale for value in alert]

    def measure(values: Sequence[float]) -> float:
        if statistic == "mean":
            return mean(values)
        return quantile(values, quantile_probability)

    observed_scaled = measure(alert_scaled) - measure(reference_scaled)
    observed = observed_scaled * scale
    if not math.isfinite(observed):
        raise ValueError("permutation effect is outside the finite float range")
    combined = reference_scaled + alert_scaled
    reference_size = len(reference_scaled)
    rng = random.Random(seed)
    extreme = 0
    for _ in range(resamples):
        shuffled = rng.sample(combined, len(combined))
        candidate = measure(shuffled[reference_size:]) - measure(shuffled[:reference_size])
        if abs(candidate) >= abs(observed_scaled) or math.isclose(
            abs(candidate), abs(observed_scaled), rel_tol=1e-12, abs_tol=0.0
        ):
            extreme += 1
    p_value = (extreme + 1) / (resamples + 1)
    return observed, p_value

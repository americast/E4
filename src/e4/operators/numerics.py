"""Numerical helpers with explicit independent-unit and missingness handling."""

from __future__ import annotations

import hashlib
import itertools
import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
from scipy import stats


@dataclass(frozen=True, slots=True)
class ContrastResult:
    raw_delta: float
    normalized_effect: float
    p_value: float
    baseline_center: float
    alert_center: float
    baseline_support: int
    alert_support: int
    baseline_mad_scale: float


def finite_values(values: Iterable[float | int | None]) -> np.ndarray:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return np.asarray(finite, dtype=np.float64)


def median(values: Sequence[float]) -> float | None:
    array = finite_values(values)
    return float(np.median(array)) if array.size else None


def percentile(values: Sequence[float], q: float) -> float | None:
    if not 0.0 <= q <= 1.0:
        raise ValueError("quantile must be in [0,1]")
    array = finite_values(values)
    return float(np.quantile(array, q, method="linear")) if array.size else None


def median_within_blocks(timestamp_values: Iterable[tuple[int, float]], window_start_ms: int,
                         block_minutes: int) -> tuple[float, ...]:
    return block_reduce(timestamp_values, window_start_ms, block_minutes, quantile=0.5)


def block_reduce(timestamp_values: Iterable[tuple[int, float]], window_start_ms: int,
                 block_minutes: int, quantile: float) -> tuple[float, ...]:
    """Collapse adjacent time points to one robust observation per fixed block."""
    if block_minutes <= 0:
        raise ValueError("block_minutes must be positive")
    if not 0 <= quantile <= 1:
        raise ValueError("block quantile must be in [0,1]")
    block_ms = block_minutes * 60_000
    blocks: dict[int, list[float]] = {}
    for timestamp, value in timestamp_values:
        if math.isfinite(float(value)):
            index = (int(timestamp) - window_start_ms) // block_ms
            blocks.setdefault(index, []).append(float(value))
    return tuple(float(np.quantile(blocks[key], quantile, method="linear")) for key in sorted(blocks))


def stable_seed(seed: int, *parts: str) -> int:
    digest = hashlib.sha256("\0".join((str(seed), *parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def permutation_contrast(reference: Sequence[float], alert: Sequence[float], *,
                         seed: int, resamples: int, statistic: str = "median",
                         minimum_blocks: int = 4, relative_scale_floor: float = 0.05,
                         absolute_scale_floor: float = 1e-9,
                         quantile: float = 0.9) -> ContrastResult | None:
    """Two-sided pooled permutation test over pre-aggregated independent units."""
    before, after = finite_values(reference), finite_values(alert)
    if before.size < minimum_blocks or after.size < minimum_blocks:
        return None
    if relative_scale_floor < 0 or absolute_scale_floor <= 0:
        raise ValueError("normalization scale floors must be nonnegative/positive")
    if statistic == "median":
        center = lambda values: float(np.median(values))
        scale_value = float(stats.median_abs_deviation(before, scale="normal", nan_policy="omit"))
    elif statistic == "mean":
        center = lambda values: float(np.mean(values))
        scale_value = float(np.std(before, ddof=1)) if before.size > 1 else 0.0
    elif statistic == "quantile":
        if not 0.0 < quantile < 1.0:
            raise ValueError("tail contrast quantile must be between zero and one")
        center = lambda values: float(np.quantile(values, quantile, method="linear"))
        scale_value = float(stats.median_abs_deviation(before, scale="normal", nan_policy="omit"))
    else:
        raise ValueError(f"unsupported contrast statistic {statistic}")
    observed = center(after) - center(before)
    pooled = np.concatenate((before, after))
    extreme = 0
    exact_count = math.comb(int(pooled.size), int(before.size))
    if exact_count <= resamples:
        all_indices = set(range(int(pooled.size)))
        for first_group in itertools.combinations(range(int(pooled.size)), int(before.size)):
            first_idx = np.fromiter(first_group, dtype=np.int64)
            first_mask = np.zeros(pooled.size, dtype=bool)
            first_mask[first_idx] = True
            delta = center(pooled[~first_mask]) - center(pooled[first_mask])
            if abs(delta) >= abs(observed) - 1e-15:
                extreme += 1
        p_value = extreme / exact_count
    else:
        rng = np.random.default_rng(seed)
        for _ in range(resamples):
            shuffled = rng.permutation(pooled)
            delta = center(shuffled[before.size:]) - center(shuffled[:before.size])
            if abs(delta) >= abs(observed) - 1e-15:
                extreme += 1
        p_value = (extreme + 1) / (resamples + 1)
    scale_floor = max(abs(center(before)) * relative_scale_floor, absolute_scale_floor)
    scale = max(scale_value, scale_floor)
    return ContrastResult(observed, observed / scale, p_value, center(before), center(after),
                          int(before.size), int(after.size), scale)


def contrast_score(result: ContrastResult, *, p_floor: float) -> float:
    """Effect magnitude times evidence; p is not interpreted as root-cause probability."""
    p = min(1.0, max(p_floor, result.p_value))
    return abs(result.normalized_effect) * max(0.0, -math.log10(p))

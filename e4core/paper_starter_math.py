"""Standalone mathematical kernels for paper-family starter operators.

These are transparent reconstructions of method families, not exact replications
of paper equations: the supplied starter material does not specify those equations.
All decision thresholds are caller-supplied. Results are immutable and expose
abstention explicitly so callers can preserve uncertainty at the operator edge.
"""

from __future__ import annotations

import math
import warnings
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Hashable, Mapping, Sequence

import numpy as np
from scipy import stats
from scipy.optimize import nnls
from sklearn.tree import DecisionTreeRegressor

from .contracts import _freeze_mapping
from .numerics import permutation_contrast, stable_seed


@dataclass(frozen=True, slots=True)
class MathResult:
    status: str
    reason: str | None
    effect: float | None
    p_value: float | None
    support_before: int
    support_after: int
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.support_before < 0 or self.support_after < 0:
            raise ValueError("math result support cannot be negative")
        if self.effect is not None and not math.isfinite(self.effect):
            raise ValueError("math result effect must be finite or missing")
        if self.p_value is not None and (not math.isfinite(self.p_value) or not 0 <= self.p_value <= 1):
            raise ValueError("math result p-value must be a finite probability or missing")
        object.__setattr__(self, "details", _freeze_mapping(self.details))


@dataclass(frozen=True, slots=True)
class ScoringResult:
    status: str
    reason: str | None
    scores: tuple[tuple[str, float], ...]
    ranked: tuple[str, ...]
    support: int
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.support < 0 or any(not math.isfinite(value) for _, value in self.scores):
            raise ValueError("scoring result support and scores must be finite and valid")
        object.__setattr__(self, "details", _freeze_mapping(self.details))


@dataclass(frozen=True, slots=True)
class VoteRecord:
    """One scorer's candidate scores and trusted data/computation fingerprint."""

    family: str
    fingerprint: str
    scores: tuple[tuple[str, float], ...]

    def __post_init__(self) -> None:
        if not self.family or not self.fingerprint:
            raise ValueError("vote family and fingerprint must be nonempty")
        items = tuple((str(key), float(value)) for key, value in self.scores)
        if len({key for key, _ in items}) != len(items):
            raise ValueError("a vote cannot repeat a candidate")
        if any(not math.isfinite(value) or value < 0 for _, value in items):
            raise ValueError("vote scores must be finite and nonnegative")
        object.__setattr__(self, "scores", items)


@dataclass(frozen=True, slots=True)
class FlowFitResult:
    status: str
    reason: str | None
    contributions_before: tuple[tuple[str, float], ...]
    contributions_after: tuple[tuple[str, float], ...]
    changes: tuple[tuple[str, float], ...]
    residual_before: float | None
    residual_after: float | None
    condition_number: float | None
    support: int
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.support < 0:
            raise ValueError("flow result support cannot be negative")
        for value in (self.residual_before, self.residual_after, self.condition_number):
            if value is not None and not math.isfinite(value):
                raise ValueError("flow diagnostics must be finite or missing")
        for group in (self.contributions_before, self.contributions_after, self.changes):
            if any(not math.isfinite(value) for _, value in group):
                raise ValueError("flow contributions must be finite")
        object.__setattr__(self, "details", _freeze_mapping(self.details))


def _finite_1d(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    if array.ndim != 1:
        raise ValueError("samples must be one-dimensional")
    return array[np.isfinite(array)]


def welch_test(before: Sequence[float], after: Sequence[float], *,
               minimum_samples: int) -> MathResult:
    """Welch two-sample t test with signed pooled standardized difference.

    The effect is the mean(after)-mean(before), divided by pooled within-group
    standard deviation; it is descriptive and is not a root-cause probability.
    """
    if minimum_samples < 2:
        raise ValueError("minimum_samples must be at least two")
    a, b = _finite_1d(before), _finite_1d(after)
    if a.size < minimum_samples or b.size < minimum_samples:
        return MathResult("abstain", "insufficient_finite_samples", None, None,
                          int(a.size), int(b.size))
    if a.size < 2 or b.size < 2:
        return MathResult("abstain", "insufficient_variance_support", None, None,
                          int(a.size), int(b.size))
    # Scale before centering so finite observations near float64's limit do not
    # overflow in variance or in the Welch degrees-of-freedom squares. The test
    # statistic, p-value, and standardized effect are invariant to this scale.
    magnitude = float(max(np.max(np.abs(a)), np.max(np.abs(b))))
    if magnitude == 0:
        return MathResult("abstain", "zero_or_invalid_pooled_variance", None, None,
                          int(a.size), int(b.size), {"mean_difference": 0.0})
    an, bn = a / magnitude, b / magnitude
    origin = float(an[0])
    ac, bc = an - origin, bn - origin
    mean_a_offset, mean_b_offset = float(np.mean(ac)), float(np.mean(bc))
    delta_scaled = mean_b_offset - mean_a_offset
    var_a, var_b = float(np.var(ac, ddof=1)), float(np.var(bc, ddof=1))
    pooled_var = ((a.size - 1) * var_a + (b.size - 1) * var_b) / (a.size + b.size - 2)

    def actual_difference_or_none(value: float) -> float | None:
        if not math.isfinite(value) or (value != 0 and abs(value) > np.finfo(float).max / magnitude):
            return None
        result = value * magnitude
        return float(result) if math.isfinite(result) else None

    mean_difference = actual_difference_or_none(delta_scaled)
    if (not all(math.isfinite(value) for value in (delta_scaled, var_a, var_b, pooled_var)) or
            pooled_var <= 0):
        return MathResult("abstain", "zero_or_invalid_pooled_variance", None, None,
                          int(a.size), int(b.size), {"mean_difference": mean_difference})
    variance_a_mean = var_a / a.size
    variance_b_mean = var_b / b.size
    standard_error_squared = variance_a_mean + variance_b_mean
    if not math.isfinite(standard_error_squared) or standard_error_squared <= 0:
        return MathResult("abstain", "invalid_welch_standard_error", None, None,
                          int(a.size), int(b.size))
    statistic = delta_scaled / math.sqrt(standard_error_squared)
    # Algebraically divide numerator and denominator by the largest variance
    # contribution; this avoids squaring tiny or huge intermediate magnitudes.
    df_scale = max(variance_a_mean, variance_b_mean)
    if df_scale <= 0 or not math.isfinite(df_scale):
        return MathResult("abstain", "invalid_welch_degrees_of_freedom", None, None,
                          int(a.size), int(b.size), {"mean_difference": mean_difference})
    scaled_a, scaled_b = variance_a_mean / df_scale, variance_b_mean / df_scale
    df_denominator = scaled_a ** 2 / (a.size - 1) + scaled_b ** 2 / (b.size - 1)
    degrees_freedom = (standard_error_squared / df_scale) ** 2 / df_denominator
    if not math.isfinite(statistic) or not math.isfinite(degrees_freedom) or degrees_freedom <= 0:
        return MathResult("abstain", "invalid_welch_test_statistic", None, None,
                          int(a.size), int(b.size), {"mean_difference": mean_difference})
    p = float(2.0 * stats.t.sf(abs(statistic), degrees_freedom))
    if not math.isfinite(p) or not 0 <= p <= 1:
        return MathResult("abstain", "nonfinite_test_result", None, None,
                          int(a.size), int(b.size), {"mean_difference": mean_difference})
    effect = delta_scaled / math.sqrt(pooled_var)
    if not math.isfinite(effect) or not 0.0 <= p <= 1.0:
        return MathResult("abstain", "nonfinite_effect_or_invalid_probability", None, None,
                          int(a.size), int(b.size), {"mean_difference": mean_difference})
    return MathResult("ok", None, effect, p,
                      int(a.size), int(b.size), {"mean_difference": mean_difference,
                      "effect": "pooled standardized mean difference (not bias-corrected)",
                      "test": "Welch unequal-variance two-sided t-test"})


def permutation_test(before: Sequence[float], after: Sequence[float], *, seed: int,
                     resamples: int, minimum_blocks: int,
                     relative_scale_floor: float,
                     absolute_scale_floor: float,
                     statistic: str, quantile: float) -> MathResult:
    """Adapter to E4's existing pooled permutation test over independent units."""
    if resamples < 1 or minimum_blocks < 1:
        raise ValueError("resamples and minimum_blocks must be positive")
    a, b = _finite_1d(before), _finite_1d(after)
    if a.size < minimum_blocks or b.size < minimum_blocks:
        return MathResult("abstain", "insufficient_finite_blocks", None, None,
                          int(a.size), int(b.size))
    result = permutation_contrast(a, b, seed=seed, resamples=resamples,
        statistic=statistic, minimum_blocks=minimum_blocks,
        relative_scale_floor=relative_scale_floor,
        absolute_scale_floor=absolute_scale_floor, quantile=quantile)
    if (result is None or not math.isfinite(result.normalized_effect) or
            not math.isfinite(result.p_value) or not 0.0 <= result.p_value <= 1.0):
        return MathResult("abstain", "permutation_test_unavailable", None, None,
                          int(a.size), int(b.size))
    return MathResult("ok", None, float(result.normalized_effect),
                      float(result.p_value), int(a.size), int(b.size),
                      {"raw_difference": float(result.raw_delta),
                       "scale": float(result.baseline_mad_scale), "test": "pooled two-sided permutation"})


def combine_votes(votes: Sequence[VoteRecord], *, family_weights: Mapping[str, float],
                  tie_tolerance: float) -> ScoringResult:
    """Normalize each independent scorer, average by family, then weighted-fuse.

    Fingerprints are trusted computation/data identities. A repeated fingerprint
    is counted once even if exposed under another operator alias; conflicting
    normalized payloads beyond tie_tolerance are rejected. Zero and unconfigured
    family votes are ignored. This is a family-balanced reconstruction, not an
    asserted paper formula.
    """
    if not math.isfinite(tie_tolerance) or tie_tolerance < 0:
        raise ValueError("tie_tolerance must be finite and nonnegative")
    weights = {str(key): float(value) for key, value in family_weights.items()}
    if any(not math.isfinite(value) or value < 0 for value in weights.values()):
        raise ValueError("family weights must be finite and nonnegative")
    unique: list[VoteRecord] = []
    fingerprints: dict[str, tuple[str, dict[str, float]]] = {}
    for vote in votes:
        if vote.family not in weights or weights[vote.family] <= 0:
            continue
        raw = dict(vote.scores)
        maximum = max(raw.values(), default=0.0)
        if maximum <= 0:
            continue
        normalized = {key: value / maximum for key, value in raw.items()}
        prior = fingerprints.get(vote.fingerprint)
        if prior is not None:
            prior_family, prior_scores = prior
            if prior_family != vote.family:
                raise ValueError("duplicate fingerprint has conflicting trusted family")
            all_keys = set(prior_scores) | set(normalized)
            if any(abs(prior_scores.get(key, 0.0) - normalized.get(key, 0.0)) > tie_tolerance
                   for key in all_keys):
                raise ValueError("duplicate fingerprint has conflicting normalized scores")
            continue
        fingerprints[vote.fingerprint] = (vote.family, normalized)
        unique.append(vote)
    by_family: dict[str, list[dict[str, float]]] = {}
    candidates = sorted({key for vote in unique for key, _ in vote.scores})
    for vote in unique:
        by_family.setdefault(vote.family, []).append(fingerprints[vote.fingerprint][1])
    active = {family: weight for family, weight in weights.items()
              if weight > 0 and family in by_family}
    total_weight = sum(active.values())
    if not candidates or not active or total_weight <= 0:
        return ScoringResult("abstain", "no_usable_independent_votes", (), (), 0,
                             {"deduplicated_votes": len(unique)})
    family_scores: dict[str, dict[str, float]] = {}
    for family, scorers in by_family.items():
        family_scores[family] = {candidate: sum(row.get(candidate, 0.0) for row in scorers) / len(scorers)
                                 for candidate in candidates}
    fused = {candidate: sum(active[family] * family_scores[family][candidate]
                            for family in active) / total_weight for candidate in candidates}
    ordered = sorted(candidates, key=lambda item: (-fused[item], item))
    top, second = fused[ordered[0]], fused[ordered[1]] if len(ordered) > 1 else -math.inf
    tied = len(ordered) > 1 and abs(top - second) <= tie_tolerance
    return ScoringResult("ambiguous" if tied else "ok",
        "contradictory_or_tied_votes" if tied else None,
        tuple((candidate, float(fused[candidate])) for candidate in candidates),
        tuple(ordered), len(unique),
        {"deduplicated_votes": len(unique), "family_count": len(active),
         "families": tuple(sorted(active)), "family_scores": tuple(
             (family, tuple((candidate, family_scores[family][candidate]) for candidate in candidates))
             for family in sorted(active)), "normalization": "per-scorer maximum; within-family mean; weighted family mean"})


def tree_attribution(features_before: Sequence[Sequence[float]],
                     kpi_before: Sequence[float],
                     features_alert: Sequence[Sequence[float]],
                     kpi_alert: Sequence[float], *, feature_names: Sequence[str],
                     max_depth: int, min_samples_leaf: int, minimum_samples: int,
                     validation_fraction: float, minimum_validation_samples: int,
                     minimum_validation_r2: float,
                     maximum_validation_normalized_error: float,
                     residual_scale_floor: float,
                     minimum_excess_nll_per_observation: float,
                     seed: int) -> ScoringResult:
    """Fit historical KPI regression and score alert residual likelihood.

    The ordered baseline window is split into a training prefix and held-out
    validation suffix. Validation R² and normalized RMSE must meet caller limits;
    Gaussian residual scale comes only from held-out residuals, with a caller
    floor. The anomaly score is the mean alert negative log likelihood minus the
    mean held-out-baseline negative log likelihood, so unequal window sizes do
    not inflate the score. Per-row residual likelihoods and original row indices
    let downstream cohort operators aggregate by actual membership.
    """
    if max_depth < 1 or min_samples_leaf < 1 or minimum_samples < 2:
        raise ValueError("tree depth and sample limits must be positive")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    if residual_scale_floor <= 0 or not math.isfinite(residual_scale_floor):
        raise ValueError("residual_scale_floor must be finite and positive")
    if (minimum_excess_nll_per_observation < 0 or
            not math.isfinite(minimum_excess_nll_per_observation)):
        raise ValueError("minimum_excess_nll_per_observation must be finite and nonnegative")
    if not math.isfinite(validation_fraction) or not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be finite and between zero and one")
    if minimum_validation_samples < 2:
        raise ValueError("minimum_validation_samples must be at least two")
    if not math.isfinite(minimum_validation_r2):
        raise ValueError("minimum_validation_r2 must be finite")
    if (not math.isfinite(maximum_validation_normalized_error) or
            maximum_validation_normalized_error < 0):
        raise ValueError("maximum_validation_normalized_error must be finite and nonnegative")
    xb, xa = np.asarray(features_before, dtype=float), np.asarray(features_alert, dtype=float)
    yb, ya = np.asarray(kpi_before, dtype=float), np.asarray(kpi_alert, dtype=float)
    names = tuple(str(name) for name in feature_names)
    if (xb.ndim != 2 or xa.ndim != 2 or yb.ndim != 1 or ya.ndim != 1 or
            xb.shape[1] != len(names) or xa.shape[1] != len(names) or
            xb.shape[0] != yb.size or xa.shape[0] != ya.size):
        raise ValueError("feature and KPI dimensions disagree")
    original_before_indices = np.arange(yb.size, dtype=int)
    original_alert_indices = np.arange(ya.size, dtype=int)
    valid_before = np.isfinite(xb).all(axis=1) & np.isfinite(yb)
    valid_alert = np.isfinite(xa).all(axis=1) & np.isfinite(ya)
    original_before_indices = original_before_indices[valid_before]
    original_alert_indices = original_alert_indices[valid_alert]
    xb, yb = xb[valid_before], yb[valid_before]
    xa, ya = xa[valid_alert], ya[valid_alert]
    split = int(math.floor(xb.shape[0] * (1.0 - validation_fraction)))
    if (xb.shape[0] < minimum_samples or xa.shape[0] < 1 or split < minimum_samples or
            xb.shape[0] - split < minimum_validation_samples):
        return ScoringResult("abstain", "insufficient_finite_rows", (), (),
                             int(xb.shape[0] + xa.shape[0]))
    if not names:
        return ScoringResult("abstain", "no_features", (), (), int(xb.shape[0] + xa.shape[0]))

    x_train, y_train = xb[:split], yb[:split]
    x_valid, y_valid = xb[split:], yb[split:]
    validation_row_indices = original_before_indices[split:]
    baseline_min = np.min(x_train, axis=0)
    baseline_max = np.max(x_train, axis=0)
    in_support = np.logical_and(xa >= baseline_min, xa <= baseline_max).all(axis=1)
    support_coverage = float(np.mean(in_support))
    per_feature_outside = tuple((name, int(np.count_nonzero(
        (xa[:, index] < baseline_min[index]) | (xa[:, index] > baseline_max[index]))))
        for index, name in enumerate(names))
    if not np.any(in_support):
        return ScoringResult("abstain", "alert_outside_baseline_feature_support", (), (),
            int(xb.shape[0] + xa.shape[0]), {"alert_support_coverage": support_coverage,
            "baseline_feature_ranges": tuple((name, float(baseline_min[index]), float(baseline_max[index]))
                for index, name in enumerate(names)), "out_of_support_counts": per_feature_outside})
    x_scored, y_scored = xa[in_support], ya[in_support]
    alert_row_indices = original_alert_indices[in_support]

    def model_for(data: np.ndarray) -> DecisionTreeRegressor:
        model = DecisionTreeRegressor(max_depth=max_depth, min_samples_leaf=min_samples_leaf,
                                      random_state=seed)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            model.fit(data, y_train)
        return model

    try:
        full_model = model_for(x_train)
        validation_residual = y_valid - full_model.predict(x_valid)
        with np.errstate(over="ignore", invalid="ignore"):
            target_scale = float(np.std(y_valid, ddof=1))
            validation_sse = float(np.dot(validation_residual, validation_residual))
            centered = y_valid - np.mean(y_valid)
            validation_sst = float(np.dot(centered, centered))
        if not math.isfinite(validation_sse) or not math.isfinite(validation_sst):
            return ScoringResult("abstain", "nonfinite_baseline_validation_metrics", (), (),
                                 int(xb.shape[0] + xa.shape[0]))
        if not math.isfinite(target_scale):
            return ScoringResult("abstain", "nonfinite_validation_target_scale", (), (),
                                 int(xb.shape[0] + xa.shape[0]))
        if validation_sst <= 0:
            return ScoringResult("abstain", "constant_validation_kpi", (), (),
                                 int(xb.shape[0] + xa.shape[0]))
        validation_r2 = 1.0 - validation_sse / validation_sst
        validation_rmse = math.sqrt(validation_sse / y_valid.size)
        normalized_error = validation_rmse / max(target_scale, residual_scale_floor)
        if (not math.isfinite(validation_r2) or not math.isfinite(normalized_error) or
                validation_r2 < minimum_validation_r2 or
                normalized_error > maximum_validation_normalized_error):
            return ScoringResult("abstain", "baseline_validation_quality_below_threshold", (), (),
                int(xb.shape[0] + xa.shape[0]), {"validation_r2": validation_r2,
                "validation_normalized_error": normalized_error,
                "minimum_validation_r2": minimum_validation_r2,
                "maximum_validation_normalized_error": maximum_validation_normalized_error})
        with np.errstate(over="ignore", invalid="ignore"):
            validation_residual_location = float(np.mean(validation_residual))
            validation_residual_scale = float(np.std(validation_residual, ddof=1))
        if not math.isfinite(validation_residual_location):
            return ScoringResult("abstain", "nonfinite_heldout_residual_location", (), (),
                                 int(xb.shape[0] + xa.shape[0]))
        residual_scale = max(validation_residual_scale, residual_scale_floor)
        if not math.isfinite(residual_scale):
            return ScoringResult("abstain", "nonfinite_heldout_residual_scale", (), (),
                                 int(xb.shape[0] + xa.shape[0]))
        constant = math.log(residual_scale) + 0.5 * math.log(2.0 * math.pi)
    except (ValueError, FloatingPointError, OverflowError, RuntimeError, np.linalg.LinAlgError):
        return ScoringResult("abstain", "baseline_regression_numerical_failure", (), (),
                             int(xb.shape[0] + xa.shape[0]))

    def residual_log_likelihood(residual: np.ndarray) -> tuple[float, ...] | None:
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            log_values = -constant - 0.5 * np.square(
                (residual - validation_residual_location) / residual_scale)
        if not np.isfinite(log_values).all():
            return None
        return tuple(float(value) for value in log_values)

    try:
        validation_predictions = full_model.predict(x_valid)
        alert_predictions = full_model.predict(x_scored)
        validation_residual = y_valid - validation_predictions
        alert_residual = y_scored - alert_predictions
        validation_ll = residual_log_likelihood(validation_residual)
        alert_ll = residual_log_likelihood(alert_residual)
    except (ValueError, FloatingPointError, OverflowError, RuntimeError, np.linalg.LinAlgError):
        validation_ll, alert_ll = None, None
        validation_residual, alert_residual = np.asarray([]), np.asarray([])
    if validation_ll is None or alert_ll is None:
        return ScoringResult("abstain", "nonfinite_alert_likelihood", (), (),
                             int(xb.shape[0] + xa.shape[0]))
    try:
        baseline_nll = -math.fsum(validation_ll) / len(validation_ll)
        alert_nll = -math.fsum(alert_ll) / len(alert_ll)
        excess_nll = alert_nll - baseline_nll
    except (OverflowError, ValueError):
        return ScoringResult("abstain", "nonfinite_excess_likelihood", (), (),
                             int(xb.shape[0] + xa.shape[0]))
    if not all(math.isfinite(value) for value in (baseline_nll, alert_nll, excess_nll)):
        return ScoringResult("abstain", "nonfinite_excess_likelihood", (), (),
                             int(xb.shape[0] + xa.shape[0]))
    details = {"model": "DecisionTreeRegressor historical KPI",
        "residual_model": "Gaussian, held-out residual location and scale; caller scale floor",
        "residual_scale": residual_scale, "residual_location": validation_residual_location,
        "baseline_nll_per_observation": baseline_nll,
        "alert_nll_per_observation": alert_nll,
        "excess_nll_per_observation": excess_nll,
        "validation_residuals": tuple(float(value) for value in validation_residual),
        "validation_log_likelihoods": validation_ll,
        "validation_row_indices": tuple(int(value) for value in validation_row_indices),
        "alert_residuals": tuple(float(value) for value in alert_residual),
        "alert_log_likelihoods": alert_ll,
        "alert_row_indices": tuple(int(value) for value in alert_row_indices),
        "validation_r2": validation_r2, "validation_normalized_error": normalized_error,
        "validation_samples": int(y_valid.size), "seed": seed,
        "baseline_feature_ranges": tuple((name, float(baseline_min[index]), float(baseline_max[index]))
            for index, name in enumerate(names)), "out_of_support_counts": per_feature_outside,
        "alert_support_coverage": support_coverage,
        "minimum_excess_nll_per_observation": minimum_excess_nll_per_observation,
        "max_depth": max_depth, "min_samples_leaf": min_samples_leaf, "causal": False}
    if excess_nll < minimum_excess_nll_per_observation:
        return ScoringResult("abstain", "alert_nll_not_excessive_over_baseline", (), (),
                             int(xb.shape[0] + xa.shape[0]), details)
    status = "partial" if support_coverage < 1 else "ok"
    reason = "alert_rows_outside_baseline_feature_support" if support_coverage < 1 else None
    return ScoringResult(status, reason,
                         (("alert_excess_nll_per_observation", excess_nll),),
                         ("alert_excess_nll_per_observation",),
                         int(xb.shape[0] + xa.shape[0]), details)


def tree_prediction_contributions(features_before: Sequence[Sequence[float]],
                                 kpi_before: Sequence[float],
                                 features_alert: Sequence[Sequence[float]],
                                 kpi_alert: Sequence[float], *,
                                 feature_names: Sequence[str],
                                 feature_groups: Mapping[str, Sequence[str]],
                                 max_depth: int, min_samples_leaf: int,
                                 minimum_samples: int, validation_fraction: float,
                                 minimum_validation_samples: int,
                                 minimum_validation_r2: float,
                                 maximum_validation_normalized_error: float,
                                 residual_scale_floor: float,
                                 minimum_observed_kpi_change: float,
                                 minimum_contribution_fraction: float,
                                 seed: int) -> ScoringResult:
    """Fit one historical KPI model per component feature group.

    Each component model predicts the KPI from its own declared metric group.
    Models are trained on a historical prefix and validated on the heldout suffix.
    The component score is its aligned predicted KPI shift, provided it has
    predictive support, the observed KPI shift exceeds its caller gate, and the
    predicted component shift explains the caller-declared fraction of that
    observed shift. Correlated components can explain the same shift; scores are
    predictive associations and are not summed as causal credit.
    """
    if max_depth < 1 or min_samples_leaf < 1 or minimum_samples < 2:
        raise ValueError("tree depth and sample limits must be positive")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    if not math.isfinite(validation_fraction) or not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be finite and between zero and one")
    if minimum_validation_samples < 2:
        raise ValueError("minimum_validation_samples must be at least two")
    if not math.isfinite(minimum_validation_r2):
        raise ValueError("minimum_validation_r2 must be finite")
    if (not math.isfinite(maximum_validation_normalized_error) or
            maximum_validation_normalized_error < 0):
        raise ValueError("maximum_validation_normalized_error must be finite and nonnegative")
    if residual_scale_floor <= 0 or not math.isfinite(residual_scale_floor):
        raise ValueError("residual_scale_floor must be finite and positive")
    if minimum_observed_kpi_change < 0 or not math.isfinite(minimum_observed_kpi_change):
        raise ValueError("minimum_observed_kpi_change must be finite and nonnegative")
    if (not 0 <= minimum_contribution_fraction <= 1 or
            not math.isfinite(minimum_contribution_fraction)):
        raise ValueError("minimum_contribution_fraction must be finite and in [0,1]")
    names = tuple(str(name) for name in feature_names)
    groups = {str(key): tuple(str(name) for name in columns)
              for key, columns in feature_groups.items()}
    if (not names or not groups or any(not key or not columns or len(set(columns)) != len(columns) or
            any(name not in names for name in columns) for key, columns in groups.items())):
        raise ValueError("feature_groups must map nonempty candidate IDs to known unique feature names")
    xb, xa = np.asarray(features_before, dtype=float), np.asarray(features_alert, dtype=float)
    yb, ya = np.asarray(kpi_before, dtype=float), np.asarray(kpi_alert, dtype=float)
    if (xb.ndim != 2 or xa.ndim != 2 or yb.ndim != 1 or ya.ndim != 1 or
            xb.shape[1] != len(names) or xa.shape[1] != len(names) or
            xb.shape[0] != yb.size or xa.shape[0] != ya.size):
        raise ValueError("feature and KPI dimensions disagree")
    valid_before = np.isfinite(xb).all(axis=1) & np.isfinite(yb)
    valid_alert = np.isfinite(xa).all(axis=1) & np.isfinite(ya)
    xb, yb = xb[valid_before], yb[valid_before]
    xa, ya = xa[valid_alert], ya[valid_alert]
    split = int(math.floor(xb.shape[0] * (1.0 - validation_fraction)))
    support = int(xb.shape[0] + xa.shape[0])
    if (xb.shape[0] < minimum_samples or xa.shape[0] < 1 or split < minimum_samples or
            xb.shape[0] - split < minimum_validation_samples):
        return ScoringResult("abstain", "insufficient_finite_rows", (), (), support)
    x_train, y_train = xb[:split], yb[:split]
    x_valid, y_valid = xb[split:], yb[split:]
    with np.errstate(over="ignore", invalid="ignore"):
        overall_observed_delta = float(np.mean(ya) - np.mean(y_valid))
    if not math.isfinite(overall_observed_delta):
        return ScoringResult("abstain", "nonfinite_observed_kpi_shift", (), (), support)
    if abs(overall_observed_delta) < minimum_observed_kpi_change:
        return ScoringResult("abstain", "observed_kpi_shift_below_threshold", (), (), support,
                             {"observed_kpi_delta": overall_observed_delta})
    reports = []
    scores = []
    unavailable = []
    for group_id, columns in sorted(groups.items()):
        indices = [names.index(name) for name in columns]
        train_group, valid_group, alert_group = x_train[:, indices], x_valid[:, indices], xa[:, indices]
        lower, upper = np.min(train_group, axis=0), np.max(train_group, axis=0)
        covered = ((alert_group >= lower) & (alert_group <= upper)).all(axis=1)
        if not np.any(covered):
            unavailable.append(group_id)
            scores.append((group_id, 0.0))
            reports.append((group_id, {"status": "abstain", "reason": "alert_group_outside_baseline_support",
                "features": columns, "alert_support_coverage": 0.0}))
            continue
        alert_group = alert_group[covered]
        alert_target = ya[covered]
        model = DecisionTreeRegressor(max_depth=max_depth, min_samples_leaf=min_samples_leaf,
                                      random_state=seed)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                model.fit(train_group, y_train)
            valid_prediction = model.predict(valid_group)
            alert_prediction = model.predict(alert_group)
            valid_residual = y_valid - valid_prediction
            with np.errstate(over="ignore", invalid="ignore"):
                target_scale = float(np.std(y_valid, ddof=1))
                valid_sse = float(np.dot(valid_residual, valid_residual))
                centered_target = y_valid - np.mean(y_valid)
                valid_sst = float(np.dot(centered_target, centered_target))
            if (not math.isfinite(target_scale) or not math.isfinite(valid_sse) or
                    not math.isfinite(valid_sst) or valid_sst <= 0):
                raise FloatingPointError("nonfinite or constant validation target")
            validation_r2 = 1.0 - valid_sse / valid_sst
            validation_error = math.sqrt(valid_sse / y_valid.size) / max(target_scale, residual_scale_floor)
            with np.errstate(over="ignore", invalid="ignore"):
                observed_delta = float(np.mean(alert_target) - np.mean(y_valid))
                predicted_delta = float(np.mean(alert_prediction) - np.mean(valid_prediction))
            if (not math.isfinite(validation_r2) or not math.isfinite(validation_error) or
                    not math.isfinite(observed_delta) or not math.isfinite(predicted_delta)):
                raise FloatingPointError("nonfinite per-group validation or shift")
            valid_model = (validation_r2 >= minimum_validation_r2 and
                           validation_error <= maximum_validation_normalized_error)
            effect_large_enough = abs(observed_delta) >= minimum_observed_kpi_change
            direction_matches = predicted_delta * observed_delta > 0
            contribution_fraction = (abs(predicted_delta) / abs(observed_delta)
                                     if observed_delta != 0 else 0.0)
            score = (abs(predicted_delta) if valid_model and effect_large_enough and
                     direction_matches and contribution_fraction >= minimum_contribution_fraction else 0.0)
            if not math.isfinite(score):
                raise FloatingPointError("nonfinite component contribution")
            reports.append((group_id, {"status": "ok" if valid_model else "abstain",
                "reason": None if valid_model else "component_model_validation_quality_below_threshold",
                "features": columns, "validation_r2": validation_r2,
                "validation_normalized_error": validation_error,
                "observed_kpi_delta": observed_delta, "predicted_kpi_delta": predicted_delta,
                "contribution_fraction": contribution_fraction,
                "alert_support_coverage": float(np.mean(covered)), "aligned_score": score}))
            scores.append((group_id, score))
        except (ValueError, FloatingPointError, OverflowError, RuntimeError, np.linalg.LinAlgError):
            unavailable.append(group_id)
            scores.append((group_id, 0.0))
            reports.append((group_id, {"status": "abstain", "reason": "component_model_numerical_failure",
                                       "features": columns}))
    if not any(score > 0 for _, score in scores):
        return ScoringResult("abstain", "no_validated_component_explains_observed_shift",
            tuple(scores), (), support, {"observed_kpi_delta": overall_observed_delta,
            "feature_group_models": tuple(reports), "unavailable_groups": tuple(unavailable),
            "causal": False})
    ranked = tuple(key for key, score in sorted(scores, key=lambda item: (-item[1], item[0])) if score > 0)
    status = "ambiguous" if len(ranked) > 1 else "ok"
    return ScoringResult(status, "multiple_groups_predict_shift" if status == "ambiguous" else None,
        tuple(scores), ranked, support, {"model": "component-specific historical DecisionTreeRegressor",
        "observed_kpi_delta": overall_observed_delta,
        "feature_group_models": tuple(reports), "unavailable_groups": tuple(unavailable),
        "contribution_interpretation": "aligned predictive KPI shifts; correlated groups can overlap; no causal sum",
        "causal": False})


def fit_flow_contributions(features_before: Sequence[Sequence[float]],
                           target_before: Sequence[float],
                           features_after: Sequence[Sequence[float]],
                           target_after: Sequence[float], *, component_names: Sequence[str],
                           fit_mode: str, validation_fraction: float,
                           minimum_validation_samples: int,
                           minimum_validation_skill: float,
                           maximum_validation_normalized_error: float,
                           residual_scale_floor: float,
                           ridge: float, max_condition: float,
                           maximum_relative_residual: float,
                           minimum_rows: int) -> FlowFitResult:
    """Validate and fit an explicit historical or pooled NNLS flow model.

    Each feature matrix has samples×components shape; each target is one observed
    nonnegative flow/KPI per row. First, a historical training prefix is checked
    against a held-out baseline suffix and a training-mean predictor. In
    `historical` mode coefficients use the training prefix; in `pooled` mode the
    validated model is refit on both windows using equal row weights. Window
    contributions are coefficient×feature mean. Targets are responses only and
    never added to the feature matrix. This is a reconstructed additive model.
    """
    if ridge < 0 or not math.isfinite(ridge):
        raise ValueError("ridge must be finite and nonnegative")
    if max_condition <= 0 or not math.isfinite(max_condition):
        raise ValueError("max_condition must be finite and positive")
    if maximum_relative_residual < 0 or not math.isfinite(maximum_relative_residual):
        raise ValueError("maximum_relative_residual must be finite and nonnegative")
    if minimum_rows < 1:
        raise ValueError("minimum_rows must be positive")
    if fit_mode not in {"historical", "pooled"}:
        raise ValueError("fit_mode must be 'historical' or 'pooled'")
    if not math.isfinite(validation_fraction) or not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be finite and between zero and one")
    if minimum_validation_samples < 2:
        raise ValueError("minimum_validation_samples must be at least two")
    if not math.isfinite(minimum_validation_skill):
        raise ValueError("minimum_validation_skill must be finite")
    if (not math.isfinite(maximum_validation_normalized_error) or
            maximum_validation_normalized_error < 0):
        raise ValueError("maximum_validation_normalized_error must be finite and nonnegative")
    if residual_scale_floor <= 0 or not math.isfinite(residual_scale_floor):
        raise ValueError("residual_scale_floor must be finite and positive")
    xb, xa = np.asarray(features_before, dtype=float), np.asarray(features_after, dtype=float)
    yb, ya = np.asarray(target_before, dtype=float), np.asarray(target_after, dtype=float)
    names = tuple(str(name) for name in component_names)
    if (xb.ndim != 2 or xa.ndim != 2 or yb.ndim != 1 or ya.ndim != 1 or
            xb.shape[1] != len(names) or xa.shape[1] != len(names) or
            xb.shape[0] != yb.size or xa.shape[0] != ya.size):
        raise ValueError("each feature matrix must be samples×components and align with its target")
    support = int(xb.shape[0] + xa.shape[0])
    split = int(math.floor(xb.shape[0] * (1.0 - validation_fraction)))
    if (xb.shape[0] < minimum_rows or xa.shape[0] < minimum_rows or not names or
            split < minimum_rows or xb.shape[0] - split < minimum_validation_samples):
        return FlowFitResult("abstain", "insufficient_rows_or_components", (), (), (), None, None,
                             None, support)
    if any(not np.isfinite(array).all() for array in (xb, xa, yb, ya)):
        return FlowFitResult("abstain", "nonfinite_flow_data", (), (), (), None, None,
                             None, support)
    if any(np.any(array < 0) for array in (xb, xa, yb, ya)):
        return FlowFitResult("abstain", "negative_flow_data", (), (), (), None, None,
                             None, support)
    x_train, y_train = xb[:split], yb[:split]
    x_valid, y_valid = xb[split:], yb[split:]
    if x_train.shape[0] < x_train.shape[1]:
        return FlowFitResult("abstain", "underdetermined_design", (), (), (), None, None,
                             None, support, {"rows": int(x_train.shape[0]), "columns": int(x_train.shape[1])})
    try:
        validation_rank = int(np.linalg.matrix_rank(x_train))
        validation_condition = float(np.linalg.cond(x_train))
    except (ValueError, FloatingPointError, OverflowError, np.linalg.LinAlgError):
        return FlowFitResult("abstain", "validation_design_numerical_failure", (), (), (), None, None,
                             None, support)
    if (validation_rank < x_train.shape[1] or not math.isfinite(validation_condition) or
            validation_condition > max_condition):
        return FlowFitResult("abstain", "collinear_or_ill_conditioned_design", (), (), (), None, None,
                             validation_condition if math.isfinite(validation_condition) else None, support,
                             {"rank": validation_rank, "columns": int(x_train.shape[1]),
                              "max_condition": max_condition})
    try:
        validation_fit = np.vstack((x_train, math.sqrt(ridge) * np.eye(x_train.shape[1]))) if ridge else x_train
        validation_target = np.concatenate((y_train, np.zeros(x_train.shape[1]))) if ridge else y_train
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            validation_coefficients, _ = nnls(validation_fit, validation_target)
            validation_residual = y_valid - x_valid @ validation_coefficients
            baseline_residual = y_valid - float(np.mean(y_train))
        with np.errstate(over="ignore", invalid="ignore"):
            model_sse = float(np.dot(validation_residual, validation_residual))
            baseline_sse = float(np.dot(baseline_residual, baseline_residual))
        if not math.isfinite(baseline_sse) or baseline_sse <= 0:
            return FlowFitResult("abstain", "constant_validation_baseline", (), (), (), None, None,
                                 validation_condition, support)
        validation_skill = 1.0 - model_sse / baseline_sse
        validation_rmse = math.sqrt(model_sse / y_valid.size)
        with np.errstate(over="ignore", invalid="ignore"):
            validation_target_scale = float(np.std(y_valid, ddof=1))
        validation_scale = max(validation_target_scale, residual_scale_floor)
        if not math.isfinite(validation_scale):
            return FlowFitResult("abstain", "nonfinite_baseline_validation_scale", (), (), (),
                                 None, None, validation_condition, support)
        validation_normalized_error = validation_rmse / validation_scale
        if not math.isfinite(validation_skill) or not math.isfinite(validation_normalized_error):
            return FlowFitResult("abstain", "nonfinite_baseline_validation_metrics", (), (), (),
                None, None, validation_condition, support,
                {"validation_skill_vs_training_mean": None,
                 "validation_normalized_error": None})
        if (validation_skill < minimum_validation_skill or
                validation_normalized_error > maximum_validation_normalized_error):
            return FlowFitResult("abstain", "baseline_validation_quality_below_threshold", (), (), (),
                None, None, validation_condition, support, {"validation_skill": validation_skill,
                "validation_normalized_error": validation_normalized_error,
                "minimum_validation_skill": minimum_validation_skill,
                "maximum_validation_normalized_error": maximum_validation_normalized_error})
    except (ValueError, FloatingPointError, OverflowError, RuntimeError, np.linalg.LinAlgError):
        return FlowFitResult("abstain", "baseline_validation_numerical_failure", (), (), (), None, None,
                             validation_condition, support)

    if fit_mode == "historical":
        fit_design, fit_target = x_train, y_train
    else:
        fit_design, fit_target = np.vstack((xb, xa)), np.concatenate((yb, ya))
    if fit_design.shape[0] < fit_design.shape[1]:
        return FlowFitResult("abstain", "underdetermined_fit_design", (), (), (), None, None,
                             validation_condition, support)
    try:
        fit_rank = int(np.linalg.matrix_rank(fit_design))
        condition = float(np.linalg.cond(fit_design))
    except (ValueError, FloatingPointError, OverflowError, np.linalg.LinAlgError):
        return FlowFitResult("abstain", "fit_design_numerical_failure", (), (), (), None, None,
                             None, support)
    if fit_rank < fit_design.shape[1] or not math.isfinite(condition) or condition > max_condition:
        return FlowFitResult("abstain", "collinear_or_ill_conditioned_fit_design", (), (), (), None, None,
                             condition if math.isfinite(condition) else None, support,
                             {"rank": fit_rank, "columns": int(fit_design.shape[1]),
                              "max_condition": max_condition})
    try:
        fit_matrix = np.vstack((fit_design, math.sqrt(ridge) * np.eye(fit_design.shape[1]))) if ridge else fit_design
        fit_target_augmented = np.concatenate((fit_target, np.zeros(fit_design.shape[1]))) if ridge else fit_target
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            coefficients, _ = nnls(fit_matrix, fit_target_augmented)
            residual_before = float(np.linalg.norm(xb @ coefficients - yb) /
                                    max(np.linalg.norm(yb), np.finfo(float).eps))
            residual_after = float(np.linalg.norm(xa @ coefficients - ya) /
                                   max(np.linalg.norm(ya), np.finfo(float).eps))
    except (ValueError, FloatingPointError, OverflowError, RuntimeError, np.linalg.LinAlgError):
        return FlowFitResult("abstain", "nnls_fit_numerical_failure", (), (), (), None, None,
                             condition, support)
    if not math.isfinite(residual_before) or not math.isfinite(residual_after):
        return FlowFitResult("abstain", "nonfinite_fit_residual", (), (), (), None, None,
                             condition, support)
    if max(residual_before, residual_after) > maximum_relative_residual:
        return FlowFitResult("abstain", "poor_fit_residual", (), (), (), residual_before,
                             residual_after, condition, support,
                             {"maximum_relative_residual": maximum_relative_residual})
    with np.errstate(over="ignore", invalid="ignore"):
        contribution_before = coefficients * np.mean(xb, axis=0)
        contribution_after = coefficients * np.mean(xa, axis=0)
        changes = contribution_after - contribution_before
    if not all(np.isfinite(array).all() for array in
               (coefficients, contribution_before, contribution_after, changes)):
        return FlowFitResult("abstain", "nonfinite_weighted_contributions", (), (), (),
                             residual_before, residual_after, condition, support)
    return FlowFitResult("ok", None,
        tuple((name, float(value)) for name, value in zip(names, contribution_before)),
        tuple((name, float(value)) for name, value in zip(names, contribution_after)),
        tuple((name, float(value)) for name, value in zip(names, changes)),
        residual_before, residual_after, condition, support,
        {"method": "declared nonnegative additive regression",
         "fit_mode": fit_mode,
         "ridge": ridge, "maximum_relative_residual": maximum_relative_residual,
         "validation_skill_vs_training_mean": validation_skill,
         "validation_normalized_error": validation_normalized_error,
         "validation_samples": int(y_valid.size),
         "support_before": int(xb.shape[0]), "support_after": int(xa.shape[0]),
         "coefficients": tuple((name, float(value)) for name, value in zip(names, coefficients)),
         "contribution_definition": "coefficient × window feature mean"})


def token_sequence_matches(tokens: Sequence[Hashable], patterns: Sequence[Sequence[Hashable]], *,
                           sequence_length: int, max_patterns: int,
                           max_pattern_length: int) -> tuple[tuple[int, int, int], ...]:
    """Aho–Corasick matching over typed token sequences, returning (pattern,start,end)."""
    if sequence_length < 0:
        raise ValueError("sequence_length must be nonnegative")
    if max_patterns < 0 or max_pattern_length < 1:
        raise ValueError("pattern limits must be nonnegative and positive respectively")
    if len(tokens) != sequence_length:
        raise ValueError("declared sequence_length does not match token count")
    pattern_list = [tuple(pattern) for pattern in patterns]
    if len(pattern_list) > max_patterns or any(len(pattern) > max_pattern_length for pattern in pattern_list):
        raise ValueError("pattern input exceeds caller-supplied limits")
    if any(not pattern for pattern in pattern_list):
        raise ValueError("empty token patterns are not supported")
    # Trie nodes are indexed arrays; failure links allow proper overlapping matches.
    next_nodes: list[dict[Hashable, int]] = [{}]
    fail = [0]
    output: list[list[int]] = [[]]
    for index, pattern in enumerate(pattern_list):
        node = 0
        for token in pattern:
            child = next_nodes[node].get(token)
            if child is None:
                child = len(next_nodes)
                next_nodes[node][token] = child
                next_nodes.append({})
                fail.append(0)
                output.append([])
            node = child
        output[node].append(index)
    queue: deque[int] = deque()
    for child in next_nodes[0].values():
        queue.append(child)
    while queue:
        node = queue.popleft()
        for token, child in next_nodes[node].items():
            queue.append(child)
            fallback = fail[node]
            while fallback and token not in next_nodes[fallback]:
                fallback = fail[fallback]
            fail[child] = next_nodes[fallback].get(token, 0)
            output[child].extend(output[fail[child]])
    matches: list[tuple[int, int, int]] = []
    node = 0
    for end_index, token in enumerate(tokens, start=1):
        while node and token not in next_nodes[node]:
            node = fail[node]
        node = next_nodes[node].get(token, 0)
        for pattern_index in output[node]:
            start = end_index - len(pattern_list[pattern_index])
            matches.append((pattern_index, start, end_index))
    return tuple(sorted(matches, key=lambda item: (item[1], item[2], item[0])))


def adjust_dependency_scores(scores: Mapping[str, float], edges: Sequence[tuple[str, str]], *,
                             downstream_penalty: float) -> ScoringResult:
    """Penalize a caller's score when an observed callee also has evidence.

    Edges are caller→callee. This graph-only, one-hop penalty encodes the
    dependency hypothesis that a downstream symptom can explain a caller symptom;
    it is neither a leaf-root prior nor causal proof. One-hop computation is
    bounded even when the observed graph contains cycles.
    """
    if not math.isfinite(downstream_penalty) or not 0 <= downstream_penalty <= 1:
        raise ValueError("downstream_penalty must be in [0,1]")
    values = {str(key): float(value) for key, value in scores.items()}
    if any(not math.isfinite(value) or value < 0 for value in values.values()):
        raise ValueError("scores must be finite and nonnegative")
    graph_edges = tuple(sorted(set((str(caller), str(callee)) for caller, callee in edges)))
    if not graph_edges:
        return ScoringResult("abstain", "missing_observed_dependency_edges", (), (), len(values))
    if any(caller not in values or callee not in values for caller, callee in graph_edges):
        return ScoringResult("abstain", "edge_endpoint_without_score", (), (), len(values))
    adjusted = dict(values)
    callees_by_caller: dict[str, set[str]] = {}
    for caller, callee in graph_edges:
        callees_by_caller.setdefault(caller, set()).add(callee)
    for caller, callees in callees_by_caller.items():
        strongest_callee = max(values[callee] for callee in callees)
        adjusted[caller] = max(0.0, values[caller] - downstream_penalty * strongest_callee)
    if not any(value > 0 for value in adjusted.values()):
        return ScoringResult("abstain", "all_adjusted_scores_are_zero", (), (), len(values),
                             {"method": "one-hop caller penalty from observed caller→callee edges",
                              "downstream_penalty": downstream_penalty, "edges_used": graph_edges,
                              "causal": False})
    ordered = sorted(adjusted, key=lambda item: (-adjusted[item], item))
    return ScoringResult("ok", None, tuple((key, float(adjusted[key])) for key in sorted(adjusted)),
                         tuple(ordered), len(values),
                         {"method": "one-hop caller penalty from observed caller→callee edges",
                          "downstream_penalty": downstream_penalty,
                          "edges_used": graph_edges, "causal": False})


__all__ = ["FlowFitResult", "MathResult", "ScoringResult", "VoteRecord",
           "adjust_dependency_scores", "combine_votes", "fit_flow_contributions",
           "permutation_test", "stable_seed", "token_sequence_matches",
           "tree_attribution", "tree_prediction_contributions", "welch_test"]

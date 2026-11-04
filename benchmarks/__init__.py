"""Benchmark evaluation tooling for academic datasets."""

from .bank_benchmark import load_ground_truth, run_benchmark

__all__ = ["load_ground_truth", "run_benchmark"]

"""Telemetry adapters and domain-specific starter libraries."""

from .bank_adapter import BankAdapter
from .bank_library import (
    build_bank_registry,
    get_bank_seed_playbook,
)
from .demo_adapter import DemoServiceAdapter

__all__ = [
    "BankAdapter",
    "DemoServiceAdapter",
    "build_bank_registry",
    "get_bank_seed_playbook",
]

"""
Execution modes for the 4-scenario test suite.

fast   — 1 cycle, minimal workload, ~2–5 min  (CI gate / quick sanity)
normal — 6 cycles, default workload, ~30 min   (standard FVT)
soak   — 12 cycles, scaled workload, ~90 min   (regression / soak)
"""

from __future__ import annotations
import dataclasses


class RunMode:
    FAST   = "fast"
    NORMAL = "normal"
    SOAK   = "soak"
    _VALID = {FAST, NORMAL, SOAK}

    @classmethod
    def validate(cls, value: str) -> str:
        v = value.lower()
        if v not in cls._VALID:
            raise ValueError(
                f"Invalid run mode '{value}'. Must be one of: "
                f"{', '.join(sorted(cls._VALID))}"
            )
        return v


@dataclasses.dataclass
class ModeProfile:
    """Workload parameters derived from the run mode."""
    num_cycles: int
    threads_multiplier: float
    files_multiplier: float
    burst_duration_sec: int
    cooldown_duration_sec: int
    held_open_files: int
    retry_timeout_sec: int

    @classmethod
    def for_mode(cls, mode: str) -> "ModeProfile":
        mode = RunMode.validate(mode)
        if mode == RunMode.FAST:
            return cls(
                num_cycles=1,
                threads_multiplier=1.0,
                files_multiplier=0.5,
                burst_duration_sec=20,
                cooldown_duration_sec=30,
                held_open_files=5,
                retry_timeout_sec=10,
            )
        if mode == RunMode.SOAK:
            return cls(
                num_cycles=12,
                threads_multiplier=4.0,
                files_multiplier=4.0,
                burst_duration_sec=120,
                cooldown_duration_sec=180,
                held_open_files=50,
                retry_timeout_sec=60,
            )
        # NORMAL (default)
        return cls(
            num_cycles=6,
            threads_multiplier=2.0,
            files_multiplier=2.0,
            burst_duration_sec=60,
            cooldown_duration_sec=90,
            held_open_files=20,
            retry_timeout_sec=30,
        )

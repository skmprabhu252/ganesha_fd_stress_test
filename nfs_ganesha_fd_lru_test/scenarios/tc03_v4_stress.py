"""
TC03 — NFSv4 Full Stress
========================
Exercises the complete NFSv4 FD/LRU lifecycle in a single scenario.

Consolidated from original TC02, TC05, TC08, TC10, TC11, TC12, TC13, TC14, TC16.

Additional V4-specific concerns vs TC02
----------------------------------------
- NFSv4 maintains substantially more state (open-state, lock-state,
  delegation) — state FDs are expected to be significantly higher.
- State-FD pressure events must be classified as WARNING/DIAGNOSTIC,
  not as LRU failures.
- Active handles carry V4 open-state records; the LRU must not
  reclaim state from an actively referenced handle.
- Hard-limit recovery is more complex because state FDs also count
  against the total limit.

Validation dimensions (all run in every cycle)
-----------------------------------------------
Same as TC02 PLUS:
  - State-FD pressure → WARNING/DIAGNOSTIC (not FAIL)
  - V4 state FDs reported separately from global FDs
  - FD breakdown: total ≈ global + state + temp with V4-level state counts
  - Active V4 handle protection (open-state records preserved)

Mode scaling
------------
fast   : 1 cycle,  threads×1,  files×0.5,  burst 20 s,  cooldown 30 s,  held-open 5
normal : 6 cycles, threads×2,  files×2,    burst 60 s,  cooldown 90 s,  held-open 20
soak   : 12 cycles, threads×4, files×4,    burst 120 s, cooldown 180 s, held-open 50
"""

from __future__ import annotations

import logging

from ..framework.config import ProtocolMode, TestConfig
from ..framework.runner import BaseScenario
from ..framework.verdict import CycleVerdict, Verdict
from .mode import ModeProfile, RunMode

logger = logging.getLogger(__name__)


class TC03_NFSv4_Stress(BaseScenario):

    SCENARIO_ID       = "TC03"
    DESCRIPTION       = "NFSv4 Full Stress — complete V4 FD/LRU lifecycle + state-FD validation"
    REQUIRED_PROTOCOL = ProtocolMode.V4

    _TARGET_FD_RATIO = 0.95

    def __init__(self, config: TestConfig, mode: str = RunMode.NORMAL) -> None:
        super().__init__(config)
        self._mode = RunMode.validate(mode)

    def setup_extra_config(self) -> None:
        profile = ModeProfile.for_mode(self._mode)
        wl = self.config.workload
        self.config.num_cycles   = self.config.num_cycles_override or profile.num_cycles
        wl.threads_per_client    = max(1, int(wl.threads_per_client * profile.threads_multiplier))
        wl.num_files             = max(1, int(wl.num_files           * profile.files_multiplier))
        wl.burst_duration_sec    = profile.burst_duration_sec
        wl.cooldown_duration_sec = profile.cooldown_duration_sec
        # V4 uses more held-open files to generate meaningful state FD pressure
        wl.held_open_files       = max(profile.held_open_files, 20)
        wl.retry_timeout_sec     = profile.retry_timeout_sec

        logger.info(
            "TC03 V4 Stress [%s]: cycles=%d | threads=%d | files=%d | "
            "burst=%ds | cooldown=%ds | held-open=%d",
            self._mode, self.config.num_cycles,
            wl.threads_per_client, wl.num_files,
            wl.burst_duration_sec, wl.cooldown_duration_sec,
            wl.held_open_files,
        )

    def setup_pressure_config(self, fd_limit: int, num_clients: int) -> None:
        """Scale threads/files to reach _TARGET_FD_RATIO × fd_limit."""
        if self._mode == RunMode.FAST:
            return
        wl = self.config.workload
        target_fds = int(fd_limit * self._TARGET_FD_RATIO)
        threads = wl.threads_per_client
        needed_files = max(1, target_fds // max(1, num_clients * threads))
        if needed_files > wl.num_files * 10:
            threads = max(threads, min(64, target_fds // max(1, num_clients * wl.num_files)))
            threads = max(1, threads)
            needed_files = max(1, target_fds // max(1, num_clients * threads))
        if needed_files != wl.num_files or threads != wl.threads_per_client:
            logger.info(
                "TC03 pressure scaling: fd_limit=%d target=%d clients=%d "
                "threads %d→%d  files %d→%d",
                fd_limit, target_fds, num_clients,
                wl.threads_per_client, threads,
                wl.num_files, needed_files,
            )
            wl.threads_per_client = threads
            wl.num_files = needed_files

    def post_cycle_hook(self, cycle: int, cv: CycleVerdict) -> None:
        # Log state-FD verdict explicitly — this is the key V4-specific check
        state = next((d for d in cv.dimensions if d.name == "state_fd_pressure"), None)
        if state:
            logger.info(
                "TC03 cycle %d: state_fd_pressure → %s | %s",
                cycle, state.verdict.value, state.reason,
            )
            if state.verdict == Verdict.FAIL:
                logger.error(
                    "TC03 cycle %d: state-FD pressure incorrectly classified as FAIL — "
                    "this is a framework logic error.", cycle
                )

        # Alert on active-handle failure
        ah = next((d for d in cv.dimensions if "active_handle" in d.name), None)
        if ah and ah.verdict == Verdict.FAIL:
            logger.error(
                "TC03 CRITICAL cycle %d: V4 held-open handle(s) invalidated by LRU: %s",
                cycle, ah.reason,
            )

        # High-watermark: scale up for next cycle if not reached
        if self._mode != RunMode.FAST:
            hiwat = next((d for d in cv.dimensions if d.name == "high_watermark_handling"), None)
            if hiwat and hiwat.verdict == Verdict.INCONCLUSIVE:
                wl = self.config.workload
                new_files = max(wl.num_files + 1, int(wl.num_files * 1.25))
                logger.warning(
                    "TC03 cycle %d: V4 high-watermark NOT reached — "
                    "scaling files %d→%d for next cycle.",
                    cycle, wl.num_files, new_files,
                )
                wl.num_files = new_files

"""
TC04 — NFSv3 + NFSv4 Mixed Stress
===================================
Exercises the complete FD/LRU lifecycle under concurrent V3 and V4 workloads.

Consolidated from original TC03, TC06, TC09, TC10, TC11, TC12, TC13, TC15, TC18.

Why mixed is the most important test
--------------------------------------
- Both V3 and V4 contribute FD pressure simultaneously.
- The LRU must correctly reclaim reclaimable global FDs while
  preserving state FDs that belong to active V4 connections.
- Futility is more likely because FD open rate comes from two
  protocol paths at the same time.
- Active-handle protection must work for BOTH V3 and V4 handles
  concurrently — neither type may be reclaimed.
- State-FD pressure from V4 must not be misclassified as a global
  LRU failure when V3 global FDs are also high.
- The test must not PASS if one protocol silently fails while the
  other continues.

Validation dimensions (all run in every cycle)
-----------------------------------------------
Same as TC02 + TC03 combined PLUS:
  - Both V3 and V4 workload completion validated independently
  - Cross-protocol interference check (neither protocol starves)
  - Mixed state/global FD pressure correct dual-category classification
  - Both V3 and V4 held-open handles validated mid-burst and post-burst

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


class TC04_Mixed_Stress(BaseScenario):

    SCENARIO_ID       = "TC04"
    DESCRIPTION       = "Mixed Stress — V3+V4 concurrent FD/LRU lifecycle, active handles, state-FD"
    REQUIRED_PROTOCOL = ProtocolMode.BOTH

    # For BOTH protocol two workers per client run simultaneously, so each worker
    # needs half the target FDs to reach the same total pressure.
    _TARGET_FD_RATIO = 0.95
    _WORKERS_PER_CLIENT = 2   # v3 + v4

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
        # Mixed test always uses the higher held-open count to exercise
        # both V3 and V4 active handle protection simultaneously
        wl.held_open_files       = max(profile.held_open_files, 20)
        wl.retry_timeout_sec     = profile.retry_timeout_sec

        logger.info(
            "TC04 Mixed Stress [%s]: cycles=%d | threads=%d | files=%d | "
            "burst=%ds | cooldown=%ds | held-open=%d",
            self._mode, self.config.num_cycles,
            wl.threads_per_client, wl.num_files,
            wl.burst_duration_sec, wl.cooldown_duration_sec,
            wl.held_open_files,
        )

    def setup_pressure_config(self, fd_limit: int, num_clients: int) -> None:
        """
        Scale threads/files to reach _TARGET_FD_RATIO × fd_limit.
        For BOTH protocol, total concurrent workers = num_clients × 2 (v3+v4).
        """
        if self._mode == RunMode.FAST:
            return
        wl = self.config.workload
        target_fds = int(fd_limit * self._TARGET_FD_RATIO)
        total_workers = num_clients * self._WORKERS_PER_CLIENT
        threads = wl.threads_per_client
        needed_files = max(1, target_fds // max(1, total_workers * threads))
        if needed_files > wl.num_files * 10:
            threads = max(threads, min(64, target_fds // max(1, total_workers * wl.num_files)))
            threads = max(1, threads)
            needed_files = max(1, target_fds // max(1, total_workers * threads))
        if needed_files != wl.num_files or threads != wl.threads_per_client:
            logger.info(
                "TC04 pressure scaling: fd_limit=%d target=%d clients=%d workers=%d "
                "threads %d→%d  files %d→%d",
                fd_limit, target_fds, num_clients, total_workers,
                wl.threads_per_client, threads,
                wl.num_files, needed_files,
            )
            wl.threads_per_client = threads
            wl.num_files = needed_files

    def post_cycle_hook(self, cycle: int, cv: CycleVerdict) -> None:
        # Verify the BOTH workload dimension is present
        names = {d.name for d in cv.dimensions}
        if f"{ProtocolMode.BOTH}_workload_completion" not in names:
            logger.warning(
                "TC04 cycle %d: BOTH protocol workload dimension missing — "
                "one protocol may not have generated workload.", cycle
            )

        # State-FD pressure must be WARNING not FAIL
        state = next((d for d in cv.dimensions if d.name == "state_fd_pressure"), None)
        if state and state.verdict == Verdict.FAIL:
            logger.error(
                "TC04 cycle %d: state-FD pressure incorrectly classified as FAIL "
                "(should be WARNING/DIAGNOSTIC).", cycle
            )

        # Active handle failures are the most critical failure
        ah = next((d for d in cv.dimensions if "active_handle" in d.name), None)
        if ah and ah.verdict == Verdict.FAIL:
            logger.error(
                "TC04 CRITICAL cycle %d: Held-open handle(s) invalidated under "
                "mixed V3+V4 LRU pressure: %s", cycle, ah.reason,
            )

        # Futility advisory
        futility = next((d for d in cv.dimensions if d.name == "futility_detection"), None)
        if futility and futility.verdict == Verdict.FAIL:
            logger.error(
                "TC04 cycle %d: Futility + no LRU recovery under mixed load — "
                "possible LRU regression.", cycle
            )

        # High-watermark: scale up for next cycle if not reached
        if self._mode != RunMode.FAST:
            hiwat = next((d for d in cv.dimensions if d.name == "high_watermark_handling"), None)
            if hiwat and hiwat.verdict == Verdict.INCONCLUSIVE:
                wl = self.config.workload
                new_files = max(wl.num_files + 1, int(wl.num_files * 1.25))
                logger.warning(
                    "TC04 cycle %d: high-watermark NOT reached — "
                    "scaling files %d→%d for next cycle.",
                    cycle, wl.num_files, new_files,
                )
                wl.num_files = new_files

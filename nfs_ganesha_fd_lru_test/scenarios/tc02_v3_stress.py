"""
TC02 — NFSv3 Full Stress
========================
Exercises the complete NFSv3 FD/LRU lifecycle in a single scenario.

Consolidated from original TC01, TC04, TC07, TC10, TC11, TC12, TC16.

Validation dimensions (all run in every cycle)
-----------------------------------------------
Workload
  - V3 workload completion (< 1 % persistent failures)
  - Client-side EMFILE distinguished from server-side EIO
  - No mount loss (ESTALE rate)

FD pressure
  - High-watermark detection and LRU wakeup (WARNING, not FAIL)
  - Hard-limit detection and client retry recovery

LRU reclamation
  - Global FD reclaimed ≥ 50 % of peak → PASS
  - 20–50 % → WARNING
  - < 20 %  → FAIL

Futility
  - Futility events detected and correlated
  - Futility + no reclamation + no settle → FAIL

Cooldown / retention
  - FD settles within cooldown window
  - Settled FD does not accumulate monotonically across cycles

Active handle protection
  - Held-open V3 files validated mid-burst and post-burst
  - Any ESTALE / EBADF → immediate FAIL

Server health
  - No Ganesha restart
  - ganesha_stats always reachable
  - FD accounting: total ≈ global + state + temp

Mode scaling
------------
fast   : 1 cycle,  threads×1,  files×0.5,  burst 20 s,  cooldown 30 s
normal : 6 cycles, threads×2,  files×2,    burst 60 s,  cooldown 90 s
soak   : 12 cycles, threads×4, files×4,    burst 120 s, cooldown 180 s
"""

from __future__ import annotations

import logging

from ..framework.config import ProtocolMode, TestConfig
from ..framework.runner import BaseScenario
from ..framework.verdict import CycleVerdict, Verdict
from .mode import ModeProfile, RunMode

logger = logging.getLogger(__name__)


class TC02_NFSv3_Stress(BaseScenario):

    SCENARIO_ID       = "TC02"
    DESCRIPTION       = "NFSv3 Full Stress — complete V3 FD/LRU lifecycle validation"
    REQUIRED_PROTOCOL = ProtocolMode.V3

    def __init__(self, config: TestConfig, mode: str = RunMode.NORMAL) -> None:
        super().__init__(config)
        self._mode = RunMode.validate(mode)

    # Target: reach 95 % of the FD limit to reliably trigger the high-watermark.
    # Ganesha's default high-watermark threshold is 90 % of the system limit.
    _TARGET_FD_RATIO = 0.95

    def setup_extra_config(self) -> None:
        profile = ModeProfile.for_mode(self._mode)
        wl = self.config.workload
        self.config.num_cycles   = self.config.num_cycles_override or profile.num_cycles
        wl.threads_per_client    = max(1, int(wl.threads_per_client * profile.threads_multiplier))
        wl.num_files             = max(1, int(wl.num_files           * profile.files_multiplier))
        wl.burst_duration_sec    = profile.burst_duration_sec
        wl.cooldown_duration_sec = profile.cooldown_duration_sec
        wl.held_open_files       = profile.held_open_files
        wl.retry_timeout_sec     = profile.retry_timeout_sec

        logger.info(
            "TC02 V3 Stress [%s]: cycles=%d | threads=%d | files=%d | "
            "burst=%ds | cooldown=%ds | held-open=%d",
            self._mode, self.config.num_cycles,
            wl.threads_per_client, wl.num_files,
            wl.burst_duration_sec, wl.cooldown_duration_sec,
            wl.held_open_files,
        )

    def setup_pressure_config(self, fd_limit: int, num_clients: int) -> None:
        """
        Scale threads and files so the concurrent open count reaches
        _TARGET_FD_RATIO × fd_limit, distributing evenly across clients.

        Peak concurrent FDs ≈ num_clients × threads × files_per_thread.
        Solving for files_per_thread (keeping threads fixed):
            files = target_fds / (num_clients × threads)
        If that still requires more than 10× the current file count we also
        raise threads to keep files per thread reasonable.
        """
        if self._mode == RunMode.FAST:
            return   # fast mode is a smoke test — no pressure scaling

        wl = self.config.workload
        target_fds = int(fd_limit * self._TARGET_FD_RATIO)
        threads = wl.threads_per_client
        needed_files = max(1, target_fds // max(1, num_clients * threads))

        # If needed_files is > 10× current, also scale up threads to share the load
        if needed_files > wl.num_files * 10:
            threads = max(threads, min(64, target_fds // max(1, num_clients * wl.num_files)))
            threads = max(1, threads)
            needed_files = max(1, target_fds // max(1, num_clients * threads))

        if needed_files != wl.num_files or threads != wl.threads_per_client:
            logger.info(
                "TC02 pressure scaling: fd_limit=%d target=%d clients=%d "
                "threads %d→%d  files %d→%d",
                fd_limit, target_fds, num_clients,
                wl.threads_per_client, threads,
                wl.num_files, needed_files,
            )
            wl.threads_per_client = threads
            wl.num_files = needed_files

    def post_cycle_hook(self, cycle: int, cv: CycleVerdict) -> None:
        # If high-watermark was not reached, scale up by 25 % for the next cycle
        if self._mode != RunMode.FAST:
            hiwat = next((d for d in cv.dimensions if d.name == "high_watermark_handling"), None)
            if hiwat and hiwat.verdict == Verdict.INCONCLUSIVE:
                wl = self.config.workload
                new_files = max(wl.num_files + 1, int(wl.num_files * 1.25))
                logger.warning(
                    "TC02 cycle %d: V3 high-watermark NOT reached — "
                    "scaling files %d→%d for next cycle.",
                    cycle, wl.num_files, new_files,
                )
                wl.num_files = new_files

        # Alert immediately on active-handle failure
        ah = next((d for d in cv.dimensions if "active_handle" in d.name), None)
        if ah and ah.verdict == Verdict.FAIL:
            logger.error(
                "TC02 CRITICAL cycle %d: V3 held-open handle(s) invalidated by LRU: %s",
                cycle, ah.reason,
            )

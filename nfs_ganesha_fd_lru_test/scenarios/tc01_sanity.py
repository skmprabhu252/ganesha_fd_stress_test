"""
TC01 — Sanity Test
==================
Quick smoke check (~1 cycle, minimal workload).

Purpose
-------
Verify that the environment is correctly configured and that the
Ganesha FD/LRU mechanism functions at a basic level before running
any sustained stress.

What is validated
-----------------
- SSH connectivity to server and all clients
- ganesha_stats inode returns valid output
- Ganesha log is accessible
- NFS mount works for the required protocol
- A small workload completes without errors
- FD usage rises and settles back
- No Ganesha restart
- FD accounting is consistent
- Held-open handles remain valid

This test is intentionally short.  It is not a stress test.
If it fails, there is an environment or configuration problem
that must be fixed before running TC02–TC04.
"""

from __future__ import annotations

import logging

from ..framework.config import ProtocolMode, TestConfig
from ..framework.runner import BaseScenario
from ..framework.verdict import CycleVerdict, Verdict
from .mode import ModeProfile, RunMode

logger = logging.getLogger(__name__)


class TC01_Sanity(BaseScenario):

    SCENARIO_ID       = "TC01"
    DESCRIPTION       = "Sanity — environment validation + minimal FD lifecycle smoke test"
    REQUIRED_PROTOCOL = ProtocolMode.BOTH   # verify both V3 and V4 mounts exist

    def __init__(self, config: TestConfig, mode: str = RunMode.FAST) -> None:
        super().__init__(config)
        self._mode = RunMode.validate(mode)

    def setup_extra_config(self) -> None:
        # Sanity always uses the fast profile regardless of mode argument
        profile = ModeProfile.for_mode(RunMode.FAST)
        wl = self.config.workload
        self.config.num_cycles             = 1          # exactly one cycle
        wl.threads_per_client              = max(1, int(wl.threads_per_client * profile.threads_multiplier))
        wl.num_files                       = max(1, int(wl.num_files * profile.files_multiplier))
        wl.burst_duration_sec              = profile.burst_duration_sec
        wl.cooldown_duration_sec           = profile.cooldown_duration_sec
        wl.held_open_files                 = profile.held_open_files
        wl.retry_timeout_sec               = profile.retry_timeout_sec

        logger.info(
            "TC01 Sanity: 1 cycle | threads=%d | files=%d | burst=%ds | cooldown=%ds",
            wl.threads_per_client, wl.num_files,
            wl.burst_duration_sec, wl.cooldown_duration_sec,
        )

    def post_cycle_hook(self, cycle: int, cv: CycleVerdict) -> None:
        if cv.overall == Verdict.FAIL:
            logger.error(
                "TC01 Sanity FAILED — do not proceed to TC02-TC04 until this is fixed."
            )
        elif cv.overall in (Verdict.PASS, Verdict.WARNING):
            logger.info("TC01 Sanity %s — environment is ready for stress testing.", cv.overall.value)

"""
Server-side FD/LRU monitor.

Polls ``ganesha_stats inode`` on the Ganesha server at configurable
intervals and collects new log entries from the Ganesha log.

The monitor runs in a background thread during both burst and cooldown
phases and stores:

  - Ordered list of FDSample snapshots
  - Ordered list of LogEvent objects

Consumers read the samples and events after the phase ends.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .config import ServerConfig
from .fd_stats import FDSample, parse_ganesha_stats
from .log_parser import LogEvent, LogEventKind, parse_log_text
from .ssh_client import SSHClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

@dataclass
class MonitorPhase:
    """All samples and events collected during a single monitoring phase."""
    label: str
    samples: List[FDSample] = field(default_factory=list)
    events: List[LogEvent]  = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: float   = 0.0

    # ---- derived metrics --------------------------------------------------

    @property
    def peak_fsal_fd(self) -> int:
        if not self.samples:
            return 0
        return max(s.fsal_opened_fd for s in self.samples)

    @property
    def settled_fsal_fd(self) -> int:
        """Last sample's FSAL FD count (post-cooldown representative)."""
        if not self.samples:
            return 0
        return self.samples[-1].fsal_opened_fd

    @property
    def peak_global_fd(self) -> int:
        if not self.samples:
            return 0
        return max(s.global_fd for s in self.samples)

    @property
    def settled_global_fd(self) -> int:
        if not self.samples:
            return 0
        return self.samples[-1].global_fd

    @property
    def peak_lru_entries(self) -> int:
        if not self.samples:
            return 0
        return max(s.lru_entries_in_use for s in self.samples)

    @property
    def settled_lru_entries(self) -> int:
        if not self.samples:
            return 0
        return self.samples[-1].lru_entries_in_use

    @property
    def high_watermark_reached(self) -> bool:
        return any(e.kind == LogEventKind.HIGH_WATERMARK for e in self.events)

    @property
    def hard_limit_reached(self) -> bool:
        return any(e.kind == LogEventKind.HARD_LIMIT for e in self.events)

    @property
    def futility_detected(self) -> bool:
        return any(e.kind == LogEventKind.FUTILITY for e in self.events)

    @property
    def state_fd_pressure_detected(self) -> bool:
        return any(e.kind == LogEventKind.STATE_FD_PRESSURE for e in self.events)

    @property
    def ganesha_restarted(self) -> bool:
        return any(e.kind == LogEventKind.GANESHA_RESTART for e in self.events)

    def lru_made_progress(self, protocol: str = "V3") -> bool:
        """
        True if the LRU shows meaningful downward movement after cooldown.

        NFSv3 (stateless):
          Every open FD is an LRU entry.  Signal: lru_entries_in_use.

        NFSv4 (stateful):
          State FDs are NEVER in the LRU — they are closed by clients via
          the CLOSE operation.  Only global (reclaimable) FDs go through
          the LRU.  Signal: global_fd only.
          If global_fd breakdown is unavailable, fall back to
          fsal_opened_fd minus state_fd.
        """
        if not self.samples:
            return False

        is_v4 = protocol in ("V4", "BOTH")

        if is_v4:
            peak    = self.peak_global_fd
            settled = self.settled_global_fd
            if peak == 0:
                # No breakdown — approximate reclaimable as fsal minus state
                peak_state = max((s.state_fd for s in self.samples), default=0)
                peak    = max(0, self.peak_fsal_fd - peak_state)
                settled = max(0, self.settled_fsal_fd - self.samples[-1].state_fd)
        else:
            # V3: lru_entries_in_use is the correct signal
            peak    = self.peak_lru_entries
            settled = self.settled_lru_entries
            if peak == 0:
                peak    = self.peak_fsal_fd
                settled = self.settled_fsal_fd

        if peak == 0:
            return True   # no data — inconclusive but not a failure

        return settled < peak * 0.90  # > 10 % reduction counts as progress

    def fd_settled(self) -> bool:
        """
        True if the last three FD samples are within 10 % of each other.

        Uses fsal_opened_fd as the primary signal; falls back to
        lru_entries_in_use when fsal_opened_fd is 0 (shouldn't happen
        after parser fix, but kept for safety).
        """
        recent = [s.fsal_opened_fd or s.lru_entries_in_use for s in self.samples[-3:]]
        if len(recent) < 2:
            return True
        spread = max(recent) - min(recent)
        avg = sum(recent) / len(recent)
        if avg == 0:
            return True
        return (spread / avg) <= 0.10


class ServerMonitor:
    """
    Background monitor that polls ganesha_stats and collects log entries.

    Usage::

        monitor = ServerMonitor(server_cfg, ssh_client)
        phase = monitor.start_phase("burst")
        ... run workload ...
        monitor.stop_phase(phase)
    """

    def __init__(
        self,
        server: ServerConfig,
        ssh: SSHClient,
        poll_interval_sec: float = 5.0,
        log_tail_lines: int = 200,
    ) -> None:
        self.server = server
        self.ssh = ssh
        self.poll_interval_sec = poll_interval_sec
        self.log_tail_lines = log_tail_lines

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._current_phase: Optional[MonitorPhase] = None
        self._lock = threading.Lock()
        self._last_log_offset: int = 0
        self._stats_unavailable_since: Optional[float] = None
        self.stats_unavailable = False

    # ------------------------------------------------------------------
    # Internal polling
    # ------------------------------------------------------------------

    def _poll_stats(self) -> Optional[FDSample]:
        result = self.ssh.run_remote(
            self.server.ssh_host,
            self.server.ganesha_stats_cmd,
            timeout=15,
        )
        if not result.ok:
            if self._stats_unavailable_since is None:
                self._stats_unavailable_since = time.monotonic()
                logger.warning("ganesha_stats unavailable: %s", result.stderr)
            return None

        self._stats_unavailable_since = None
        return parse_ganesha_stats(result.stdout)

    def _poll_log(self) -> List[LogEvent]:
        cmd = (
            f"tail -n {self.log_tail_lines} {self.server.ganesha_log_path} 2>/dev/null"
        )
        result = self.ssh.run_remote(self.server.ssh_host, cmd, timeout=15)
        if not result.ok:
            return []
        return parse_log_text(result.stdout)

    def _monitor_loop(self, phase: MonitorPhase) -> None:
        while not self._stop_event.is_set():
            # --- FD stats ---
            sample = self._poll_stats()
            if sample is not None:
                with self._lock:
                    phase.samples.append(sample)
            else:
                elapsed_unavailable = (
                    time.monotonic() - self._stats_unavailable_since
                    if self._stats_unavailable_since is not None
                    else 0
                )
                if elapsed_unavailable > 30:
                    self.stats_unavailable = True
                    logger.error("SERVER MONITORING FAILURE: ganesha_stats unavailable >30s")

            # --- Log events ---
            events = self._poll_log()
            if events:
                with self._lock:
                    # Deduplicate by raw_line to avoid re-adding old entries
                    existing = {e.raw_line for e in phase.events}
                    for ev in events:
                        if ev.raw_line not in existing:
                            phase.events.append(ev)
                            existing.add(ev.raw_line)

            self._stop_event.wait(timeout=self.poll_interval_sec)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_phase(self, label: str) -> MonitorPhase:
        """Start a new monitoring phase and return it."""
        self._stop_event.clear()
        phase = MonitorPhase(label=label)
        self._current_phase = phase

        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(phase,),
            daemon=True,
            name=f"monitor-{label}",
        )
        self._thread.start()
        logger.info("Monitor started: phase=%s", label)
        return phase

    def stop_phase(self, phase: MonitorPhase) -> MonitorPhase:
        """Stop the current monitoring phase."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
        phase.end_time = time.time()
        logger.info(
            "Monitor stopped: phase=%s samples=%d events=%d",
            phase.label, len(phase.samples), len(phase.events),
        )
        return phase

    def collect_baseline(self, num_samples: int = 5, interval_sec: float = 3.0) -> MonitorPhase:
        """
        Collect *num_samples* FD snapshots before the workload starts.

        Returns a MonitorPhase with the baseline data.
        """
        phase = MonitorPhase(label="baseline")
        logger.info(
            "Collecting baseline (%d samples) from %s...", num_samples, self.server.ssh_host
        )
        for i in range(num_samples):
            sample = self._poll_stats()
            if sample is not None:
                phase.samples.append(sample)
                logger.debug("Baseline sample %d: %s", i + 1, sample)
            else:
                logger.warning("Baseline sample %d: ganesha_stats unavailable", i + 1)
            if i < num_samples - 1:
                time.sleep(interval_sec)
        phase.end_time = time.time()
        return phase

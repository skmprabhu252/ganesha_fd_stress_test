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
import re
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .config import ServerConfig
from .fd_stats import FDSample, parse_ganesha_stats
from .log_parser import LogEvent, LogEventKind, parse_log_text
from .ssh_client import SSHClient

logger = logging.getLogger(__name__)


def _fmt_epoch(epoch: float) -> str:
    """Format a Unix epoch as HH:MM:SS in local time — for log messages only."""
    import datetime
    return datetime.datetime.fromtimestamp(epoch).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Restart event deduplication key
# ---------------------------------------------------------------------------

_EPOCH_RE = re.compile(r"epoch\s+([0-9a-fA-F]+)", re.I)
_NODE_RE  = re.compile(r":\s+(\S+)\s+:", re.I)


def _restart_dedup_key(ev: LogEvent) -> tuple:
    """
    Return a hashable key that identifies a unique Ganesha restart instance.

    A single restart emits multiple matching log lines (one per thread /
    init step).  Collapsing by (minute_bucket, epoch, node) reduces them
    to a single representative event so the verdict engine sees an accurate
    restart count.
    """
    epoch_m = _EPOCH_RE.search(ev.raw_line)
    node_m  = _NODE_RE.search(ev.raw_line)
    epoch   = epoch_m.group(1) if epoch_m else ""
    node    = node_m.group(1)  if node_m  else ""
    # Bucket to the nearest minute to tolerate slight timestamp spread
    # across the lines emitted during a single startup sequence.
    minute_bucket = int(ev.timestamp) // 60
    return (minute_bucket, epoch, node)


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
        # Primary: log-file events emitted by Ganesha when the LRU thread wakes.
        if any(e.kind == LogEventKind.HIGH_WATERMARK for e in self.events):
            return True
        # Fallback: ganesha_stats reports "FD usage : Above High Water Mark"
        # (or "Hard Limit reached") as a text label in its inode output.
        # The log may not contain the matching log-level message (e.g. it was
        # rotated, below the tail window, or the build emits a different string),
        # but the stats label is always present while the condition holds.
        _hwm_labels = {"above high water mark", "hard limit reached"}
        return any(
            s.fd_usage_label.lower() in _hwm_labels
            for s in self.samples
        )

    @property
    def hard_limit_reached(self) -> bool:
        # Primary: log-file HARD_LIMIT events.
        if any(e.kind == LogEventKind.HARD_LIMIT for e in self.events):
            return True
        # Fallback: ganesha_stats "FD usage : Hard Limit reached" label.
        return any(
            "hard limit" in s.fd_usage_label.lower()
            for s in self.samples
        )

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
            # V3: fsal_opened_fd (Total FD) is the correct reclamation signal.
            # lru_entries_in_use counts inode-cache slots which do NOT drop
            # when the LRU reclaims FDs (the inode stays cached for reuse).
            peak    = self.peak_fsal_fd
            settled = self.settled_fsal_fd
            if peak == 0:
                # No FD data — last resort fallback to inode cache count
                peak    = self.peak_lru_entries
                settled = self.settled_lru_entries

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
        monitor.calibrate_server_time()   # anchors log filter to server clock
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

        # _test_start_time is the server-side Unix epoch at the moment the
        # test begins.  Any log event whose parsed timestamp predates this
        # value is pre-test history and must be discarded.
        #
        # Initialised to None.  calibrate_server_time() must be called before
        # the first monitoring phase starts; if it is not called (e.g. in unit
        # tests that don't have SSH) the filter falls back to accepting all events.
        self._test_start_time: Optional[float] = None

    # ------------------------------------------------------------------
    # Server-clock calibration
    # ------------------------------------------------------------------

    def calibrate_server_time(self) -> None:
        """
        Fetch the current Unix epoch from the Ganesha server via SSH and
        store it as ``_test_start_time``.

        This anchors the log-event filter to the **server's own clock**,
        eliminating any controller-vs-server clock skew.  Log lines whose
        parsed timestamp predates this value are considered pre-test history
        (previous runs, old LRU messages, etc.) and are silently discarded.

        Must be called once before the first :meth:`start_phase` call.
        If the SSH command fails, falls back to ``time.time()`` on the
        controller so the test can still proceed — a warning is logged.
        """
        result = self.ssh.run_remote(
            self.server.ssh_host,
            "date +%s",
            timeout=10,
        )
        if result.ok:
            try:
                server_epoch = float(result.stdout.strip())
                self._test_start_time = server_epoch
                logger.info(
                    "Server time calibrated: server_epoch=%.0f (%s)  "
                    "— log events before this timestamp will be discarded",
                    server_epoch,
                    _fmt_epoch(server_epoch),
                )
                return
            except ValueError:
                logger.warning(
                    "Could not parse server epoch from %r — falling back to controller time",
                    result.stdout.strip(),
                )
        else:
            logger.warning(
                "SSH 'date +%%s' failed on %s (%s) — falling back to controller time",
                self.server.ssh_host, result.stderr,
            )
        # Fallback: use controller's local time
        self._test_start_time = time.time()
        logger.info(
            "Server time fallback: using controller epoch=%.0f (%s)",
            self._test_start_time,
            _fmt_epoch(self._test_start_time),
        )

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
                    # Drop events that predate this test run.
                    # The log is read with `tail -n N` (no cursor), so every
                    # poll re-reads the same historical tail on every iteration.
                    # _test_start_time is the server-side epoch captured by
                    # calibrate_server_time() just before the test began — so
                    # the comparison is entirely in the server's own time domain
                    # and is immune to controller-vs-server clock skew.
                    # When _test_start_time is None (unit tests / no SSH),
                    # accept all events.
                    if self._test_start_time is not None:
                        dropped = [e for e in events
                                   if e.timestamp < self._test_start_time]
                        if dropped:
                            logger.debug(
                                "Dropped %d pre-test log event(s) "
                                "(oldest server time=%s, test started=%s)",
                                len(dropped),
                                _fmt_epoch(min(e.timestamp for e in dropped)),
                                _fmt_epoch(self._test_start_time),
                            )
                        events = [e for e in events
                                  if e.timestamp >= self._test_start_time]

                    # De-duplicate events so the verdict engine sees clean counts.
                    #
                    # Non-restart events: deduplicate by raw_line (a tail window
                    # will return the same lines on every poll).
                    #
                    # GANESHA_RESTART events: additionally collapse by
                    # (minute_bucket, epoch_token, node_name) — a single restart
                    # emits one matching line per thread / init step.
                    existing_lines = {e.raw_line for e in phase.events}
                    seen_restart_keys = {
                        _restart_dedup_key(e)
                        for e in phase.events
                        if e.kind == LogEventKind.GANESHA_RESTART
                    }
                    for ev in events:
                        if ev.raw_line in existing_lines:
                            continue
                        if ev.kind == LogEventKind.GANESHA_RESTART:
                            key = _restart_dedup_key(ev)
                            if key in seen_restart_keys:
                                continue
                            seen_restart_keys.add(key)
                        phase.events.append(ev)
                        existing_lines.add(ev.raw_line)

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

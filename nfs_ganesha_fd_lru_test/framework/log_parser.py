"""
Ganesha log parser for FD/LRU-related structured events.

Recognises the message categories introduced by Patch 1247084:
  - FD_COUNT_DIAG       — periodic FD breakdown log
  - HIGH_WATERMARK      — Ganesha detected FD pressure, woke LRU thread
  - HARD_LIMIT          — configured hard FD limit exceeded
  - FUTILITY            — LRU cannot keep up with FD open rate
  - STATE_FD_PRESSURE   — state FDs above relevant threshold

Events are timestamped and stored as LogEvent objects that can be
correlated with FDSample snapshots.

Restart detection
-----------------
A GANESHA_RESTART event is raised only when the log contains a line that
unambiguously signals a fresh daemon start, i.e. the startup banner that
Ganesha always prints when it first initialises.  Normal per-thread log
lines that happen to contain the daemon binary name (gpfs.ganesha.nfsd)
are NOT considered restarts — every log line from a running daemon
includes the process name, so matching on the name alone produces massive
false-positive counts.

The specific patterns matched are:
  • "NFS-Ganesha Release" / "Starting NFS-Ganesha"  — Ganesha's own banner
  • "Initializing memory and logging"                — very first init step
  • "ganesha_init_complete" / "Init complete"        — end of init sequence
  • "Loading parameters from"                        — config load at startup

Lines that contain the process name in a routine per-RPC or per-thread
context (e.g. "gpfs.ganesha.nfsd-NNNN[svc_0]") do NOT match.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Event categories
# ---------------------------------------------------------------------------

class LogEventKind(Enum):
    FD_COUNT_DIAG     = auto()
    HIGH_WATERMARK    = auto()
    HARD_LIMIT        = auto()
    FUTILITY          = auto()
    STATE_FD_PRESSURE = auto()
    GANESHA_RESTART   = auto()
    GENERIC_WARNING   = auto()


# ---------------------------------------------------------------------------
# Log event
# ---------------------------------------------------------------------------

@dataclass
class LogEvent:
    kind: LogEventKind
    timestamp: float            # unix epoch derived from log line or parse time
    raw_line: str
    total_fd: int     = 0
    global_fd: int    = 0
    state_fd: int     = 0
    temp_fd: int      = 0
    hiwat: int        = 0
    lowat: int        = 0
    futility_count: int = 0
    message: str      = ""

    def __str__(self) -> str:  # pragma: no cover
        ts = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        return f"[{ts}] {self.kind.name} total={self.total_fd} global={self.global_fd} state={self.state_fd} temp={self.temp_fd}"


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# Ganesha log timestamp formats:
#   "2024/05/01 15:32:41"  — slash-separated (older builds)
#   "2026-08-31 19:22:42"  — ISO dash-separated (GPFS/RHEL9 builds, as seen in production)
#   "15:32:41"             — time-only (short form, date inferred from today)
_TS_FULL  = re.compile(r"(\d{4}[/-]\d{2}[/-]\d{2} \d{2}:\d{2}:\d{2})")
_TS_SHORT = re.compile(r"(\d{2}:\d{2}:\d{2})")

# FD count diagnostic — emitted periodically by the FD manager
# Expected: "FD count: total=500000 global=480000 state=15000 temp=5000 hiwat=... lowat=..."
_DIAG_PATTERN = re.compile(
    r"FD\s+count.*?total[=:\s]+(\d+).*?global[=:\s]+(\d+).*?state[=:\s]+(\d+).*?temp[=:\s]+(\d+)",
    re.I | re.S,
)

# High-watermark
_HIWAT_PATTERN = re.compile(
    r"(high.?water.?mark|hiwat\s+reached|waking\s+lru\s+thread|fd\s+pressure\s+detected)",
    re.I,
)

# Hard limit
_HARD_LIMIT_PATTERN = re.compile(r"(hard.?limit|fd\s+hard\s+limit\s+exceeded)", re.I)

# Futility
_FUTILITY_PATTERN = re.compile(
    r"(futility|lru\s+futility|futility\s+count\s+exceeded)", re.I
)

# State FD pressure
_STATE_FD_PATTERN = re.compile(
    r"(state\s+fd.*?hiwat|state\s+fd\s+pressure|state\s+fds?\s+exceed)", re.I
)

# Ganesha restart/crash signal.
#
# The canonical restart marker in the GPFS/RHEL9 structured log format is:
#
#   ... gpfs.ganesha.nfsd-NNNN[main] nfs_start :NFS STARTUP :EVENT : NFS SERVER INITIALIZED
#
# This line is emitted by the nfs_start() function exactly once per daemon
# init, when the NFS server has completed all initialisation steps.
# It is the most reliable restart indicator because:
#   • function name "nfs_start" only runs at startup
#   • component "NFS STARTUP" is only active during init
#   • message "NFS SERVER INITIALIZED" is the final init-complete log entry
#
# Additional patterns cover other Ganesha build variants / log styles:
#   • "NFS-Ganesha Release …"        — upstream release banner
#   • "Starting/Started NFS-Ganesha" — systemd service messages
#   • "Initializing memory and logging" — very first init step (some builds)
#   • "Loading parameters from …"    — config load at startup
#   • "ganesha_init_complete"        — explicit completion marker (some builds)
#
# Deliberately excluded: any line that merely contains the process name
# "gpfs.ganesha.nfsd" in a per-thread / per-RPC context — those appear in
# every log line from a running daemon and would produce false positives.
_RESTART_PATTERN = re.compile(
    r"("
    r"NFS\s+SERVER\s+INITIALIZED"
    r"|NFS\s+STARTUP"
    r"|\bnfs_start\b"
    r"|NFS-Ganesha\s+Release"
    r"|Starting\s+NFS-Ganesha"
    r"|Started\s+NFS-Ganesha"
    r"|Initializing\s+memory\s+and\s+logging"
    r"|Loading\s+parameters\s+from"
    r"|ganesha_init_complete"
    r")",
    re.I,
)

# Number extraction helper
_FD_NUMS = re.compile(
    r"total[=:\s]+(\d+).*?global[=:\s]+(\d+).*?state[=:\s]+(\d+).*?temp[=:\s]+(\d+)",
    re.I | re.S,
)
_SINGLE_NUM = re.compile(r"\b(\d+)\b")
_HIWAT_NUM  = re.compile(r"hiwat[=:\s]+(\d+)", re.I)
_LOWAT_NUM  = re.compile(r"lowat[=:\s]+(\d+)", re.I)
_FUTILITY_CNT = re.compile(r"futility[=:\s]+(\d+)", re.I)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _extract_timestamp(line: str, now: float) -> float:
    """Extract unix epoch from a log line, falling back to *now*."""
    import datetime
    m = _TS_FULL.search(line)
    if m:
        raw = m.group(1)
        # Normalise separator so both "2026-08-31" and "2026/08/31" parse correctly
        normalised = raw.replace("/", "-")
        try:
            dt = datetime.datetime.strptime(normalised, "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()
        except ValueError:
            pass
    m = _TS_SHORT.search(line)
    if m:
        try:
            today = datetime.date.today()
            dt = datetime.datetime.strptime(
                f"{today} {m.group(1)}", "%Y-%m-%d %H:%M:%S"
            )
            return dt.timestamp()
        except ValueError:
            pass
    return now


def _extract_fd_breakdown(line: str) -> Tuple[int, int, int, int]:
    m = _FD_NUMS.search(line)
    if m:
        return int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    return 0, 0, 0, 0


def parse_log_line(line: str, now: Optional[float] = None) -> Optional[LogEvent]:
    """
    Parse a single Ganesha log line and return a :class:`LogEvent` if it
    matches a known FD/LRU pattern, or None otherwise.
    """
    if now is None:
        now = time.time()
    ts = _extract_timestamp(line, now)
    total, global_fd, state_fd, temp_fd = _extract_fd_breakdown(line)

    if _RESTART_PATTERN.search(line):
        return LogEvent(
            kind=LogEventKind.GANESHA_RESTART,
            timestamp=ts,
            raw_line=line,
            message=line.strip(),
        )

    if _HARD_LIMIT_PATTERN.search(line):
        return LogEvent(
            kind=LogEventKind.HARD_LIMIT,
            timestamp=ts,
            raw_line=line,
            total_fd=total, global_fd=global_fd,
            state_fd=state_fd, temp_fd=temp_fd,
            message=line.strip(),
        )

    if _STATE_FD_PATTERN.search(line):
        return LogEvent(
            kind=LogEventKind.STATE_FD_PRESSURE,
            timestamp=ts,
            raw_line=line,
            total_fd=total, global_fd=global_fd,
            state_fd=state_fd, temp_fd=temp_fd,
            message=line.strip(),
        )

    if _FUTILITY_PATTERN.search(line):
        fc = 0
        fm = _FUTILITY_CNT.search(line)
        if fm:
            fc = int(fm.group(1))
        return LogEvent(
            kind=LogEventKind.FUTILITY,
            timestamp=ts,
            raw_line=line,
            total_fd=total, global_fd=global_fd,
            state_fd=state_fd, temp_fd=temp_fd,
            futility_count=fc,
            message=line.strip(),
        )

    if _HIWAT_PATTERN.search(line):
        hiwat = 0
        hm = _HIWAT_NUM.search(line)
        if hm:
            hiwat = int(hm.group(1))
        lowat = 0
        lm = _LOWAT_NUM.search(line)
        if lm:
            lowat = int(lm.group(1))
        return LogEvent(
            kind=LogEventKind.HIGH_WATERMARK,
            timestamp=ts,
            raw_line=line,
            total_fd=total, global_fd=global_fd,
            state_fd=state_fd, temp_fd=temp_fd,
            hiwat=hiwat, lowat=lowat,
            message=line.strip(),
        )

    if _DIAG_PATTERN.search(line):
        return LogEvent(
            kind=LogEventKind.FD_COUNT_DIAG,
            timestamp=ts,
            raw_line=line,
            total_fd=total, global_fd=global_fd,
            state_fd=state_fd, temp_fd=temp_fd,
            message=line.strip(),
        )

    return None


def parse_log_text(text: str, now: Optional[float] = None) -> List[LogEvent]:
    """Parse an entire log excerpt and return all recognised events."""
    if now is None:
        now = time.time()
    events: List[LogEvent] = []
    for line in text.splitlines():
        ev = parse_log_line(line.strip(), now)
        if ev is not None:
            events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Event collection helpers
# ---------------------------------------------------------------------------

def high_watermark_events(events: List[LogEvent]) -> List[LogEvent]:
    return [e for e in events if e.kind == LogEventKind.HIGH_WATERMARK]


def hard_limit_events(events: List[LogEvent]) -> List[LogEvent]:
    return [e for e in events if e.kind == LogEventKind.HARD_LIMIT]


def futility_events(events: List[LogEvent]) -> List[LogEvent]:
    return [e for e in events if e.kind == LogEventKind.FUTILITY]


def state_pressure_events(events: List[LogEvent]) -> List[LogEvent]:
    return [e for e in events if e.kind == LogEventKind.STATE_FD_PRESSURE]


def restart_events(events: List[LogEvent]) -> List[LogEvent]:
    return [e for e in events if e.kind == LogEventKind.GANESHA_RESTART]

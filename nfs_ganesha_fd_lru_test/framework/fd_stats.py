"""
FD statistics data model and ganesha_stats inode parser.

Parses the output of ``ganesha_stats inode`` into FDSample objects.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# FD sample
# ---------------------------------------------------------------------------

@dataclass
class FDSample:
    """One snapshot of Ganesha FD counters.

    Relationship between fields
    ---------------------------
    LRU entry  =  inode cache entry.  Exists as long as the inode is cached.
                  The LRU may close the *FD* while keeping the inode entry —
                  that is the reclamation mechanism.

    FSAL opened FD count  =  the subset of LRU entries that currently have
                              an open file descriptor at the FSAL layer.
                              Can legitimately be 0 while lru_entries_in_use
                              is non-zero (all FDs reclaimed, inodes still
                              cached).

    Accounting identity (when per-category breakdown is available):
        FSAL opened FD count  ==  global_fd  +  state_fd  +  temp_fd
    """

    timestamp: float = field(default_factory=time.time)

    # Direct ganesha_stats inode fields
    fsal_opened_fd: int = 0       # FDs currently open at FSAL — subset of LRU entries
    system_fd_limit: int = 0
    # fd_usage_pct: computed from fsal_opened_fd/system_fd_limit when the
    # ganesha_stats output reports a text label ("Below Low Water Mark" etc.)
    # rather than a numeric percentage.
    fd_usage_pct: float = 0.0
    fd_usage_label: str = ""      # raw text label from ganesha_stats, if present
    lru_entries_in_use: int = 0   # all cached inodes (FD open or reclaimed)
    chunks_in_use: int = 0

    # Per-category FD breakdown (absent in some builds — all zero when not reported)
    total_fd: int = 0    # == fsal_opened_fd when populated; backfilled from it
    global_fd: int = 0   # NFSv3 + NFSv4 reclaimable FDs (go through LRU)
    state_fd: int = 0    # NFSv4 state FDs (closed by client CLOSE, NOT LRU)
    temp_fd: int = 0     # short-lived FDs, reclaimed quickly by LRU

    raw_output: str = ""

    @property
    def is_valid(self) -> bool:
        return self.fsal_opened_fd >= 0 and self.system_fd_limit > 0

    @property
    def effective_total_fd(self) -> int:
        """
        The authoritative total open FD count.

        ``FSAL opened FD count = global_fd + state_fd + temp_fd``

        When a separate ``Total FDs`` breakdown field is absent (as in this
        build), ``fsal_opened_fd`` IS the total — but only when it is > 0.
        Zero is a valid and normal state (all FDs reclaimed by LRU).
        """
        if self.total_fd > 0:
            return self.total_fd
        # fsal_opened_fd == 0 is valid: LRU reclaimed all FDs, inodes still cached.
        # Only use it as the total when the breakdown fields are also non-zero,
        # meaning we have real data to validate against.
        return self.fsal_opened_fd

    @property
    def fd_accounting_check(self) -> Tuple[bool, int]:
        """
        Validate:  FSAL opened FD count == global_fd + state_fd + temp_fd.

        Returns (ok, discrepancy).

        Skipped (returns True, 0) when:
        - The per-category breakdown is not available (all three are 0), OR
        - fsal_opened_fd == 0 (LRU has reclaimed all FDs — nothing to validate).
        """
        if self.global_fd == 0 and self.state_fd == 0 and self.temp_fd == 0:
            return True, 0   # breakdown not available
        if self.fsal_opened_fd == 0:
            return True, 0   # all FDs reclaimed — accounting is vacuously correct
        expected = self.global_fd + self.state_fd + self.temp_fd
        total = self.effective_total_fd
        discrepancy = abs(total - expected)
        return discrepancy == 0, discrepancy

    def __str__(self) -> str:  # pragma: no cover
        ts = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        usage = self.fd_usage_label if self.fd_usage_label else f"{self.fd_usage_pct:.1f}%"
        return (
            f"[{ts}] fsal_fd={self.fsal_opened_fd} "
            f"limit={self.system_fd_limit} "
            f"usage={usage} "
            f"lru={self.lru_entries_in_use} "
            f"total={self.total_fd} "
            f"global={self.global_fd} "
            f"state={self.state_fd} "
            f"temp={self.temp_fd}"
        )


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Number token: digits with optional comma-thousands separators (e.g. 3,227)
_NUM = r"[\d,]+"

def _int(raw: str) -> int:
    """Parse a possibly comma-formatted integer string."""
    return int(raw.replace(",", ""))


# Patterns that cover common ganesha_stats inode output layouts.
# Keys are normalised field names.
#
# fsal_opened_fd aliases across known Ganesha builds / versions:
#   "FSAL opened FD count"     — this build (RHEL9 / gpfs-backed)
#   "FSAL opened FD"           — upstream / standard builds
#   "fd_used"                  — some RHEL/CentOS packaging
#   "open_fds" / "nr_open_fds" — alternate stat key names
#   "Current open FDs"         — some downstream forks
#   "Inode FD"                 — older builds that report per-inode FD count
#   "nr_fds"                   — compact key format
#
# fd_usage: this build outputs a text label ("Below Low Water Mark",
#   "Above High Water Mark") rather than a numeric percentage.
#   _USAGE_LABEL_RE captures that text; fd_usage_pct is then computed
#   from fsal_opened_fd / system_fd_limit * 100.
#
_PATTERNS: Dict[str, List[re.Pattern]] = {
    "fsal_opened_fd": [
        re.compile(r"FSAL\s+opened\s+FD\s+count\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"FSAL\s+opened\s+FD\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"fd_used\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"open_fds\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"nr_open_fds\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"nr_fds\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"Current\s+open\s+FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"Inode\s+FD\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "system_fd_limit": [
        re.compile(r"System\s+limit\s+(?:on\s+)?FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"fd_limit\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"max_fds?\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "fd_usage_pct": [
        re.compile(r"FD\s+usage\s*[:\-=]\s*([\d.]+)\s*%?", re.I),
        re.compile(r"fd_usage_pct\s*[:\-=]\s*([\d.]+)", re.I),
    ],
    "lru_entries": [
        re.compile(r"LRU\s+entries\s+in\s+use\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"lru_entries\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "chunks_in_use": [
        re.compile(r"Chunks\s+in\s+use\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"chunks_in_use\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    # Patch-1247084 extended breakdown
    "total_fd": [
        re.compile(r"Total\s+FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"total_fds?\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "global_fd": [
        re.compile(r"Global\s+FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"global_fds?\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "state_fd": [
        re.compile(r"State\s+FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"state_fds?\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
    "temp_fd": [
        re.compile(r"Temp(?:orary)?\s+FDs?\s*[:\-=]\s*(" + _NUM + r")", re.I),
        re.compile(r"temp(?:orary)?_fds?\s*[:\-=]\s*(" + _NUM + r")", re.I),
    ],
}


# Pattern to capture the text label when FD usage is not a number.
# Matches: "FD usage : Below Low Water Mark" / "FD usage : Above High Water Mark" etc.
_USAGE_LABEL_RE = re.compile(
    r"FD\s+usage\s*[:\-=]\s*([A-Za-z][^\n\r]*)", re.I
)


def parse_ganesha_stats(output: str) -> FDSample:
    """
    Parse the text output of ``ganesha_stats inode`` into a :class:`FDSample`.

    Each field has a list of patterns tried in order; the first match wins.
    Numbers may include comma thousands-separators (e.g. "3,227").

    When "FD usage" is a text label instead of a number (e.g. this build
    reports "Below Low Water Mark"), fd_usage_label captures the raw text
    and fd_usage_pct is computed as fsal_opened_fd / system_fd_limit * 100.

    The parser is intentionally lenient: unknown/missing fields default to 0.
    """
    sample = FDSample(timestamp=time.time(), raw_output=output)

    for key, patterns in _PATTERNS.items():
        for pattern in patterns:
            m = pattern.search(output)
            if not m:
                continue
            raw_val = m.group(1)
            if key == "fd_usage_pct":
                sample.fd_usage_pct = float(raw_val)
            elif key == "lru_entries":
                sample.lru_entries_in_use = _int(raw_val)
            elif key == "chunks_in_use":
                sample.chunks_in_use = _int(raw_val)
            else:
                setattr(sample, key, _int(raw_val))
            break   # first matching pattern wins

    # If fd_usage_pct was not set from a number, check for a text label and
    # compute the percentage from the parsed FD counts instead.
    if sample.fd_usage_pct == 0.0:
        lm = _USAGE_LABEL_RE.search(output)
        if lm:
            sample.fd_usage_label = lm.group(1).strip()
        if sample.system_fd_limit > 0:
            sample.fd_usage_pct = sample.fsal_opened_fd / sample.system_fd_limit * 100.0

    # When no separate "Total FDs" field exists and FDs are actually open,
    # fsal_opened_fd IS the total (FSAL opened FD count = global + state + temp).
    # Backfill total_fd only when fsal_opened_fd > 0 so fd_accounting_check has
    # a consistent total to compare against.
    # When fsal_opened_fd == 0 (all FDs reclaimed, inodes still cached) there is
    # nothing to validate — leave total_fd = 0 to signal "skip accounting check".
    if sample.total_fd == 0 and sample.fsal_opened_fd > 0:
        sample.total_fd = sample.fsal_opened_fd

    return sample


def parse_dbus_fd_usage(output: str) -> FDSample:
    """
    Parse the output of dbus-send ShowFDUsage command into a :class:`FDSample`.

    Expected format:
        method return time=1788647786.969864 sender=:1.1007353 -> destination=:1.1032548 serial=26575 reply_serial=2
           boolean true
           string "OK"
           struct {
              uint64 1788647786
              uint64 969291083
           }
           struct {
              string "System limit on FDs"
              uint32 20000
              string "FD Low WaterMark"
              uint32 0
              string "FD High WaterMark"
              uint32 12000
              string "FD Hard Limt"
              uint32 18000
              string "FD usage"
              string "        Below High Water Mark "
              string "FSAL opened Global FD count"
              uint32 1774
              string "FSAL opened State FD count"
              uint32 0
              string "NFSv4 open state count"
              uint64 0
           }
    """
    sample = FDSample(timestamp=time.time(), raw_output=output)
    
    # Extract key-value pairs from the dbus struct output
    # Pattern: string "Key Name" followed by uint32/uint64 value
    lines = output.split('\n')
    
    current_key = None
    for line in lines:
        line = line.strip()
        
        # Match string keys
        string_match = re.match(r'string\s+"([^"]+)"', line)
        if string_match:
            current_key = string_match.group(1)
            continue
        
        # Match numeric values (uint32 or uint64)
        value_match = re.match(r'uint(?:32|64)\s+(\d+)', line)
        if value_match and current_key:
            value = int(value_match.group(1))
            
            # Map dbus keys to FDSample fields
            key_lower = current_key.lower()
            
            if "system limit" in key_lower and "fd" in key_lower:
                sample.system_fd_limit = value
            elif "fsal opened global fd" in key_lower:
                sample.global_fd = value
            elif "fsal opened state fd" in key_lower:
                sample.state_fd = value
            elif "nfsv4 open state count" in key_lower:
                # This is an alternative measure of state FDs
                if sample.state_fd == 0:
                    sample.state_fd = value
            elif "fd high watermark" in key_lower or "fd high water mark" in key_lower:
                # Store for reference but not directly used in FDSample
                pass
            elif "fd low watermark" in key_lower or "fd low water mark" in key_lower:
                # Store for reference but not directly used in FDSample
                pass
            
            current_key = None
    
    # Calculate total FSAL opened FD count
    # Total = Global + State (temp_fd not reported in dbus output)
    sample.fsal_opened_fd = sample.global_fd + sample.state_fd
    sample.total_fd = sample.fsal_opened_fd
    
    # Extract FD usage label if present
    usage_label_match = re.search(r'string\s+"FD usage"\s+string\s+"([^"]+)"', output, re.IGNORECASE)
    if usage_label_match:
        sample.fd_usage_label = usage_label_match.group(1).strip()
    
    # Calculate FD usage percentage
    if sample.system_fd_limit > 0:
        sample.fd_usage_pct = (sample.fsal_opened_fd / sample.system_fd_limit) * 100.0
    
    return sample


# ---------------------------------------------------------------------------
# Baseline analysis
# ---------------------------------------------------------------------------

@dataclass
class BaselineStats:
    samples: List[FDSample] = field(default_factory=list)

    @property
    def stable(self) -> bool:
        """True if FSAL opened FD count does not change meaningfully."""
        if len(self.samples) < 2:
            return True
        fds = [s.fsal_opened_fd for s in self.samples]
        spread = max(fds) - min(fds)
        avg = sum(fds) / len(fds)
        if avg == 0:
            return True
        return (spread / avg) < 0.10  # < 10 % variation is considered stable

    @property
    def average_fsal_fd(self) -> float:
        if not self.samples:
            return 0.0
        return sum(s.fsal_opened_fd for s in self.samples) / len(self.samples)

    @property
    def system_fd_limit(self) -> int:
        if not self.samples:
            return 0
        return self.samples[-1].system_fd_limit

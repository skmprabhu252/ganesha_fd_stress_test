"""
Report generator.

Produces a structured, human-readable final report from a SuiteVerdict
plus the accumulated test context (environment, workload counters,
FD/LRU time series).
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from .config import TestConfig
from .fd_stats import BaselineStats
from .monitor import MonitorPhase
from .verdict import CycleVerdict, DimensionResult, SuiteVerdict, Verdict
from .workload import WorkloadStats


# ---------------------------------------------------------------------------
# Environment snapshot captured once at test start
# ---------------------------------------------------------------------------

@dataclass
class EnvironmentInfo:
    server_address: str = ""
    ganesha_version: str = ""
    kernel_os: str = ""
    nfs_export: str = ""
    protocol: str = ""
    fd_system_limit: int = 0
    client_addresses: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

SEPARATOR = "=" * 72


class ReportBuilder:
    def __init__(
        self,
        config: TestConfig,
        env: EnvironmentInfo,
        baseline: BaselineStats,
    ) -> None:
        self.config = config
        self.env = env
        self.baseline = baseline
        self._lines: List[str] = []

    def _ln(self, line: str = "") -> None:
        self._lines.append(line)

    def _section(self, title: str) -> None:
        self._ln()
        self._ln(SEPARATOR)
        self._ln(f"  {title}")
        self._ln(SEPARATOR)

    # ------------------------------------------------------------------
    # Section writers
    # ------------------------------------------------------------------

    def _write_environment(self) -> None:
        self._section("1. ENVIRONMENT")
        self._ln(f"  Server             : {self.env.server_address}")
        self._ln(f"  NFS Export         : {self.env.nfs_export}")
        self._ln(f"  Protocol           : {self.env.protocol}")
        self._ln(f"  Ganesha Version    : {self.env.ganesha_version or '(unknown)'}")
        self._ln(f"  Kernel/OS          : {self.env.kernel_os or '(unknown)'}")
        self._ln(f"  FD System Limit    : {self.env.fd_system_limit:,}")
        self._ln(f"  Clients            : {', '.join(self.env.client_addresses)}")

    def _write_workload_config(self) -> None:
        wl = self.config.workload
        self._section("2. WORKLOAD CONFIGURATION")
        self._ln(f"  Cycles             : {self.config.num_cycles}")
        self._ln(f"  Threads/client     : {wl.threads_per_client}")
        self._ln(f"  Files/thread       : {wl.num_files}")
        self._ln(f"  File size          : {wl.file_size_bytes:,} bytes")
        self._ln(f"  Directories        : {wl.num_directories}")
        self._ln(f"  Files/directory    : {wl.files_per_directory}")
        self._ln(f"  Held-open files    : {wl.held_open_files}")
        self._ln(f"  Burst duration     : {wl.burst_duration_sec}s")
        self._ln(f"  Cooldown duration  : {wl.cooldown_duration_sec}s")
        self._ln(f"  Retry timeout      : {wl.retry_timeout_sec}s")
        self._ln(f"  FD tolerance       : {self.config.fd_tolerance_pct}%")

    def _write_baseline(self) -> None:
        self._section("3. BASELINE FD STATISTICS")
        if not self.baseline.samples:
            self._ln("  (no baseline samples collected)")
            return
        stable = "STABLE" if self.baseline.stable else "UNSTABLE — server FD count was changing before workload"
        self._ln(f"  Baseline stability : {stable}")
        self._ln(f"  Average Total FD   : {self.baseline.average_fsal_fd:,.0f}")
        self._ln(f"  System FD limit    : {self.baseline.system_fd_limit:,}")
        self._ln()
        # Column notes:
        #   Total FD  = total FSAL-opened FDs (= Global + State + Temp when breakdown available)
        #   Global FD = NFSv3 + reclaimable NFSv4 FDs managed by the LRU
        #   State FD  = NFSv4 open-state FDs (closed by client CLOSE, never by LRU)
        #   Temp FD   = short-lived FDs reclaimed quickly by the LRU
        #   LRU cache = total inode-cache entries (superset of Total FD; FDs may be
        #               reclaimed while the inode entry remains in cache)
        self._ln(f"  {'#':<4} {'Total FD':>12} {'Limit':>12} {'Usage%':>8} {'Global':>10} {'State':>10} {'Temp':>10} {'LRU cache':>10}")
        self._ln(f"  {'-'*4} {'-'*12} {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
        for i, s in enumerate(self.baseline.samples, 1):
            self._ln(
                f"  {i:<4} {s.fsal_opened_fd:>12,} {s.system_fd_limit:>12,} "
                f"{s.fd_usage_pct:>8.1f} {s.global_fd:>10,} {s.state_fd:>10,} "
                f"{s.temp_fd:>10,} {s.lru_entries_in_use:>10,}"
            )

    def _write_fd_time_series(
        self,
        phases: List[MonitorPhase],
    ) -> None:
        self._section("4. FD/LRU TIME SERIES")
        # Column key:
        #   Total FD  = FSAL opened FD count (= Global + State + Temp when breakdown available)
        #   Global FD = reclaimable FDs managed by the LRU (NFSv3 + reclaimable NFSv4)
        #   State FD  = NFSv4 open-state FDs (client-closed, not LRU-managed)
        #   Temp FD   = short-lived FDs quickly reclaimed by the LRU
        #   LRU cache = inode-cache entries (superset; includes entries whose FDs were reclaimed)
        for phase in phases:
            self._ln(f"\n  Phase: {phase.label.upper()}")
            if not phase.samples:
                self._ln("    (no samples)")
                continue
            self._ln(
                f"  {'Time':>8} {'Total FD':>12} {'Global':>10} {'State':>10} "
                f"{'Temp':>10} {'Usage%':>8} {'LRU cache':>10}"
            )
            self._ln(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*10}")
            for s in phase.samples:
                ts = time.strftime("%H:%M:%S", time.localtime(s.timestamp))
                self._ln(
                    f"  {ts:>8} {s.fsal_opened_fd:>12,} {s.global_fd:>10,} "
                    f"{s.state_fd:>10,} {s.temp_fd:>10,} "
                    f"{s.fd_usage_pct:>8.1f} {s.lru_entries_in_use:>10,}"
                )

    def _write_log_events(self, phases: List[MonitorPhase]) -> None:
        self._section("5. GANESHA LOG EVENTS (CORRELATED)")
        # phase.events are deduplicated within each phase, but consecutive phases
        # can pull overlapping tails. Deduplicate by raw_line across all phases.
        seen = set()
        unique_events = []
        for p in phases:
            for e in p.events:
                if e.raw_line not in seen:
                    seen.add(e.raw_line)
                    unique_events.append(e)

        all_events = sorted(
            unique_events,
            key=lambda e: e.timestamp,
        )
        if not all_events:
            self._ln("  (no structured log events captured)")
            return

        for ev in all_events:
            ts = time.strftime("%H:%M:%S", time.localtime(ev.timestamp))
            self._ln(
                f"  [{ts}] {ev.kind.name:<22} "
                f"total={ev.total_fd:>8,} global={ev.global_fd:>8,} "
                f"state={ev.state_fd:>8,} temp={ev.temp_fd:>8,}  "
                f"{ev.message[:80]}"
            )

    def _write_workload_stats(self, all_stats: List[WorkloadStats]) -> None:
        self._section("6. WORKLOAD COUNTERS")
        merged = WorkloadStats()
        for s in all_stats:
            merged.merge(s)

        self._ln(f"  Opens attempted    : {merged.opens_attempted:,}")
        self._ln(f"  Opens succeeded    : {merged.opens_succeeded:,}")
        self._ln(f"  Opens failed       : {merged.opens_failed:,}")
        self._ln(f"  Opens retried      : {merged.opens_retried:,}")
        self._ln(f"  Opens eventually OK: {merged.opens_eventually_ok:,}")
        self._ln(f"  Closes             : {merged.closes:,}")
        self._ln(f"  Creates            : {merged.creates:,}")
        self._ln(f"  Reads              : {merged.reads:,}")
        self._ln(f"  Writes             : {merged.writes:,}")
        self._ln(f"  Directory ops      : {merged.dir_ops:,}")
        self._ln(f"  Active handles     : {merged.active_handles:,}")
        self._ln(f"  Active handle fail : {merged.active_handle_failures:,}")
        self._ln(f"  Client EMFILE      : {merged.emfile_count:,}")
        self._ln(f"  Server EIO         : {merged.eio_count:,}")
        self._ln(f"  ESTALE             : {merged.estale_count:,}")

    def _write_verdict(self, suite: SuiteVerdict) -> None:
        self._section("7. VERDICT")
        for cv in suite.cycle_verdicts:
            self._ln(str(cv))

        self._ln()
        self._ln(f"  {'--- Final Suite Dimensions ---':}")
        for d in suite.final_dimensions:
            self._ln(str(d))

        self._ln()
        self._ln(f"  {'PROTOCOL':<30} {self.env.protocol}")
        self._ln()

        # Flat summary
        _DIMS_OF_INTEREST = [
            "V3_workload_completion",
            "V4_workload_completion",
            "BOTH_workload_completion",
            "lru_reclamation",
            "fd_settled_after_cooldown",
            "fd_retention_across_cycles",
            "fd_accounting",
            "high_watermark_handling",
            "hard_limit_handling",
            "ganesha_no_restart",
        ]

        # Collect the worst verdict for each dimension across all cycles
        worst: Dict[str, Verdict] = {}
        all_dims = [d for cv in suite.cycle_verdicts for d in cv.dimensions]
        all_dims += suite.final_dimensions
        for d in all_dims:
            prev = worst.get(d.name, Verdict.PASS)
            order = [Verdict.PASS, Verdict.INCONCLUSIVE, Verdict.WARNING, Verdict.FAIL]
            if order.index(d.verdict) > order.index(prev):
                worst[d.name] = d.verdict

        self._ln(f"  {'Dimension':<40} {'Result'}")
        self._ln(f"  {'-'*40} {'-'*12}")
        for name, verdict in sorted(worst.items()):
            self._ln(f"  {name:<40} {verdict.value}")

        self._ln()
        overall_str = suite.overall.value
        self._ln(f"  {'OVERALL':=<40} {overall_str:=>12}")

    # ------------------------------------------------------------------
    # Master builder
    # ------------------------------------------------------------------

    def build(
        self,
        suite: SuiteVerdict,
        phases: List[MonitorPhase],
        all_stats: List[WorkloadStats],
    ) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        self._ln(SEPARATOR)
        self._ln(f"  NFS-GANESHA FD/LRU STRESS AND RECLAMATION TEST REPORT")
        self._ln(f"  Generated: {ts}")
        self._ln(SEPARATOR)

        self._write_environment()
        self._write_workload_config()
        self._write_baseline()
        self._write_fd_time_series(phases)
        self._write_log_events(phases)
        self._write_workload_stats(all_stats)
        self._write_verdict(suite)

        self._ln()
        self._ln(SEPARATOR)
        return "\n".join(self._lines)

    def to_json(
        self,
        suite: SuiteVerdict,
        all_stats: List[WorkloadStats],
    ) -> str:
        """Return a machine-readable JSON summary."""
        data = {
            "environment": {
                "server": self.env.server_address,
                "protocol": self.env.protocol,
                "fd_system_limit": self.env.fd_system_limit,
                "clients": self.env.client_addresses,
            },
            "baseline": {
                "stable": self.baseline.stable,
                "average_fsal_fd": self.baseline.average_fsal_fd,
            },
            "cycle_verdicts": [
                {
                    "cycle": cv.cycle,
                    "protocol": cv.protocol,
                    "overall": cv.overall.value,
                    "dimensions": [
                        {"name": d.name, "verdict": d.verdict.value, "reason": d.reason}
                        for d in cv.dimensions
                    ],
                }
                for cv in suite.cycle_verdicts
            ],
            "final_dimensions": [
                {"name": d.name, "verdict": d.verdict.value, "reason": d.reason}
                for d in suite.final_dimensions
            ],
            "overall": suite.overall.value,
            "aggregate_stats": (
                WorkloadStats.__new__(WorkloadStats).__dict__
                if not all_stats
                else {f: getattr(
                    next(
                        (s for s in all_stats), WorkloadStats()
                    ), f
                ) for f in WorkloadStats.__dataclass_fields__}
            ),
        }
        # Merge all stats into aggregate
        merged = WorkloadStats()
        for s in all_stats:
            merged.merge(s)
        data["aggregate_stats"] = merged.as_dict()
        return json.dumps(data, indent=2)

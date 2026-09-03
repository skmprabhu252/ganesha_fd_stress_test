"""
Verdict engine.

Evaluates the accumulated evidence from one complete test cycle and
produces per-dimension verdicts:

  PASS
  WARNING (expected pressure, correct behavior)
  FAIL
  INCONCLUSIVE

The engine deliberately avoids classifying expected FD pressure events
(high-watermark, hard-limit, futility) as failures — only incorrect or
un-recovered behavior is a failure.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

from .config import ProtocolMode
from .fd_stats import FDSample
from .log_parser import LogEvent, LogEventKind
from .monitor import MonitorPhase
from .workload import WorkloadStats

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Verdict values
# ---------------------------------------------------------------------------

class Verdict(Enum):
    PASS         = "PASS"
    WARNING      = "WARNING"          # expected pressure, correct behavior
    FAIL         = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"     # test could not reach the required condition


# ---------------------------------------------------------------------------
# Per-dimension result
# ---------------------------------------------------------------------------

@dataclass
class DimensionResult:
    name: str
    verdict: Verdict
    reason: str
    evidence: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        verdict_str = self.verdict.value.ljust(12)
        return f"  {self.name:<30} {verdict_str}  {self.reason}"


# ---------------------------------------------------------------------------
# Full cycle verdict
# ---------------------------------------------------------------------------

@dataclass
class CycleVerdict:
    cycle: int
    protocol: str
    dimensions: List[DimensionResult] = field(default_factory=list)

    @property
    def overall(self) -> Verdict:
        verdicts = {d.verdict for d in self.dimensions}
        if Verdict.FAIL in verdicts:
            return Verdict.FAIL
        if Verdict.INCONCLUSIVE in verdicts:
            return Verdict.INCONCLUSIVE
        if Verdict.WARNING in verdicts:
            return Verdict.WARNING
        return Verdict.PASS

    def __str__(self) -> str:
        lines = [f"\n--- Cycle {self.cycle} ({self.protocol}) ---"]
        for d in self.dimensions:
            lines.append(str(d))
        lines.append(f"  {'OVERALL':<30} {self.overall.value}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Suite verdict (across all cycles)
# ---------------------------------------------------------------------------

@dataclass
class SuiteVerdict:
    protocol: str
    cycle_verdicts: List[CycleVerdict] = field(default_factory=list)
    final_dimensions: List[DimensionResult] = field(default_factory=list)

    @property
    def overall(self) -> Verdict:
        verdicts = {d.verdict for d in self.final_dimensions}
        # Also aggregate from per-cycle
        for cv in self.cycle_verdicts:
            verdicts.add(cv.overall)
        if Verdict.FAIL in verdicts:
            return Verdict.FAIL
        if Verdict.INCONCLUSIVE in verdicts:
            return Verdict.INCONCLUSIVE
        if Verdict.WARNING in verdicts:
            return Verdict.WARNING
        return Verdict.PASS

    def __str__(self) -> str:
        lines = ["\n=== Suite Verdict ==="]
        for cv in self.cycle_verdicts:
            lines.append(str(cv))
        lines.append("\n--- Final Dimensions ---")
        for d in self.final_dimensions:
            lines.append(str(d))
        lines.append(f"\n  {'OVERALL SUITE':<30} {self.overall.value}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Verdict helpers
# ---------------------------------------------------------------------------

def _v(condition: bool, name: str, pass_reason: str, fail_reason: str, evidence: Optional[List[str]] = None) -> DimensionResult:
    verdict = Verdict.PASS if condition else Verdict.FAIL
    reason  = pass_reason if condition else fail_reason
    return DimensionResult(name=name, verdict=verdict, reason=reason, evidence=evidence or [])


def _warn(name: str, reason: str, evidence: Optional[List[str]] = None) -> DimensionResult:
    return DimensionResult(name=name, verdict=Verdict.WARNING, reason=reason, evidence=evidence or [])


def _inconclusive(name: str, reason: str, evidence: Optional[List[str]] = None) -> DimensionResult:
    return DimensionResult(name=name, verdict=Verdict.INCONCLUSIVE, reason=reason, evidence=evidence or [])


# ---------------------------------------------------------------------------
# Core verdict evaluation
# ---------------------------------------------------------------------------

class VerdictEngine:
    def __init__(self, fd_tolerance_pct: float = 10.0, fd_accounting_tolerance: int = 100) -> None:
        self.fd_tolerance_pct = fd_tolerance_pct
        self.fd_accounting_tolerance = fd_accounting_tolerance

    # ------------------------------------------------------------------
    # Individual dimension checks
    # ------------------------------------------------------------------

    def check_workload_completion(
        self, stats: WorkloadStats, protocol: str
    ) -> DimensionResult:
        name = f"{protocol}_workload_completion"
        if stats.opens_attempted == 0:
            return _inconclusive(name, "No open operations recorded — workload may not have run")

        # Persistent failures = never eventually recovered
        persistent = stats.opens_failed - stats.opens_eventually_ok
        ok = persistent == 0 or (persistent / stats.opens_attempted) < 0.01  # < 1 % persistent failure
        return _v(
            ok, name,
            f"Workload completed. opens={stats.opens_succeeded}/{stats.opens_attempted} "
            f"retries={stats.opens_retried}",
            f"Persistent open failures: {persistent}/{stats.opens_attempted} "
            f"(emfile={stats.emfile_count} eio={stats.eio_count} estale={stats.estale_count})",
        )

    def check_active_handles(self, stats: WorkloadStats, protocol: str) -> DimensionResult:
        name = f"{protocol}_active_handle_protection"
        if stats.active_handles == 0:
            return _inconclusive(name, "No held-open handles were configured")
        ok = stats.active_handle_failures == 0
        return _v(
            ok, name,
            f"All {stats.active_handles} held-open handles remained valid",
            f"{stats.active_handle_failures}/{stats.active_handles} held-open handles failed "
            "(ESTALE/EBADF) — LRU incorrectly reclaimed active resources",
        )

    def check_ganesha_no_restart(self, phases: List[MonitorPhase]) -> DimensionResult:
        import re
        thread_re = re.compile(r"\[(\w+?)\d*\]")
        seen = set()
        restarts = []
        for p in phases:
            for e in p.events:
                if e.kind == LogEventKind.GANESHA_RESTART:
                    normalized_line = thread_re.sub(r"[\1]", e.raw_line)
                    key = (e.kind, int(e.timestamp), normalized_line)
                    if key not in seen:
                        seen.add(key)
                        restarts.append(e)
        ok = len(restarts) == 0
        evidence = [e.raw_line for e in restarts[:5]]
        return _v(
            ok, "ganesha_no_restart",
            "No Ganesha restart detected",
            f"Ganesha restarted {len(restarts)} time(s) during test",
            evidence=evidence,
        )

    def check_lru_reclamation(
        self, burst: MonitorPhase, cooldown: MonitorPhase, protocol: str = "V3"
    ) -> DimensionResult:
        """
        Evaluate whether the Ganesha reaper thread reclaimed FDs correctly.

        Ganesha reaper / LRU design
        ----------------------------
        Watermark thresholds (default Ganesha configuration):
          Hard limit  = 100 % of system_fd_limit  → triggers aggressive reap
          High Water Mark (HWM) = 90 % of system_fd_limit  → reaper target
          Low  Water Mark (LWM) = ~10 % of system_fd_limit → reaper STOPS here

        Reaper behaviour when hard limit is hit (NFSv3):
          1. Reaper thread wakes and aggressively closes LRU entries.
          2. Reaper reaps until FD count drops BELOW HWM (90 %).
          3. Reaper STOPS at HWM — it does NOT drive FDs to LWM during the
             burst.  FDs will only reach LWM level very slowly over a long
             idle period after all client opens have ceased.

        Therefore the ONLY correct pass criterion for a burst+cooldown test is:

            settled FD count  <  HWM  (i.e.  < 0.90 × system_fd_limit)

        A result between HWM and LWM is NORMAL and correct.
        A result above HWM after cooldown means the reaper failed to reclaim.
        A result at or below LWM during a short cooldown would be surprising
        and is NOT required.

        When system_fd_limit is unknown (no stats samples), we fall back to
        a relative-drop heuristic.

        NFSv4 note
        ----------
        State FDs are closed by the client CLOSE operation, not the LRU.
        Only global (reclaimable) FDs are subject to reaper reclamation; the
        same HWM target applies to the global_fd count.
        """
        name = "lru_reclamation"

        if not burst.samples or not cooldown.samples:
            return _inconclusive(name, "Insufficient FD samples to evaluate LRU reclamation")

        is_v4 = protocol in (ProtocolMode.V4, ProtocolMode.BOTH)

        # Derive system_fd_limit from the most recent sample that has it.
        all_samples = burst.samples + cooldown.samples
        fd_limit = next(
            (s.system_fd_limit for s in reversed(all_samples) if s.system_fd_limit > 0),
            0,
        )
        # HWM is Ganesha's default 90 % of the system FD limit.
        hwm = int(fd_limit * 0.90) if fd_limit > 0 else 0

        if is_v4:
            # V4: reclaimable = global_fd only; state_fd is closed by clients
            if burst.peak_global_fd > 0:
                peak_reclaim    = burst.peak_global_fd
                settled_reclaim = cooldown.settled_global_fd
                signal = "global FDs (V4 reclaimable)"
            else:
                # global_fd breakdown not available — approximate
                peak_state   = max((s.state_fd for s in burst.samples), default=0)
                peak_reclaim = max(0, burst.peak_fsal_fd - peak_state)
                last         = cooldown.samples[-1]
                settled_reclaim = max(0, last.fsal_opened_fd - last.state_fd)
                signal = "FSAL FDs minus state FDs"
        else:
            # V3: all open FDs are LRU-reclaimable
            peak_reclaim    = burst.peak_fsal_fd
            settled_reclaim = cooldown.settled_fsal_fd
            signal = "FSAL FDs (V3)"
            if peak_reclaim == 0:
                peak_reclaim    = burst.peak_lru_entries
                settled_reclaim = cooldown.settled_lru_entries
                signal = "LRU entries (V3 fallback)"

        if peak_reclaim == 0:
            return _inconclusive(name, "No FD data available — cannot evaluate LRU reclamation")

        # ── Primary check: reaper brought FDs below the HWM ──────────────────
        if hwm > 0:
            below_hwm = settled_reclaim < hwm
            hwm_pct   = hwm / fd_limit * 100.0
            if below_hwm:
                return DimensionResult(
                    name=name,
                    verdict=Verdict.PASS,
                    reason=(
                        f"Reaper reclaimed FDs to below HWM: "
                        f"{settled_reclaim:,} < HWM {hwm:,} ({hwm_pct:.0f}% of {fd_limit:,}) ✓  "
                        f"[{signal}: burst peak {peak_reclaim:,}]"
                    ),
                )
            # FDs are still above HWM after cooldown — reaper did not finish
            overshoot = settled_reclaim - hwm
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=(
                    f"Reaper did NOT bring FDs below HWM after cooldown: "
                    f"settled={settled_reclaim:,} ≥ HWM={hwm:,} ({hwm_pct:.0f}% of {fd_limit:,})  "
                    f"overshoot={overshoot:,}  [{signal}: burst peak {peak_reclaim:,}]"
                ),
            )

        # ── Fallback (no fd_limit available): relative-drop heuristic ─────────
        reclaimed_pct = (peak_reclaim - settled_reclaim) / peak_reclaim * 100.0
        if reclaimed_pct >= 50.0:
            return DimensionResult(
                name=name,
                verdict=Verdict.PASS,
                reason=f"LRU reclaimed {reclaimed_pct:.1f}% of peak {signal} "
                       f"({peak_reclaim:,} → {settled_reclaim:,})",
            )
        if reclaimed_pct >= 20.0:
            return DimensionResult(
                name=name,
                verdict=Verdict.WARNING,
                reason=f"Partial LRU reclamation {reclaimed_pct:.1f}% of {signal} — "
                       f"fd_limit unavailable, cannot check HWM "
                       f"({peak_reclaim:,} → {settled_reclaim:,})",
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.FAIL,
            reason=f"LRU reclamation insufficient: only {reclaimed_pct:.1f}% of {signal} "
                   f"reclaimed ({peak_reclaim:,} → {settled_reclaim:,})",
        )

    def check_v4_state_fd_closure(
        self, burst: MonitorPhase, cooldown: MonitorPhase
    ) -> DimensionResult:
        """
        NFSv4-specific check: state FDs must return to initial levels after cooldown.

        State FDs represent open-state records maintained by Ganesha on behalf
        of NFSv4 clients.  They are closed by the client via the CLOSE
        operation — the LRU has no authority to reclaim them.

        After the burst workload finishes and clients close their files,
        state_fd should return back to its pre-burst baseline.  If it
        stays elevated, it indicates either an NFSv4 state leak or clients
        that did not cleanly close their connections.

        This is a distinct signal from lru_reclamation (which only covers
        the reclaimable/global FD portion).
        """
        name = "v4_state_fd_closure"

        if not burst.samples or not cooldown.samples:
            return _inconclusive(name, "Insufficient samples to evaluate V4 state FD closure")

        initial_state = burst.samples[0].state_fd
        settled_state = cooldown.samples[-1].state_fd
        peak_state    = max((s.state_fd for s in burst.samples), default=0)

        if peak_state == 0:
            return _inconclusive(
                name,
                "state_fd breakdown not available in this build — "
                "V4 state FD closure cannot be evaluated independently",
            )

        # Ensure no state FD leak: settled_state must return back to initial_state.
        # We allow a small tolerance of up to 5 FDs or 1.0% of peak_state to avoid
        # noise from concurrent system locks/tasks.
        leaked_fds = max(0, settled_state - initial_state)
        leak_percentage_of_peak = (leaked_fds / peak_state) * 100.0 if peak_state > 0 else 0.0

        if leaked_fds <= 5 or leak_percentage_of_peak <= 1.0:
            return DimensionResult(
                name=name,
                verdict=Verdict.PASS,
                reason=f"V4 state FDs cleanly closed: initial={initial_state:,}, "
                       f"peak={peak_state:,}, settled={settled_state:,} (no FD leak detected)",
            )
        elif leak_percentage_of_peak <= 5.0:
            return DimensionResult(
                name=name,
                verdict=Verdict.WARNING,
                reason=f"Minor V4 state FD leak detected: settled state={settled_state:,} "
                       f"exceeds initial={initial_state:,} (leaked {leaked_fds:,} FDs, "
                       f"{leak_percentage_of_peak:.1f}% of peak)",
            )
        else:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=f"V4 state FD leak detected: settled state={settled_state:,} "
                       f"exceeds initial={initial_state:,} (leaked {leaked_fds:,} FDs, "
                       f"{leak_percentage_of_peak:.1f}% of peak)",
            )

    def check_fd_settled(self, cooldown: MonitorPhase) -> DimensionResult:
        name = "fd_settled_after_cooldown"
        if not cooldown.samples:
            return _inconclusive(name, "No cooldown samples")
        settled = cooldown.fd_settled()
        return _v(
            settled, name,
            # FDs settling between HWM and LWM after a burst is the normal,
            # correct outcome — the reaper stops at HWM, not at LWM.
            "FD usage stable after cooldown (level between HWM and LWM is normal)",
            "FD usage did not stabilise after cooldown — may indicate FD leak "
            "or reaper unable to keep up with open rate",
        )

    def check_fd_retention_across_cycles(
        self, settled_fds: List[int]
    ) -> DimensionResult:
        """
        Evaluate whether settled FD count is accumulating across cycles.

        settled_fds[i] = settled FSAL FD count after cooldown of cycle i.
        """
        name = "fd_retention_across_cycles"
        if len(settled_fds) < 2:
            return _inconclusive(name, "Need at least 2 cycles to evaluate FD retention")

        # Compute per-cycle increase relative to cycle-1 baseline
        baseline = settled_fds[0]
        if baseline == 0:
            baseline = 1  # avoid division by zero

        increases = [(v - baseline) / baseline * 100.0 for v in settled_fds]
        max_increase = max(increases)

        # A monotonically increasing trend above tolerance is a failure
        monotone_increase = all(
            settled_fds[i] <= settled_fds[i + 1] for i in range(len(settled_fds) - 1)
        )
        total_increase_pct = (settled_fds[-1] - settled_fds[0]) / baseline * 100.0

        evidence = [
            f"Cycle {i+1} settled: {v:,}" for i, v in enumerate(settled_fds)
        ]

        if monotone_increase and total_increase_pct > self.fd_tolerance_pct:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=f"FD retention detected: settled FD count grew monotonically "
                       f"+{total_increase_pct:.1f}% over {len(settled_fds)} cycles "
                       f"(tolerance={self.fd_tolerance_pct}%)",
                evidence=evidence,
            )
        if total_increase_pct > self.fd_tolerance_pct:
            return DimensionResult(
                name=name,
                verdict=Verdict.WARNING,
                reason=f"FD count increased +{total_increase_pct:.1f}% across cycles "
                       f"(tolerance={self.fd_tolerance_pct}%) — non-monotonic, investigate",
                evidence=evidence,
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.PASS,
            reason=f"Settled FD count stable across {len(settled_fds)} cycles "
                   f"(max change +{total_increase_pct:.1f}%)",
            evidence=evidence,
        )

    def check_fd_accounting(self, phases: List[MonitorPhase]) -> DimensionResult:
        name = "fd_accounting"
        violations: List[str] = []

        for phase in phases:
            for sample in phase.samples:
                ok, discrepancy = sample.fd_accounting_check
                if not ok and discrepancy > self.fd_accounting_tolerance:
                    violations.append(
                        f"phase={phase.label} "
                        f"total={sample.total_fd} "
                        f"global+state+temp={sample.global_fd+sample.state_fd+sample.temp_fd} "
                        f"discrepancy={discrepancy}"
                    )

        if not violations:
            return DimensionResult(
                name=name,
                verdict=Verdict.PASS,
                reason="FD accounting consistent (total ≈ global+state+temp)",
            )
        violation_rate = len(violations) / max(
            sum(len(p.samples) for p in phases), 1
        )
        if violation_rate > 0.50:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=f"FD accounting inconsistency in {len(violations)} samples "
                       f"({violation_rate:.0%} of samples exceed tolerance={self.fd_accounting_tolerance})",
                evidence=violations[:5],
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.WARNING,
            reason=f"FD accounting inconsistency in {len(violations)} samples "
                   f"({violation_rate:.0%} — below 50% threshold, investigate)",
            evidence=violations[:5],
        )

    def check_high_watermark(
        self, burst: MonitorPhase, cooldown: MonitorPhase, protocol: str = "V3"
    ) -> DimensionResult:
        """
        Verify the Ganesha reaper correctly handles the high-water-mark condition.

        Ganesha reaper design
        ---------------------
        HWM (90 % of fd_limit) is the reaper's target, not its floor.
        When FDs cross HWM the reaper wakes; it reaps until FDs drop *below*
        HWM, then stops.  The Low Water Mark (~10 %) is only reached during a
        sustained idle period — it is NOT the expected end state after a short
        cooldown window.

        Verdicts
        --------
        WARNING      : HWM was reached AND settled FDs < HWM by end of cooldown
                       (reaper did its job — HWM events are expected under load).
        FAIL         : HWM was reached but settled FDs ≥ HWM after cooldown
                       (reaper woke but did not finish reclaiming).
        INCONCLUSIVE : HWM was not reached — workload pressure was insufficient.
        """
        name = "high_watermark_handling"
        if not burst.high_watermark_reached:
            return _inconclusive(
                name,
                "High watermark was not reached — FD pressure may have been insufficient. "
                "FD pressure exercised: LIMITED",
            )

        # Derive HWM threshold from samples (system_fd_limit × 0.90).
        all_samples = burst.samples + cooldown.samples
        fd_limit = next(
            (s.system_fd_limit for s in reversed(all_samples) if s.system_fd_limit > 0),
            0,
        )
        hwm = int(fd_limit * 0.90) if fd_limit > 0 else 0

        settled = cooldown.settled_fsal_fd
        if protocol in (ProtocolMode.V4, ProtocolMode.BOTH) and cooldown.settled_global_fd > 0:
            settled = cooldown.settled_global_fd

        if hwm > 0:
            # Reaper's job: bring FDs below HWM.
            if settled < hwm:
                return _warn(
                    name,
                    f"HWM reached (EXPECTED) → reaper woke → FDs reclaimed below HWM ✓  "
                    f"settled={settled:,} < HWM={hwm:,} ({hwm/fd_limit*100:.0f}% of {fd_limit:,})",
                )
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=(
                    f"HWM reached but reaper did NOT bring FDs below HWM after cooldown: "
                    f"settled={settled:,} ≥ HWM={hwm:,} ({hwm/fd_limit*100:.0f}% of {fd_limit:,})"
                ),
            )

        # fd_limit unknown — fall back to lru_made_progress heuristic
        lru_ok = cooldown.lru_made_progress(protocol, burst_phase=burst)
        if lru_ok:
            return _warn(name, "HWM reached (EXPECTED) → LRU woke → FDs reclaimed ✓")
        return DimensionResult(
            name=name,
            verdict=Verdict.FAIL,
            reason="HWM reached but LRU did not reclaim FDs meaningfully after cooldown "
                   "(fd_limit unavailable — used relative-drop heuristic)",
        )

    def check_hard_limit(self, burst: MonitorPhase, stats: WorkloadStats) -> DimensionResult:
        name = "hard_limit_handling"
        if not burst.hard_limit_reached:
            return _inconclusive(name, "Hard limit was not reached in this scenario")

        # Hard limit reached — did we recover?
        recovered = stats.opens_eventually_ok > 0 or stats.opens_failed == 0
        if recovered:
            return _warn(
                name,
                f"Hard limit reached (EXPECTED) → transient failures={stats.opens_failed} "
                f"→ recovery retries={stats.opens_retried} → eventually_ok={stats.opens_eventually_ok}",
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.FAIL,
            reason=f"Hard limit reached and client did not recover "
                   f"(failed={stats.opens_failed} eventually_ok={stats.opens_eventually_ok})",
        )

    def check_futility(
        self, burst: MonitorPhase, cooldown: MonitorPhase, protocol: str = "V3"
    ) -> DimensionResult:
        name = "futility_detection"
        if not burst.futility_detected:
            return _inconclusive(name, "No futility events detected in this cycle")

        # Futility + reclaimable FD remains high after cooldown + no settle = FAIL
        lru_no_progress = not cooldown.lru_made_progress(protocol, burst_phase=burst)
        no_settle = not cooldown.fd_settled()

        if lru_no_progress and no_settle:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason="Futility detected + global FD high after cooldown + FD did not settle — "
                       "strong evidence of LRU/reclamation failure",
            )
        return _warn(
            name,
            "Futility events detected (EXPECTED under high load) — "
            "LRU subsequently made progress ✓",
        )

    def check_state_fd_pressure(self, burst: MonitorPhase) -> DimensionResult:
        name = "state_fd_pressure"
        if not burst.state_fd_pressure_detected:
            return DimensionResult(
                name=name,
                verdict=Verdict.PASS,
                reason="No state-FD pressure events detected",
            )
        return _warn(
            name,
            "State-FD pressure detected (DIAGNOSTIC) — "
            "aggressive LRU reclamation not expected for state FDs. "
            "Not classified as LRU failure.",
        )

    def check_server_monitoring(self, phases: List[MonitorPhase]) -> DimensionResult:
        name = "server_monitoring"
        total_samples = sum(len(p.samples) for p in phases)
        if total_samples == 0:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason="SERVER MONITORING FAILURE: no ganesha_stats samples collected",
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.PASS,
            reason=f"Server monitoring OK — {total_samples} FD samples collected",
        )

    def check_mount_loss(self, stats: WorkloadStats, protocol: str) -> DimensionResult:
        name = f"{protocol}_no_mount_loss"
        # ESTALE often indicates mount loss
        if stats.estale_count > 0 and (stats.estale_count / max(stats.opens_attempted, 1)) > 0.05:
            return DimensionResult(
                name=name,
                verdict=Verdict.FAIL,
                reason=f"Possible mount loss: {stats.estale_count} ESTALE errors "
                       f"({stats.estale_count/stats.opens_attempted:.1%} of opens)",
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.PASS,
            reason=f"No mount-loss indicators (ESTALE={stats.estale_count})",
        )

    def check_client_fd_exhaustion(self, stats: WorkloadStats, protocol: str) -> DimensionResult:
        """
        Distinguish client-side EMFILE from server-side FD pressure.
        """
        name = f"{protocol}_client_fd_exhaustion"
        if stats.emfile_count > 0:
            return _warn(
                name,
                f"Client-side EMFILE detected ({stats.emfile_count} times) — "
                "this is client FD exhaustion, NOT Ganesha FD pressure",
            )
        return DimensionResult(
            name=name,
            verdict=Verdict.PASS,
            reason="No client-side EMFILE (client FD exhaustion) detected",
        )

    # ------------------------------------------------------------------
    # Per-cycle evaluation
    # ------------------------------------------------------------------

    def evaluate_cycle(
        self,
        cycle: int,
        protocol: str,
        stats: WorkloadStats,
        burst_phase: MonitorPhase,
        cooldown_phase: MonitorPhase,
    ) -> CycleVerdict:
        cv = CycleVerdict(cycle=cycle, protocol=protocol)

        is_v4 = protocol in (ProtocolMode.V4, ProtocolMode.BOTH)

        cv.dimensions += [
            self.check_workload_completion(stats, protocol),
            self.check_active_handles(stats, protocol),
            self.check_ganesha_no_restart([burst_phase, cooldown_phase]),
            self.check_lru_reclamation(burst_phase, cooldown_phase, protocol),
            self.check_fd_settled(cooldown_phase),
            self.check_fd_accounting([burst_phase, cooldown_phase]),
            self.check_high_watermark(burst_phase, cooldown_phase, protocol),
            self.check_hard_limit(burst_phase, stats),
            self.check_futility(burst_phase, cooldown_phase, protocol),
            self.check_state_fd_pressure(burst_phase),
            self.check_server_monitoring([burst_phase, cooldown_phase]),
            self.check_mount_loss(stats, protocol),
            self.check_client_fd_exhaustion(stats, protocol),
        ]

        # V4-specific: state FDs must be released by clients after cooldown
        if is_v4:
            cv.dimensions.append(
                self.check_v4_state_fd_closure(burst_phase, cooldown_phase)
            )

        return cv

    # ------------------------------------------------------------------
    # Suite-level evaluation
    # ------------------------------------------------------------------

    def evaluate_suite(
        self,
        protocol: str,
        cycle_verdicts: List[CycleVerdict],
        settled_fds: List[int],
        all_phases: List[MonitorPhase],
        all_stats: List[WorkloadStats],
    ) -> SuiteVerdict:
        sv = SuiteVerdict(protocol=protocol, cycle_verdicts=cycle_verdicts)

        # Aggregate stats across cycles
        combined_stats = WorkloadStats()
        for s in all_stats:
            combined_stats.merge(s)

        sv.final_dimensions += [
            self.check_fd_retention_across_cycles(settled_fds),
            self.check_fd_accounting(all_phases),
            self.check_ganesha_no_restart(all_phases),
            self.check_server_monitoring(all_phases),
            DimensionResult(
                name="aggregate_workload",
                verdict=Verdict.PASS if combined_stats.opens_succeeded > 0 else Verdict.FAIL,
                reason=(
                    f"Total opens: {combined_stats.opens_succeeded}/"
                    f"{combined_stats.opens_attempted} "
                    f"retries={combined_stats.opens_retried} "
                    f"active_handle_failures={combined_stats.active_handle_failures}"
                ),
            ),
        ]

        return sv

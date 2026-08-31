"""
Test runner base class and cycle executor.

All TC-XX test scenarios inherit from BaseScenario and delegate to
CycleRunner for the common burst→cooldown→verdict lifecycle.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .config import ProtocolMode, TestConfig
from .fd_stats import BaselineStats
from .monitor import MonitorPhase, ServerMonitor
from .preflight import PreflightError, run_preflight
from .report import EnvironmentInfo, ReportBuilder
from .ssh_client import SSHClient
from .verdict import CycleVerdict, SuiteVerdict, Verdict, VerdictEngine
from .workload import WorkloadStats, WorkloadWorker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cycle runner — orchestrates one burst+cooldown cycle
# ---------------------------------------------------------------------------

class CycleRunner:
    """
    Runs one burst+cooldown cycle and evaluates it.

    Before each burst the NFS export is mounted on every client (controller
    and workers) via SSH.  After the cooldown the mounts are released.

    Mount points follow the pattern:
        <client.mount_point>/v3   (NFSv3)
        <client.mount_point>/v4   (NFSv4)
    """

    def __init__(
        self,
        config: TestConfig,
        monitor: ServerMonitor,
        verdict_engine: VerdictEngine,
    ) -> None:
        self.config = config
        self.monitor = monitor
        self.verdict_engine = verdict_engine

    # ------------------------------------------------------------------
    # Mount helpers
    # ------------------------------------------------------------------

    def _client_ssh(self, client) -> SSHClient:
        return SSHClient(
            user=client.ssh_user,
            ssh_opts=client.ssh_opts,
            identity_file=client.identity_file,
        )

    @staticmethod
    def _nfs_versions(protocol: str) -> List[Tuple[str, str]]:
        """
        Return (nfs_ver, subdir) pairs for the given protocol mode.

        V3   → [("3", "v3")]
        V4   → [("4", "v4")]
        BOTH → [("3", "v3"), ("4", "v4")]
        """
        if protocol == ProtocolMode.V3:
            return [("3", "v3")]
        if protocol == ProtocolMode.V4:
            return [("4", "v4")]
        # BOTH
        return [("3", "v3"), ("4", "v4")]

    def _mount_nfs(self, client, ver: str, sub: str) -> str:
        """
        Mount the NFS export on *client* using NFS version *ver* at
        *<client.mount_point>/<sub>*.  Returns the mount point path.
        Raises RuntimeError if the mount fails.
        """
        mnt = os.path.join(client.mount_point, sub)
        ssh = self._client_ssh(client)
        cmd = (
            f"mkdir -p {mnt} && "
            f"mountpoint -q {mnt} && echo already_mounted || "
            f"mount -t nfs -o vers={ver} "
            f"{self.config.server.address}:{self.config.server.nfs_export} {mnt} && "
            f"echo mounted"
        )
        result = ssh.run_remote(client.address, cmd, timeout=30)
        if not result.ok:
            raise RuntimeError(
                f"NFS v{ver} mount failed on {client.address} ({mnt}): {result.stderr}"
            )
        logger.debug("mount %s:%s → %s:%s (v%s) OK", client.address, mnt,
                     self.config.server.address, self.config.server.nfs_export, ver)
        return mnt

    def _umount_nfs(self, client, sub: str) -> None:
        """Unmount *<client.mount_point>/<sub>* on *client* (best-effort)."""
        mnt = os.path.join(client.mount_point, sub)
        ssh = self._client_ssh(client)
        cmd = f"mountpoint -q {mnt} && umount -l {mnt} || true"
        result = ssh.run_remote(client.address, cmd, timeout=30)
        if not result.ok:
            logger.warning("umount %s:%s failed (ignored): %s",
                           client.address, mnt, result.stderr)

    def _cleanup_workload_dirs(self, client, sub: str) -> None:
        """
        Remove workload-owned subdirectories under the NFS mount point.

        Must be called after the mount is established but before the burst
        starts.  Without this, files left from a previous cycle remain on the
        server and keep Ganesha FDs open, which inflates the baseline FD count
        and prevents accurate measurement of per-cycle FD pressure.

        Only the directories created by the workload worker are removed:
          client_<id>_thread_NNN_<sub>/  — per-thread file trees
          _held_open_<id>/               — held-open handle files

        The operation is best-effort: a failure is logged but does not abort
        the cycle — a dirty mount is better than no test at all.
        """
        mnt = os.path.join(client.mount_point, sub)
        client_id = client.address.replace(".", "_").replace(":", "_")
        ssh = self._client_ssh(client)
        # Use shell globbing to remove all thread dirs and the held-open dir
        # for this client in one SSH round-trip.  The `|| true` ensures the
        # command succeeds even when the directories do not exist yet (cycle 1).
        cmd = (
            f"rm -rf {mnt}/client_{client_id}_thread_*_{sub} "
            f"{mnt}/_held_open_{client_id} || true"
        )
        result = ssh.run_remote(client.address, cmd, timeout=60)
        if not result.ok:
            logger.warning(
                "Workload dir cleanup failed on %s:%s (ignored): %s",
                client.address, mnt, result.stderr,
            )
        else:
            logger.debug("Cleaned up workload dirs on %s:%s", client.address, mnt)

    def _mount_all(self, protocol: str) -> None:
        """Mount all required NFS versions on every client."""
        for client in self.config.clients:
            for ver, sub in self._nfs_versions(protocol):
                self._mount_nfs(client, ver, sub)

    def _cleanup_all(self, protocol: str) -> None:
        """Remove workload files from all clients before a burst cycle."""
        for client in self.config.clients:
            for _ver, sub in self._nfs_versions(protocol):
                self._cleanup_workload_dirs(client, sub)

    def _umount_all(self, protocol: str) -> None:
        """Unmount all NFS mounts on every client (best-effort)."""
        for client in self.config.clients:
            for _ver, sub in self._nfs_versions(protocol):
                self._umount_nfs(client, sub)

    # ------------------------------------------------------------------
    # Cycle execution
    # ------------------------------------------------------------------

    def _make_workers(self, client, protocol: str) -> List[WorkloadWorker]:
        """
        Build one WorkloadWorker per NFS version required by *protocol*.

        V3   → one worker against /mnt/fd_stress/v3
        V4   → one worker against /mnt/fd_stress/v4
        BOTH → two workers, one against each mount point, running concurrently
        """
        wl = self.config.workload
        workers = []
        # Derive a filesystem-safe client identifier from the client address
        # (e.g. "172.16.4.91" → "172_16_4_91").  This is embedded in each
        # thread's subdirectory name so every client creates distinct server-side
        # files — preventing Ganesha from reusing a single global FD for the
        # same path opened from multiple clients.
        client_id = client.address.replace(".", "_").replace(":", "_")
        for _ver, sub in self._nfs_versions(protocol):
            mount_point = os.path.join(client.mount_point, sub)
            workers.append(WorkloadWorker(
                mount_point=mount_point,
                num_threads=wl.threads_per_client,
                num_files=wl.num_files,
                file_size_bytes=wl.file_size_bytes,
                held_open_count=wl.held_open_files,
                burst_duration_sec=wl.burst_duration_sec,
                retry_timeout_sec=wl.retry_timeout_sec,
                retry_interval_sec=wl.retry_interval_sec,
                protocol=sub,       # "v3" or "v4" — labels thread subdirs correctly
                client_id=client_id,
            ))
        return workers

    def run(
        self,
        cycle_number: int,
        protocol: str,
    ) -> Tuple[CycleVerdict, MonitorPhase, MonitorPhase, WorkloadStats]:
        """
        Execute one cycle.

        1. Mount NFS on every client via SSH.
        2. Launch the workload remotely on every client concurrently.
        3. Collect and merge stats from all clients.
        4. Run cooldown while the server monitor continues sampling.
        5. Unmount NFS on all clients.
        6. Evaluate and return the cycle verdict.

        Returns (CycleVerdict, burst_phase, cooldown_phase, aggregate_stats).
        """
        wl = self.config.workload
        logger.info("=== Cycle %d / Protocol %s ===", cycle_number, protocol)

        # Mount NFS on all clients before the burst, then clean up any files
        # left by the previous cycle so Ganesha starts each burst with a
        # clean FD slate.
        self._mount_all(protocol)
        self._cleanup_all(protocol)

        try:
            # --- BURST PHASE ---
            burst_phase = self.monitor.start_phase(f"burst_cycle_{cycle_number}")
            logger.info("  Burst phase started (duration=%ds)", wl.burst_duration_sec)

            # Launch workload on every client concurrently.
            # For BOTH protocol each client runs two workers (v3 + v4) in parallel.
            client_results: Dict[str, Optional[WorkloadStats]] = {}
            client_errors:  Dict[str, str] = {}
            threads = []

            def _run_on_client(client):
                workers = self._make_workers(client, protocol)
                ssh = self._client_ssh(client)
                # Run all workers for this client concurrently (v3 and v4 in parallel)
                per_worker_stats: List[Optional[WorkloadStats]] = [None] * len(workers)
                per_worker_errors: List[str] = [""] * len(workers)

                def _run_worker(idx, w):
                    try:
                        per_worker_stats[idx] = w.run_remote_burst(ssh, client.address)
                    except Exception as exc:  # pylint: disable=broad-except
                        per_worker_errors[idx] = str(exc)
                        logger.error("  Client %s worker[%s] FAILED: %s",
                                     client.address, w.mount_point, exc)

                wthreads = [
                    threading.Thread(
                        target=_run_worker,
                        args=(i, w),
                        daemon=True,
                        name=f"burst-{client.address}-{w.mount_point}",
                    )
                    for i, w in enumerate(workers)
                ]
                for wt in wthreads:
                    wt.start()
                for wt in wthreads:
                    wt.join(timeout=wl.burst_duration_sec + wl.retry_timeout_sec + 60)

                # Merge stats from all workers on this client
                merged = WorkloadStats()
                any_ok = False
                for i, ws in enumerate(per_worker_stats):
                    if ws is not None:
                        merged.merge(ws)
                        any_ok = True
                    elif per_worker_errors[i]:
                        client_errors[f"{client.address}:{workers[i].mount_point}"] = (
                            per_worker_errors[i]
                        )

                client_results[client.address] = merged if any_ok else None
                if any_ok:
                    logger.info("  Client %s burst complete (%d worker(s))",
                                client.address, len(workers))

            for client in self.config.clients:
                t = threading.Thread(
                    target=_run_on_client,
                    args=(client,),
                    daemon=True,
                    name=f"burst-{client.address}",
                )
                t.start()
                threads.append(t)

            # Wait for all clients (burst_duration + retry headroom + 60s)
            join_timeout = wl.burst_duration_sec + wl.retry_timeout_sec + 60
            for t in threads:
                t.join(timeout=join_timeout)

            self.monitor.stop_phase(burst_phase)

            # Merge stats from all clients into one aggregate
            stats = WorkloadStats()
            for client in self.config.clients:
                cs = client_results.get(client.address)
                if cs is not None:
                    stats.merge(cs)
                else:
                    logger.error(
                        "  No stats from client %s — treating as zero contribution",
                        client.address,
                    )

            logger.info(
                "  Burst done: opens=%d/%d failed=%d retries=%d "
                "held_failures=%d clients=%d ssh_errors=%d other_errors=%d",
                stats.opens_succeeded, stats.opens_attempted,
                stats.opens_failed, stats.opens_retried,
                stats.active_handle_failures,
                len(self.config.clients), len(client_errors),
                stats.other_errors,
            )

            # --- COOLDOWN PHASE ---
            cooldown_phase = self.monitor.start_phase(f"cooldown_cycle_{cycle_number}")
            logger.info("  Cooldown phase (%ds)...", wl.cooldown_duration_sec)

            remaining = wl.cooldown_duration_sec
            interval  = wl.cooldown_sample_interval_sec
            while remaining > 0:
                time.sleep(min(interval, remaining))
                remaining -= interval

            self.monitor.stop_phase(cooldown_phase)
            logger.info("  Cooldown done: samples=%d", len(cooldown_phase.samples))

        finally:
            # Unmount NFS on all clients after the cooldown regardless of errors
            self._umount_all(protocol)

        # --- EVALUATE ---
        cycle_verdict = self.verdict_engine.evaluate_cycle(
            cycle=cycle_number,
            protocol=protocol,
            stats=stats,
            burst_phase=burst_phase,
            cooldown_phase=cooldown_phase,
        )
        logger.info("  Cycle %d verdict: %s", cycle_number, cycle_verdict.overall.value)
        return cycle_verdict, burst_phase, cooldown_phase, stats


# ---------------------------------------------------------------------------
# Base scenario
# ---------------------------------------------------------------------------

class BaseScenario:
    """
    Abstract base for all TC-XX scenarios.

    Subclasses declare:
      - SCENARIO_ID  : str   e.g. "TC01"
      - DESCRIPTION  : str
      - REQUIRED_PROTOCOL : optional str (override default)

    and implement:
      - setup_extra_config()     modify config before run (optional)
      - post_cycle_hook()        called after each cycle (optional)
    """

    SCENARIO_ID: str = "TCXX"
    DESCRIPTION: str = "Base scenario"
    REQUIRED_PROTOCOL: Optional[str] = None   # None = use config.protocol

    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self._apply_protocol_override()
        self.ssh = SSHClient(
            user=config.server.ssh_user,
            ssh_opts=config.server.ssh_opts,
            identity_file=config.server.identity_file,
        )
        self.monitor = ServerMonitor(
            server=config.server,
            ssh=self.ssh,
            poll_interval_sec=5.0,
        )
        self.verdict_engine = VerdictEngine(
            fd_tolerance_pct=config.fd_tolerance_pct,
            fd_accounting_tolerance=config.fd_accounting_tolerance,
        )
        self.runner = CycleRunner(config, self.monitor, self.verdict_engine)

    def _apply_protocol_override(self) -> None:
        if self.REQUIRED_PROTOCOL:
            self.config.protocol = ProtocolMode.validate(self.REQUIRED_PROTOCOL)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def setup_extra_config(self) -> None:
        """Override to adjust workload config for this specific scenario."""

    def setup_pressure_config(self, fd_limit: int, num_clients: int) -> None:
        """
        Override to scale threads/files so the workload actually pressures the
        Ganesha FD limit.  Called after baseline collection, once fd_limit is
        known from the live server.  Default implementation does nothing.
        """

    def post_cycle_hook(self, cycle: int, cv: CycleVerdict) -> None:
        """Called after each cycle verdict is produced."""

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self) -> SuiteVerdict:
        logger.info(
            ">>> Scenario %s: %s  [protocol=%s, cycles=%d]",
            self.SCENARIO_ID, self.DESCRIPTION,
            self.config.protocol, self.config.num_cycles,
        )

        self.setup_extra_config()

        # Pre-flight
        preflight_report = run_preflight(self.config, ssh=self.ssh)
        logger.info(preflight_report.summary())
        if not preflight_report.passed:
            raise PreflightError(
                f"{self.SCENARIO_ID}: Pre-flight FAILED — aborting before workload.\n"
                + preflight_report.summary()
            )

        # Environment info
        env = self._collect_env_info()

        # Baseline
        baseline = BaselineStats(
            samples=self.monitor.collect_baseline(num_samples=5, interval_sec=3).samples
        )
        if not baseline.stable:
            logger.warning(
                "%s: Baseline FD count is UNSTABLE before workload starts", self.SCENARIO_ID
            )

        # Scale workload to the actual FD limit now that we have it from the server
        fd_limit = baseline.system_fd_limit or env.fd_system_limit
        if fd_limit > 0:
            self.setup_pressure_config(fd_limit, len(self.config.clients))

        # Cycles
        cycle_verdicts: List[CycleVerdict] = []
        all_burst_phases: List[MonitorPhase] = []
        all_cooldown_phases: List[MonitorPhase] = []
        all_stats: List[WorkloadStats] = []
        settled_fds: List[int] = []

        for cycle in range(1, self.config.num_cycles + 1):
            cv, burst, cooldown, stats = self.runner.run(cycle, self.config.protocol)
            cycle_verdicts.append(cv)
            all_burst_phases.append(burst)
            all_cooldown_phases.append(cooldown)
            all_stats.append(stats)
            settled_fds.append(cooldown.settled_fsal_fd)
            self.post_cycle_hook(cycle, cv)

            # Fail fast on Ganesha restart
            if cooldown.ganesha_restarted or burst.ganesha_restarted:
                logger.error("%s: Ganesha restart detected — stopping cycles", self.SCENARIO_ID)
                break

        # Suite verdict
        all_phases = all_burst_phases + all_cooldown_phases
        suite = self.verdict_engine.evaluate_suite(
            protocol=self.config.protocol,
            cycle_verdicts=cycle_verdicts,
            settled_fds=settled_fds,
            all_phases=all_phases,
            all_stats=all_stats,
        )

        # Final report
        report_builder = ReportBuilder(config=self.config, env=env, baseline=baseline)
        report_text = report_builder.build(suite, all_phases, all_stats)
        logger.info("\n%s", report_text)

        return suite

    # ------------------------------------------------------------------
    # Environment collection helpers
    # ------------------------------------------------------------------

    def _collect_env_info(self) -> EnvironmentInfo:
        env = EnvironmentInfo(
            server_address=self.config.server.address,
            nfs_export=self.config.server.nfs_export,
            protocol=self.config.protocol,
            client_addresses=[c.address for c in self.config.clients],
        )
        # OS info from server
        ssh_host = self.config.server.ssh_host
        r = self.ssh.run_remote(ssh_host, "uname -sr", timeout=10)
        if r.ok:
            env.kernel_os = r.stdout

        # FD limit from ganesha_stats (best effort)
        r2 = self.ssh.run_remote(
            ssh_host,
            self.config.server.ganesha_stats_cmd,
            timeout=15,
        )
        if r2.ok:
            from .fd_stats import parse_ganesha_stats
            sample = parse_ganesha_stats(r2.stdout)
            env.fd_system_limit = sample.system_fd_limit

        # Ganesha version (best effort)
        r3 = self.ssh.run_remote(
            ssh_host,
            "ganesha.nfsd --version 2>&1 | head -1",
            timeout=10,
        )
        if r3.ok:
            env.ganesha_version = r3.stdout.strip()

        return env

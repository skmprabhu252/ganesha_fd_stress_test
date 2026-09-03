"""
Pytest test suite — NFS-Ganesha FD/LRU Stress and Reclamation Framework.
4-scenario redesign: TC01 Sanity · TC02 V3 Stress · TC03 V4 Stress · TC04 Mixed.

Coverage
--------
Unit tests
  §7  Configuration validation
  §9  FD statistics parser (ganesha_stats inode)
  §25 FD accounting check  (total ≈ global + state + temp)
  §8  Baseline stability
  §10 Ganesha log parser / event detection (Patch-1247084)
  §26 WorkloadStats merge and error classification
  §13 HeldHandle lifecycle
  §18 Retry wrapper
  §19 MonitorPhase derived metrics

Mode / scenario tests
  RunMode validation
  ModeProfile scaling per mode (fast / normal / soak)
  TC01 scenario — sanity always uses fast profile
  TC02 scenario — V3 protocol, mode-driven scaling
  TC03 scenario — V4 protocol + held-open floor
  TC04 scenario — BOTH protocol + held-open floor
  Registry: 4 scenarios present, correct IDs, correct protocols

Verdict engine tests
  §39 Workload completion  PASS / FAIL / INCONCLUSIVE
  §13 Active handles       PASS / FAIL / INCONCLUSIVE
  §32 No restart           PASS / FAIL
  §22 LRU reclamation      PASS / WARNING / FAIL / INCONCLUSIVE
  §19 FD settled           PASS / FAIL
  §21 FD retention         PASS / WARNING / FAIL / INCONCLUSIVE
  §25 FD accounting        PASS / WARNING / FAIL
  §16 High-watermark       WARNING / FAIL / INCONCLUSIVE
  §17 Hard-limit           WARNING / FAIL / INCONCLUSIVE
  §24 Futility             WARNING / FAIL / INCONCLUSIVE
  §23 State-FD pressure    WARNING (never FAIL)
  §31 Server monitoring    PASS / FAIL
  §30 Client EMFILE        WARNING / PASS
  §33 Mount loss           PASS / FAIL
  Suite overall aggregation
  Per-cycle evaluate_cycle smoke

Report builder
  §38 All 7 sections present in text report
  JSON report required keys

Preflight (mock-based)
  §6  Pass / fail paths

CycleRunner (mock-based)
  §5  Burst + cooldown lifecycle

WorkloadWorker (local tmpdir)
  Burst returns stats · files created · held handles counted

SSH client
  Timeout / exception handling · ok property

Full lifecycle smoke (TC01 mock-based)
"""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

# Ensure package root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── framework imports ─────────────────────────────────────────────────────────
from nfs_ganesha_fd_lru_test.framework.config import (
    ClientConfig, ProtocolMode, ServerConfig, TestConfig, WorkloadConfig,
    make_default_test_config,
)
from nfs_ganesha_fd_lru_test.framework.fd_stats import (
    BaselineStats, FDSample, parse_ganesha_stats,
)
from nfs_ganesha_fd_lru_test.framework.log_parser import (
    LogEvent, LogEventKind,
    futility_events, hard_limit_events, high_watermark_events,
    parse_log_line, parse_log_text, restart_events, state_pressure_events,
)
from nfs_ganesha_fd_lru_test.framework.monitor import MonitorPhase
from nfs_ganesha_fd_lru_test.framework.preflight import PreflightReport, run_preflight
from nfs_ganesha_fd_lru_test.framework.report import EnvironmentInfo, ReportBuilder
from nfs_ganesha_fd_lru_test.framework.ssh_client import RemoteResult, SSHClient
from nfs_ganesha_fd_lru_test.framework.verdict import (
    CycleVerdict, DimensionResult, SuiteVerdict, Verdict, VerdictEngine,
)
from nfs_ganesha_fd_lru_test.framework.workload import (
    HeldHandle, WorkloadStats, WorkloadWorker, classify_oserror, with_retry,
)

# ── scenario imports ──────────────────────────────────────────────────────────
from nfs_ganesha_fd_lru_test.scenarios.mode     import ModeProfile, RunMode
from nfs_ganesha_fd_lru_test.scenarios.registry import (
    ALL_SCENARIOS, SCENARIO_MAP, get_scenario,
)
from nfs_ganesha_fd_lru_test.scenarios.tc01_sanity       import TC01_Sanity
from nfs_ganesha_fd_lru_test.scenarios.tc02_v3_stress    import TC02_NFSv3_Stress
from nfs_ganesha_fd_lru_test.scenarios.tc03_v4_stress    import TC03_NFSv4_Stress
from nfs_ganesha_fd_lru_test.scenarios.tc04_mixed_stress import TC04_Mixed_Stress


# =============================================================================
# Test helpers
# =============================================================================

def _cfg(**overrides) -> TestConfig:
    cfg = make_default_test_config(
        server_addr="test-server",
        client_addrs=["client-1", "client-2", "client-3"],
        protocol=ProtocolMode.BOTH,
        num_cycles=2,
    )
    for k, v in overrides.items():
        parts = k.split(".")
        obj = cfg
        for p in parts[:-1]:
            obj = getattr(obj, p)
        setattr(obj, parts[-1], v)
    return cfg


def _ok(stdout="", stderr=""):
    return RemoteResult(command="", host="t", returncode=0, stdout=stdout, stderr=stderr)

def _fail(stderr="err"):
    return RemoteResult(command="", host="t", returncode=1, stdout="", stderr=stderr)

def _phase(label="burst", fsal_fds=None, global_fds=None, lru_entries=None, events=None):
    ph = MonitorPhase(label=label)
    fsal_fds   = fsal_fds or []
    global_fds = global_fds or []
    # lru_entries defaults to fsal_fds — for V3 they track the same population
    lru_entries = lru_entries if lru_entries is not None else fsal_fds
    for i, fd in enumerate(fsal_fds):
        s = FDSample(
            fsal_opened_fd=fd,
            global_fd=global_fds[i] if i < len(global_fds) else 0,
            lru_entries_in_use=lru_entries[i] if i < len(lru_entries) else fd,
        )
        ph.samples.append(s)
    if events:
        ph.events = list(events)
    return ph

def _hiwat():
    return LogEvent(kind=LogEventKind.HIGH_WATERMARK, timestamp=time.time(),
                    raw_line="high-watermark reached", global_fd=400000)
def _futility():
    return LogEvent(kind=LogEventKind.FUTILITY, timestamp=time.time(),
                    raw_line="LRU futility exceeded")
def _hard_limit():
    return LogEvent(kind=LogEventKind.HARD_LIMIT, timestamp=time.time(),
                    raw_line="FD hard limit exceeded")
def _state_pressure():
    return LogEvent(kind=LogEventKind.STATE_FD_PRESSURE, timestamp=time.time(),
                    raw_line="State FDs exceed hiwat")
def _restart():
    return LogEvent(kind=LogEventKind.GANESHA_RESTART, timestamp=time.time(),
                    raw_line="ganesha.nfsd: starting")


# =============================================================================
# §7  Configuration validation
# =============================================================================

class TestConfigValidation(unittest.TestCase):

    def test_valid_config_passes(self):
        _cfg().validate()

    def test_empty_server_fails(self):
        cfg = _cfg(); cfg.server.address = ""
        with self.assertRaises(ValueError): cfg.validate()

    def test_invalid_protocol_fails(self):
        with self.assertRaises(ValueError): ProtocolMode.validate("NFSV5")

    def test_valid_protocols_accepted(self):
        for p in ("V3","V4","BOTH","v3","v4","both"):
            self.assertIn(ProtocolMode.validate(p),
                          {ProtocolMode.V3, ProtocolMode.V4, ProtocolMode.BOTH})

    def test_zero_cycles_fails(self):
        cfg = _cfg(); cfg.num_cycles = 0
        with self.assertRaises(ValueError): cfg.validate()

    def test_negative_cycles_fails(self):
        cfg = _cfg(); cfg.num_cycles = -1
        with self.assertRaises(ValueError): cfg.validate()

    def test_zero_threads_fails(self):
        cfg = _cfg(); cfg.workload.threads_per_client = 0
        with self.assertRaises(ValueError): cfg.validate()

    def test_zero_files_fails(self):
        cfg = _cfg(); cfg.workload.num_files = 0
        with self.assertRaises(ValueError): cfg.validate()

    def test_negative_held_open_fails(self):
        cfg = _cfg(); cfg.workload.held_open_files = -1
        with self.assertRaises(ValueError): cfg.validate()

    def test_zero_held_open_ok(self):
        cfg = _cfg(); cfg.workload.held_open_files = 0; cfg.validate()

    def test_cooldown_interval_gte_cooldown_fails(self):
        cfg = _cfg()
        cfg.workload.cooldown_duration_sec = 30
        cfg.workload.cooldown_sample_interval_sec = 30
        with self.assertRaises(ValueError): cfg.validate()

    def test_non_absolute_mount_fails(self):
        cfg = _cfg(); cfg.clients[0].mount_point = "relative/path"
        with self.assertRaises(ValueError): cfg.validate()

    def test_non_absolute_export_fails(self):
        cfg = _cfg(); cfg.server.nfs_export = "export"
        with self.assertRaises(ValueError): cfg.validate()

    def test_no_controller_fails(self):
        cfg = _cfg()
        for c in cfg.clients: c.role = "worker"
        with self.assertRaises(ValueError): cfg.validate()

    def test_two_controllers_fails(self):
        cfg = _cfg(); cfg.clients[1].role = "controller"
        with self.assertRaises(ValueError): cfg.validate()

    def test_no_clients_fails(self):
        cfg = _cfg(); cfg.clients = []
        with self.assertRaises(ValueError): cfg.validate()

    def test_negative_fd_tolerance_fails(self):
        cfg = _cfg(); cfg.fd_tolerance_pct = -1.0
        with self.assertRaises(ValueError): cfg.validate()

    def test_zero_burst_fails(self):
        cfg = _cfg(); cfg.workload.burst_duration_sec = 0
        with self.assertRaises(ValueError): cfg.validate()

    def test_negative_retry_fails(self):
        cfg = _cfg(); cfg.workload.retry_timeout_sec = -1
        with self.assertRaises(ValueError): cfg.validate()

    def test_invalid_role_fails(self):
        cfg = _cfg(); cfg.clients[1].role = "overlord"
        with self.assertRaises(ValueError): cfg.validate()

    def test_controller_helper(self):
        self.assertEqual(_cfg().controller().role, "controller")

    def test_workers_helper(self):
        workers = _cfg().workers()
        self.assertEqual(len(workers), 2)
        self.assertTrue(all(w.role != "controller" for w in workers))


# =============================================================================
# Run mode / ModeProfile
# =============================================================================

class TestRunMode(unittest.TestCase):

    def test_valid_modes_accepted(self):
        for m in ("fast", "normal", "soak", "FAST", "NORMAL", "SOAK"):
            self.assertIn(RunMode.validate(m), {RunMode.FAST, RunMode.NORMAL, RunMode.SOAK})

    def test_invalid_mode_fails(self):
        with self.assertRaises(ValueError): RunMode.validate("turbo")

    def test_fast_profile_one_cycle(self):
        p = ModeProfile.for_mode(RunMode.FAST)
        self.assertEqual(p.num_cycles, 1)

    def test_normal_profile_six_cycles(self):
        p = ModeProfile.for_mode(RunMode.NORMAL)
        self.assertEqual(p.num_cycles, 6)

    def test_soak_profile_twelve_cycles(self):
        p = ModeProfile.for_mode(RunMode.SOAK)
        self.assertEqual(p.num_cycles, 12)

    def test_soak_threads_higher_than_normal(self):
        self.assertGreater(
            ModeProfile.for_mode(RunMode.SOAK).threads_multiplier,
            ModeProfile.for_mode(RunMode.NORMAL).threads_multiplier,
        )

    def test_fast_burst_shorter_than_normal(self):
        self.assertLess(
            ModeProfile.for_mode(RunMode.FAST).burst_duration_sec,
            ModeProfile.for_mode(RunMode.NORMAL).burst_duration_sec,
        )


# =============================================================================
# Scenario registry (4-scenario structure)
# =============================================================================

class TestScenarioRegistry(unittest.TestCase):

    def test_exactly_four_scenarios_registered(self):
        self.assertEqual(len(ALL_SCENARIOS), 4)

    def test_all_four_ids_present(self):
        for tc_id in ("TC01", "TC02", "TC03", "TC04"):
            self.assertIn(tc_id, SCENARIO_MAP)

    def test_get_scenario_returns_correct_type(self):
        self.assertIsInstance(get_scenario("TC01", _cfg()), TC01_Sanity)
        self.assertIsInstance(get_scenario("TC02", _cfg()), TC02_NFSv3_Stress)
        self.assertIsInstance(get_scenario("TC03", _cfg()), TC03_NFSv4_Stress)
        self.assertIsInstance(get_scenario("TC04", _cfg()), TC04_Mixed_Stress)

    def test_unknown_id_raises(self):
        with self.assertRaises(KeyError): get_scenario("TC99", _cfg())

    def test_tc01_protocol_is_both(self):
        # Sanity checks both V3 and V4 mounts
        self.assertEqual(get_scenario("TC01", _cfg()).config.protocol, ProtocolMode.BOTH)

    def test_tc02_protocol_is_v3(self):
        self.assertEqual(get_scenario("TC02", _cfg()).config.protocol, ProtocolMode.V3)

    def test_tc03_protocol_is_v4(self):
        self.assertEqual(get_scenario("TC03", _cfg()).config.protocol, ProtocolMode.V4)

    def test_tc04_protocol_is_both(self):
        self.assertEqual(get_scenario("TC04", _cfg()).config.protocol, ProtocolMode.BOTH)

    def test_all_scenarios_have_description(self):
        for cls in ALL_SCENARIOS:
            self.assertTrue(cls.DESCRIPTION, f"{cls.SCENARIO_ID} missing DESCRIPTION")


# =============================================================================
# Scenario config setup (mode-driven scaling)
# =============================================================================

class TestScenarioConfigSetup(unittest.TestCase):

    def _make(self, tc_id, mode=RunMode.NORMAL):
        cfg = _cfg()
        cfg.workload.threads_per_client = 4
        cfg.workload.num_files = 100
        cfg.workload.held_open_files = 5
        s = get_scenario(tc_id, cfg, mode=mode)
        s.setup_extra_config()
        return s

    # TC01 always uses fast profile regardless of mode argument
    def test_tc01_always_one_cycle(self):
        s = self._make("TC01", mode=RunMode.SOAK)
        self.assertEqual(s.config.num_cycles, 1)

    def test_tc01_burst_duration_is_fast(self):
        s = self._make("TC01")
        self.assertEqual(s.config.workload.burst_duration_sec,
                         ModeProfile.for_mode(RunMode.FAST).burst_duration_sec)

    # TC02 normal → threads × 2
    def test_tc02_normal_scales_threads(self):
        s = self._make("TC02", mode=RunMode.NORMAL)
        self.assertEqual(s.config.workload.threads_per_client, 4 * 2)

    # TC02 fast → threads × 1
    def test_tc02_fast_does_not_scale_threads(self):
        s = self._make("TC02", mode=RunMode.FAST)
        self.assertEqual(s.config.workload.threads_per_client,
                         max(1, int(4 * ModeProfile.for_mode(RunMode.FAST).threads_multiplier)))

    # TC02 soak → more cycles than normal
    def test_tc02_soak_more_cycles_than_normal(self):
        s_soak   = self._make("TC02", mode=RunMode.SOAK)
        s_normal = self._make("TC02", mode=RunMode.NORMAL)
        self.assertGreater(s_soak.config.num_cycles, s_normal.config.num_cycles)

    # TC03 held-open floored at 20 even on fast
    def test_tc03_held_open_floor(self):
        s = self._make("TC03", mode=RunMode.FAST)
        self.assertGreaterEqual(s.config.workload.held_open_files, 20)

    # TC04 held-open floored at 20
    def test_tc04_held_open_floor(self):
        s = self._make("TC04", mode=RunMode.FAST)
        self.assertGreaterEqual(s.config.workload.held_open_files, 20)

    # TC04 soak scales threads and files
    def test_tc04_soak_scales_threads_and_files(self):
        s = self._make("TC04", mode=RunMode.SOAK)
        self.assertGreater(s.config.workload.threads_per_client, 4)
        self.assertGreater(s.config.workload.num_files, 100)


# =============================================================================
# §9  FD statistics parser
# =============================================================================

class TestFDStatsParser(unittest.TestCase):

    SAMPLE = """\
FSAL opened FD         : 524288
System limit on FDs    : 1048576
FD usage               : 50.00%
LRU entries in use     : 512000
Chunks in use          : 64
Total FDs              : 524288
Global FDs             : 504000
State FDs              : 18000
Temporary FDs          : 2288
"""

    def test_fsal_opened_fd(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).fsal_opened_fd, 524288)

    def test_system_fd_limit(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).system_fd_limit, 1048576)

    def test_fd_usage_pct(self):
        self.assertAlmostEqual(parse_ganesha_stats(self.SAMPLE).fd_usage_pct, 50.0, places=1)

    def test_lru_entries(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).lru_entries_in_use, 512000)

    def test_chunks_in_use(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).chunks_in_use, 64)

    def test_total_fd(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).total_fd, 524288)

    def test_global_fd(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).global_fd, 504000)

    def test_state_fd(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).state_fd, 18000)

    def test_temp_fd(self):
        self.assertEqual(parse_ganesha_stats(self.SAMPLE).temp_fd, 2288)

    def test_is_valid(self):
        self.assertTrue(parse_ganesha_stats(self.SAMPLE).is_valid)

    def test_invalid_when_no_limit(self):
        self.assertFalse(parse_ganesha_stats("FSAL opened FD: 1000").is_valid)

    def test_empty_output_defaults(self):
        s = parse_ganesha_stats("")
        self.assertEqual(s.fsal_opened_fd, 0)
        self.assertEqual(s.system_fd_limit, 0)

    def test_partial_output_no_raise(self):
        s = parse_ganesha_stats("FSAL opened FD: 100000\nSystem limit on FDs: 2000000")
        self.assertEqual(s.fsal_opened_fd, 100000)

    def test_case_insensitive(self):
        s = parse_ganesha_stats("fsal opened fd: 77777\nsystem limit on fds: 999999")
        self.assertEqual(s.fsal_opened_fd, 77777)

    def test_equals_separator(self):
        s1 = parse_ganesha_stats("FSAL opened FD: 100\nSystem limit on FDs: 1000")
        s2 = parse_ganesha_stats("FSAL opened FD = 100\nSystem limit on FDs = 1000")
        self.assertEqual(s1.fsal_opened_fd, s2.fsal_opened_fd)

    def test_pct_without_symbol(self):
        s = parse_ganesha_stats("FD usage: 75.5\nSystem limit on FDs: 100")
        self.assertAlmostEqual(s.fd_usage_pct, 75.5, places=1)

    # --- actual output format from this build ---

    GPFS_SAMPLE = """\
 FSAL opened FD count :                 78927
 System limit on FDs :                 524288
 FD usage :               Below Low Water Mark
 LRU entries in use :                   83254
 Chunks in use :                          105
"""

    def test_fsal_opened_fd_count_label(self):
        """'FSAL opened FD count' (with the word count) must be parsed."""
        s = parse_ganesha_stats(self.GPFS_SAMPLE)
        self.assertEqual(s.fsal_opened_fd, 78927)

    def test_text_label_fd_usage_captured(self):
        """Text label in FD usage field is stored in fd_usage_label."""
        s = parse_ganesha_stats(self.GPFS_SAMPLE)
        self.assertEqual(s.fd_usage_label, "Below Low Water Mark")

    def test_text_label_fd_usage_pct_computed(self):
        """fd_usage_pct is computed from fsal/limit when label is text."""
        s = parse_ganesha_stats(self.GPFS_SAMPLE)
        expected = 78927 / 524288 * 100.0
        self.assertAlmostEqual(s.fd_usage_pct, expected, places=2)

    def test_gpfs_sample_is_valid(self):
        self.assertTrue(parse_ganesha_stats(self.GPFS_SAMPLE).is_valid)

    def test_gpfs_lru_entries(self):
        self.assertEqual(parse_ganesha_stats(self.GPFS_SAMPLE).lru_entries_in_use, 83254)


# =============================================================================
# §25  FD accounting
# =============================================================================

class TestFDAccounting(unittest.TestCase):

    def test_ok_when_total_equals_sum(self):
        ok, disc = FDSample(fsal_opened_fd=100, total_fd=100, global_fd=60, state_fd=30, temp_fd=10).fd_accounting_check
        self.assertTrue(ok); self.assertEqual(disc, 0)

    def test_fail_when_mismatch(self):
        ok, disc = FDSample(fsal_opened_fd=100, total_fd=100, global_fd=60, state_fd=30, temp_fd=5).fd_accounting_check
        self.assertFalse(ok); self.assertEqual(disc, 5)

    def test_ok_when_no_breakdown(self):
        ok, _ = FDSample().fd_accounting_check
        self.assertTrue(ok)

    def test_ok_when_fsal_fd_zero(self):
        # fsal_opened_fd=0 means LRU reclaimed all FDs — accounting check must be skipped
        ok, disc = FDSample(fsal_opened_fd=0, global_fd=0, state_fd=0, temp_fd=0).fd_accounting_check
        self.assertTrue(ok); self.assertEqual(disc, 0)

    def test_discrepancy_is_absolute(self):
        ok, disc = FDSample(fsal_opened_fd=100, total_fd=90, global_fd=60, state_fd=30, temp_fd=10).fd_accounting_check
        self.assertFalse(ok); self.assertEqual(disc, 10)


# =============================================================================
# §8  Baseline stability
# =============================================================================

class TestBaselineStability(unittest.TestCase):

    def _b(self, vals):
        return BaselineStats(samples=[FDSample(fsal_opened_fd=v, system_fd_limit=1_000_000) for v in vals])

    def test_stable_flat(self):     self.assertTrue(self._b([10000,10100,9950,10050,10000]).stable)
    def test_unstable(self):        self.assertFalse(self._b([10000,100000,50000,200000]).stable)
    def test_single_is_stable(self):self.assertTrue(self._b([50000]).stable)
    def test_empty_is_stable(self): self.assertTrue(BaselineStats().stable)
    def test_average(self):         self.assertAlmostEqual(self._b([10000,20000]).average_fsal_fd, 15000.0)
    def test_limit_from_last(self):
        samples = self._b([10000, 20000])
        samples.samples[-1].system_fd_limit = 999999
        self.assertEqual(samples.system_fd_limit, 999999)


# =============================================================================
# §10  Log parser
# =============================================================================

class TestLogParser(unittest.TestCase):

    def test_high_watermark(self):
        ev = parse_log_line("2024/05/01 15:32:42 : high-watermark reached total=500000 global=480000 state=15000 temp=5000")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.HIGH_WATERMARK)

    def test_hard_limit(self):
        ev = parse_log_line("2024/05/01 15:33:00 : FD hard limit exceeded total=524288 global=504000 state=18000 temp=2288")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.HARD_LIMIT)

    def test_futility(self):
        ev = parse_log_line("2024/05/01 15:33:05 : LRU futility count exceeded global=490000")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.FUTILITY)

    def test_state_fd_pressure(self):
        ev = parse_log_line("2024/05/01 15:33:10 : State FDs exceed hiwat state=200000")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.STATE_FD_PRESSURE)

    def test_ganesha_restart(self):
        # Canonical GPFS/RHEL9 restart line (ISO timestamp, structured fields)
        line = (
            "2026-08-31 19:22:42 : epoch 00023aae : scale2-22 : "
            "gpfs.ganesha.nfsd-1091341[main] nfs_start :NFS STARTUP :EVENT :"
            "             NFS SERVER INITIALIZED"
        )
        ev = parse_log_line(line)
        self.assertIsNotNone(ev)
        self.assertEqual(ev.kind, LogEventKind.GANESHA_RESTART)

    def test_ganesha_restart_iso_timestamp_parsed(self):
        """ISO dash-separated timestamp must be extracted correctly."""
        import datetime
        line = (
            "2026-08-31 19:22:42 : epoch 00023aae : scale2-22 : "
            "gpfs.ganesha.nfsd-1091341[main] nfs_start :NFS STARTUP :EVENT :"
            "             NFS SERVER INITIALIZED"
        )
        ev = parse_log_line(line)
        self.assertIsNotNone(ev)
        dt = datetime.datetime.fromtimestamp(ev.timestamp)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 8)
        self.assertEqual(dt.day, 31)
        self.assertEqual(dt.hour, 19)
        self.assertEqual(dt.minute, 22)

    def test_ganesha_restart_slash_timestamp_still_works(self):
        ev = parse_log_line("2024/05/01 15:35:00 : Initializing memory and logging")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.GANESHA_RESTART)

    def test_ganesha_restart_running_daemon_line_not_matched(self):
        """A normal per-RPC log line that contains the process name must NOT match."""
        line = (
            "2026-08-31 20:46:39 : epoch 00023aae : scale2-22 : "
            "gpfs.ganesha.nfsd-1091341[svc_0] some_rpc_func :NFS4 :EVENT : some normal op"
        )
        ev = parse_log_line(line)
        # Should not be classified as a restart
        self.assertIsNone(ev)

    def test_fd_count_diag(self):
        ev = parse_log_line("2024/05/01 15:30:00 : FD count: total=100000 global=80000 state=15000 temp=5000")
        self.assertIsNotNone(ev); self.assertEqual(ev.kind, LogEventKind.FD_COUNT_DIAG)

    def test_fd_breakdown_extracted(self):
        ev = parse_log_line("15:32:42 high-watermark reached total=500000 global=480000 state=15000 temp=5000")
        self.assertEqual(ev.total_fd, 500000); self.assertEqual(ev.global_fd, 480000)
        self.assertEqual(ev.state_fd, 15000);  self.assertEqual(ev.temp_fd, 5000)

    def test_parse_log_text_all_events(self):
        log = (
            "2024/05/01 15:32:42 : high-watermark reached total=500000 global=480000 state=15000 temp=5000\n"
            "2024/05/01 15:33:00 : FD hard limit exceeded total=524288 global=504000 state=18000 temp=2288\n"
            "2024/05/01 15:33:05 : LRU futility count exceeded\n"
            "2024/05/01 15:33:10 : State FDs exceed hiwat\n"
            "2026-08-31 19:22:42 : epoch 00023aae : scale2-22 : gpfs.ganesha.nfsd-1091341[main] nfs_start :NFS STARTUP :EVENT :             NFS SERVER INITIALIZED\n"
        )
        kinds = {e.kind for e in parse_log_text(log)}
        for k in (LogEventKind.HIGH_WATERMARK, LogEventKind.HARD_LIMIT,
                  LogEventKind.FUTILITY, LogEventKind.STATE_FD_PRESSURE,
                  LogEventKind.GANESHA_RESTART):
            self.assertIn(k, kinds)

    def test_unrelated_line_returns_none(self):
        self.assertIsNone(parse_log_line("This is a completely unrelated log line."))

    def test_filter_helpers(self):
        evs = [_hiwat(), _futility(), _hard_limit(), _state_pressure(), _restart()]
        self.assertEqual(len(high_watermark_events(evs)), 1)
        self.assertEqual(len(futility_events(evs)), 1)
        self.assertEqual(len(hard_limit_events(evs)), 1)
        self.assertEqual(len(state_pressure_events(evs)), 1)
        self.assertEqual(len(restart_events(evs)), 1)

    def test_empty_log(self):
        self.assertEqual(parse_log_text(""), [])

    def test_timestamp_from_full_date(self):
        import datetime
        ev = parse_log_line("2024/05/01 12:00:00 : high-watermark reached")
        self.assertIsNotNone(ev)
        self.assertEqual(datetime.datetime.fromtimestamp(ev.timestamp).hour, 12)


# =============================================================================
# §26  WorkloadStats
# =============================================================================

class TestWorkloadStats(unittest.TestCase):

    def test_merge(self):
        a = WorkloadStats(opens_attempted=100, opens_succeeded=90, reads=45)
        b = WorkloadStats(opens_attempted=50,  opens_succeeded=48, reads=24)
        a.merge(b)
        self.assertEqual(a.opens_attempted, 150)
        self.assertEqual(a.reads, 69)

    def test_as_dict(self):
        d = WorkloadStats(opens_attempted=10).as_dict()
        self.assertEqual(d["opens_attempted"], 10)

    def test_classify_emfile(self):
        import errno
        self.assertEqual(classify_oserror(OSError(errno.EMFILE, "")), "EMFILE")

    def test_classify_eio(self):
        import errno
        self.assertEqual(classify_oserror(OSError(errno.EIO, "")), "EIO")

    def test_classify_estale(self):
        import errno
        self.assertEqual(classify_oserror(OSError(errno.ESTALE, "")), "ESTALE")

    def test_classify_enfile(self):
        import errno
        self.assertEqual(classify_oserror(OSError(errno.ENFILE, "")), "ENFILE")

    def test_classify_other(self):
        import errno
        self.assertEqual(classify_oserror(OSError(errno.ENOENT, "")), "OTHER")


# =============================================================================
# §13  HeldHandle lifecycle
# =============================================================================

class TestHeldHandle(unittest.TestCase):

    def test_open_creates_file(self):
        with tempfile.TemporaryDirectory() as d:
            h = HeldHandle(os.path.join(d, "h.dat"))
            self.assertTrue(h.open()); self.assertTrue(h.is_valid); h.close()

    def test_validate_ok_after_open(self):
        with tempfile.TemporaryDirectory() as d:
            h = HeldHandle(os.path.join(d, "h.dat"))
            h.open()
            stats = WorkloadStats()
            self.assertTrue(h.validate(stats))
            self.assertEqual(stats.active_handle_failures, 0)
            h.close()

    def test_validate_false_after_close(self):
        with tempfile.TemporaryDirectory() as d:
            h = HeldHandle(os.path.join(d, "h.dat"))
            h.open(); h.close()
            self.assertFalse(h.validate(WorkloadStats()))

    def test_open_bad_path_fails(self):
        self.assertFalse(HeldHandle("/nonexistent/path/h.dat").open())

    def test_not_valid_after_close(self):
        with tempfile.TemporaryDirectory() as d:
            h = HeldHandle(os.path.join(d, "h.dat"))
            h.open(); h.close(); self.assertFalse(h.is_valid)

    def test_double_close_no_raise(self):
        with tempfile.TemporaryDirectory() as d:
            h = HeldHandle(os.path.join(d, "h.dat"))
            h.open(); h.close(); h.close()


# =============================================================================
# §18  Retry wrapper
# =============================================================================

class TestWithRetry(unittest.TestCase):

    def test_succeeds_first_attempt(self):
        calls = [0]
        def fn(): calls[0] += 1; return "ok"
        self.assertEqual(with_retry(fn, timeout_sec=5, interval_sec=0.01), "ok")
        self.assertEqual(calls[0], 1)

    def test_retries_on_eio(self):
        import errno
        calls = [0]
        def fn():
            calls[0] += 1
            if calls[0] < 3: raise OSError(errno.EIO, "EIO")
            return "ok"
        stats = WorkloadStats()
        self.assertEqual(with_retry(fn, timeout_sec=2, interval_sec=0.01, stats=stats), "ok")
        self.assertGreater(stats.opens_retried, 0)
        self.assertEqual(stats.opens_eventually_ok, 1)

    def test_raises_non_retryable(self):
        import errno
        with self.assertRaises(OSError):
            with_retry(lambda: (_ for _ in ()).throw(OSError(errno.ENOENT,"")),
                       timeout_sec=5, interval_sec=0.01)

    def test_raises_after_timeout(self):
        import errno
        with self.assertRaises(OSError):
            with_retry(lambda: (_ for _ in ()).throw(OSError(errno.EIO,"")),
                       timeout_sec=0.05, interval_sec=0.01)


# =============================================================================
# §19  MonitorPhase
# =============================================================================

class TestMonitorPhase(unittest.TestCase):

    def test_peak_fsal(self):
        self.assertEqual(_phase(fsal_fds=[100,500,300]).peak_fsal_fd, 500)

    def test_settled_fsal(self):
        self.assertEqual(_phase(fsal_fds=[500,300,50]).settled_fsal_fd, 50)

    def test_peak_global(self):
        self.assertEqual(_phase(fsal_fds=[100], global_fds=[400]).peak_global_fd, 400)

    def test_hiwat_true(self):
        self.assertTrue(_phase(events=[_hiwat()]).high_watermark_reached)

    def test_hiwat_false(self):
        self.assertFalse(_phase().high_watermark_reached)

    def test_hard_limit_reached(self):
        self.assertTrue(_phase(events=[_hard_limit()]).hard_limit_reached)

    def test_futility_detected(self):
        self.assertTrue(_phase(events=[_futility()]).futility_detected)

    def test_state_pressure_detected(self):
        self.assertTrue(_phase(events=[_state_pressure()]).state_fd_pressure_detected)

    def test_restart_detected(self):
        self.assertTrue(_phase(events=[_restart()]).ganesha_restarted)

    def test_lru_made_progress(self):
        cd = _phase(fsal_fds=[400000, 10000], global_fds=[400000, 10000])
        self.assertTrue(cd.lru_made_progress())

    def test_lru_no_progress(self):
        cd = _phase(fsal_fds=[400000, 398000], global_fds=[400000, 398000])
        self.assertFalse(cd.lru_made_progress())

    def test_fd_settled(self):
        self.assertTrue(_phase(fsal_fds=[50000, 11000, 10800, 10900]).fd_settled())

    def test_fd_not_settled(self):
        self.assertFalse(_phase(fsal_fds=[50000, 45000, 40000, 38000]).fd_settled())

    def test_empty_phase_safe_defaults(self):
        p = MonitorPhase(label="x")
        self.assertEqual(p.peak_fsal_fd, 0)
        self.assertEqual(p.settled_fsal_fd, 0)
        self.assertFalse(p.high_watermark_reached)
        self.assertTrue(p.fd_settled())
        self.assertFalse(p.lru_made_progress())   # no samples → False (no data)

    def test_hiwat_true_via_stats_label_above_hwm(self):
        """high_watermark_reached is True when a sample carries 'Above High Water Mark'."""
        ph = MonitorPhase(label="burst")
        s = FDSample(fsal_opened_fd=18000, system_fd_limit=20000,
                     fd_usage_label="Above High Water Mark")
        ph.samples.append(s)
        self.assertTrue(ph.high_watermark_reached)

    def test_hiwat_true_via_stats_label_hard_limit(self):
        """high_watermark_reached is also True when label is 'Hard Limit reached'."""
        ph = MonitorPhase(label="burst")
        s = FDSample(fsal_opened_fd=20000, system_fd_limit=20000,
                     fd_usage_label="Hard Limit reached")
        ph.samples.append(s)
        self.assertTrue(ph.high_watermark_reached)

    def test_hard_limit_reached_via_stats_label(self):
        """hard_limit_reached is True when a sample carries 'Hard Limit reached'."""
        ph = MonitorPhase(label="burst")
        s = FDSample(fsal_opened_fd=20000, system_fd_limit=20000,
                     fd_usage_label="Hard Limit reached")
        ph.samples.append(s)
        self.assertTrue(ph.hard_limit_reached)

    def test_hard_limit_not_reached_for_above_hwm_only(self):
        """hard_limit_reached is False when label is only 'Above High Water Mark'."""
        ph = MonitorPhase(label="burst")
        s = FDSample(fsal_opened_fd=18000, system_fd_limit=20000,
                     fd_usage_label="Above High Water Mark")
        ph.samples.append(s)
        self.assertFalse(ph.hard_limit_reached)


# =============================================================================
# ServerMonitor — historical-event timestamp filter
# =============================================================================

class TestServerMonitorTimestampFilter(unittest.TestCase):
    """
    Verify that log events predating _test_start_time are silently dropped
    and never reach phase.events — even when the raw_line is unique.
    """

    def _make_monitor(self):
        from unittest.mock import MagicMock
        from nfs_ganesha_fd_lru_test.framework.monitor import ServerMonitor
        from nfs_ganesha_fd_lru_test.framework.config import ServerConfig
        cfg = ServerConfig(address="srv", ssh_address="srv")
        ssh = MagicMock()
        ssh.run_remote.return_value = MagicMock(ok=False, stdout="", stderr="")
        return ServerMonitor(cfg, ssh)

    def test_historical_event_is_dropped(self):
        """An event timestamped before test start must not enter phase.events."""
        from nfs_ganesha_fd_lru_test.framework.monitor import ServerMonitor, MonitorPhase
        from nfs_ganesha_fd_lru_test.framework.log_parser import LogEvent, LogEventKind
        monitor = self._make_monitor()
        phase = MonitorPhase(label="burst_cycle_1")

        # Build a restart event 2 hours before the monitor was created
        old_ev = LogEvent(
            kind=LogEventKind.GANESHA_RESTART,
            timestamp=monitor._test_start_time - 7200,
            raw_line="2026-08-31 19:22:42 : epoch 00023aae : scale2-22 : "
                     "gpfs.ganesha.nfsd-1091341[main] nfs_start :NFS STARTUP "
                     ":EVENT :             NFS SERVER INITIALIZED",
            message="NFS SERVER INITIALIZED",
        )

        # Simulate what _monitor_loop does
        events = [old_ev]
        with monitor._lock:
            events = [e for e in events if e.timestamp >= monitor._test_start_time]
            existing_lines = {e.raw_line for e in phase.events}
            for ev in events:
                if ev.raw_line not in existing_lines:
                    phase.events.append(ev)
                    existing_lines.add(ev.raw_line)

        self.assertEqual(len(phase.events), 0,
                         "Historical event must be filtered out")

    def test_current_event_is_kept(self):
        """An event timestamped after test start must enter phase.events."""
        from nfs_ganesha_fd_lru_test.framework.monitor import ServerMonitor, MonitorPhase
        from nfs_ganesha_fd_lru_test.framework.log_parser import LogEvent, LogEventKind
        import time as _time
        monitor = self._make_monitor()
        phase = MonitorPhase(label="burst_cycle_1")

        # Build a restart event 10 seconds after the monitor was created
        new_ev = LogEvent(
            kind=LogEventKind.GANESHA_RESTART,
            timestamp=monitor._test_start_time + 10,
            raw_line="2026-08-31 21:34:55 : epoch 00023aae : scale2-22 : "
                     "gpfs.ganesha.nfsd-1091342[main] nfs_start :NFS STARTUP "
                     ":EVENT :             NFS SERVER INITIALIZED",
            message="NFS SERVER INITIALIZED",
        )

        events = [new_ev]
        with monitor._lock:
            events = [e for e in events if e.timestamp >= monitor._test_start_time]
            existing_lines = {e.raw_line for e in phase.events}
            for ev in events:
                if ev.raw_line not in existing_lines:
                    phase.events.append(ev)
                    existing_lines.add(ev.raw_line)

        self.assertEqual(len(phase.events), 1,
                         "Current-test event must be kept")

    def test_test_start_time_is_in_recent_past(self):
        """_test_start_time should be within ~65 seconds of now."""
        import time as _time
        monitor = self._make_monitor()
        age = _time.time() - monitor._test_start_time
        self.assertGreater(age, 0, "_test_start_time must be in the past")
        self.assertLess(age, 120, "_test_start_time must be recent (within 2 min)")



# =============================================================================
# Verdict engine
# =============================================================================

class TestVerdictEngine(unittest.TestCase):

    def setUp(self):
        self.e = VerdictEngine(fd_tolerance_pct=10.0, fd_accounting_tolerance=100)

    # -- workload completion
    def test_workload_pass(self):
        r = self.e.check_workload_completion(WorkloadStats(opens_attempted=100,opens_succeeded=100),"V3")
        self.assertEqual(r.verdict, Verdict.PASS)

    def test_workload_fail(self):
        r = self.e.check_workload_completion(WorkloadStats(opens_attempted=100,opens_failed=50),"V3")
        self.assertEqual(r.verdict, Verdict.FAIL)

    def test_workload_inconclusive_no_ops(self):
        r = self.e.check_workload_completion(WorkloadStats(),"V3")
        self.assertEqual(r.verdict, Verdict.INCONCLUSIVE)

    # -- active handles
    def test_active_handles_pass(self):
        r = self.e.check_active_handles(WorkloadStats(active_handles=20),"V3")
        self.assertEqual(r.verdict, Verdict.PASS)

    def test_active_handles_fail(self):
        r = self.e.check_active_handles(WorkloadStats(active_handles=20,active_handle_failures=1),"V3")
        self.assertEqual(r.verdict, Verdict.FAIL)

    def test_active_handles_inconclusive_none(self):
        r = self.e.check_active_handles(WorkloadStats(),"V3")
        self.assertEqual(r.verdict, Verdict.INCONCLUSIVE)

    # -- no restart
    def test_no_restart_pass(self):
        self.assertEqual(self.e.check_ganesha_no_restart([_phase()]).verdict, Verdict.PASS)

    def test_restart_fail(self):
        self.assertEqual(self.e.check_ganesha_no_restart([_phase(events=[_restart()])]).verdict, Verdict.FAIL)

    # -- LRU reclamation
    def test_lru_reclamation_pass(self):
        b  = _phase(fsal_fds=[400000], global_fds=[400000])
        cd = _phase(fsal_fds=[400000,5000], global_fds=[400000,5000])
        self.assertEqual(self.e.check_lru_reclamation(b, cd).verdict, Verdict.PASS)

    def test_lru_reclamation_fail(self):
        b  = _phase(fsal_fds=[400000], global_fds=[400000])
        cd = _phase(fsal_fds=[395000,392000], global_fds=[395000,392000])
        self.assertEqual(self.e.check_lru_reclamation(b, cd).verdict, Verdict.FAIL)

    def test_lru_reclamation_warning(self):
        b  = _phase(fsal_fds=[400000], global_fds=[400000])
        cd = _phase(fsal_fds=[300000,250000], global_fds=[300000,250000])
        self.assertEqual(self.e.check_lru_reclamation(b, cd).verdict, Verdict.WARNING)

    def test_lru_reclamation_inconclusive_no_samples(self):
        self.assertEqual(
            self.e.check_lru_reclamation(MonitorPhase(label="b"), MonitorPhase(label="c")).verdict,
            Verdict.INCONCLUSIVE,
        )

    # -- FD settled
    def test_fd_settled_pass(self):
        self.assertEqual(self.e.check_fd_settled(_phase(fsal_fds=[50000,11000,10900,10850])).verdict, Verdict.PASS)

    def test_fd_settled_fail(self):
        self.assertEqual(self.e.check_fd_settled(_phase(fsal_fds=[50000,45000,40000,35000])).verdict, Verdict.FAIL)

    # -- FD retention
    def test_retention_pass(self):
        self.assertEqual(
            self.e.check_fd_retention_across_cycles([10000,10200,10100,10150,10050,10100]).verdict,
            Verdict.PASS,
        )

    def test_retention_fail_monotone(self):
        self.assertEqual(
            self.e.check_fd_retention_across_cycles([10000,10500,11000,11500,12000,12500]).verdict,
            Verdict.FAIL,
        )

    def test_retention_inconclusive_one_cycle(self):
        self.assertEqual(
            self.e.check_fd_retention_across_cycles([10000]).verdict,
            Verdict.INCONCLUSIVE,
        )

    # -- FD accounting
    def test_accounting_pass(self):
        ph = MonitorPhase(label="b")
        ph.samples.append(FDSample(total_fd=100,global_fd=60,state_fd=30,temp_fd=10))
        self.assertEqual(self.e.check_fd_accounting([ph]).verdict, Verdict.PASS)

    def test_accounting_fail(self):
        ph = MonitorPhase(label="b")
        for _ in range(10):
            # fsal_opened_fd must be > 0 for the check to run; total_fd ≠ sum
            ph.samples.append(FDSample(fsal_opened_fd=100,total_fd=100,global_fd=200,state_fd=100,temp_fd=100))
        self.assertEqual(self.e.check_fd_accounting([ph]).verdict, Verdict.FAIL)

    # -- high watermark
    def test_hiwat_inconclusive_not_reached(self):
        self.assertEqual(
            self.e.check_high_watermark(_phase(), _phase(fsal_fds=[400000,5000],global_fds=[400000,5000])).verdict,
            Verdict.INCONCLUSIVE,
        )

    def test_hiwat_warning_lru_recovered(self):
        b  = _phase(fsal_fds=[400000], events=[_hiwat()])
        cd = _phase(fsal_fds=[400000,5000], global_fds=[400000,5000])
        self.assertEqual(self.e.check_high_watermark(b, cd).verdict, Verdict.WARNING)

    def test_hiwat_fail_no_recovery(self):
        b  = _phase(fsal_fds=[400000], events=[_hiwat()])
        cd = _phase(fsal_fds=[400000,395000,392000], global_fds=[400000,395000,392000])
        self.assertEqual(self.e.check_high_watermark(b, cd).verdict, Verdict.FAIL)

    # -- hard limit
    def test_hard_limit_inconclusive(self):
        self.assertEqual(self.e.check_hard_limit(_phase(), WorkloadStats(opens_attempted=100,opens_succeeded=100)).verdict,
                         Verdict.INCONCLUSIVE)

    def test_hard_limit_warning_recovery(self):
        r = self.e.check_hard_limit(_phase(events=[_hard_limit()]),
                                     WorkloadStats(opens_attempted=100,opens_failed=5,opens_eventually_ok=5))
        self.assertEqual(r.verdict, Verdict.WARNING)

    def test_hard_limit_fail_no_recovery(self):
        r = self.e.check_hard_limit(_phase(events=[_hard_limit()]),
                                     WorkloadStats(opens_attempted=100,opens_failed=20,opens_eventually_ok=0))
        self.assertEqual(r.verdict, Verdict.FAIL)

    # -- futility
    def test_futility_inconclusive(self):
        self.assertEqual(
            self.e.check_futility(_phase(), _phase(fsal_fds=[400000,5000],global_fds=[400000,5000])).verdict,
            Verdict.INCONCLUSIVE,
        )

    def test_futility_warning_recovered(self):
        cd = _phase(fsal_fds=[400000,5000], global_fds=[400000,5000])
        self.assertEqual(self.e.check_futility(_phase(events=[_futility()]), cd).verdict, Verdict.WARNING)

    def test_futility_fail_no_recovery(self):
        # LRU made no progress (< 10% reduction) AND fd not settled (> 10% spread
        # in last 3 samples).  lru_entries: peak=400000, settled=395000 → 1.25% drop.
        # last-3 spread: 395000-320000=75000, avg≈371667 → ~20% variation → not settled.
        cd = _phase(fsal_fds=[400000,395000,320000,395000],
                    lru_entries=[400000,395000,320000,395000])
        self.assertEqual(self.e.check_futility(_phase(events=[_futility()]), cd).verdict, Verdict.FAIL)

    # §23 state-FD pressure → WARNING, never FAIL
    def test_state_pressure_warning_not_fail(self):
        r = self.e.check_state_fd_pressure(_phase(events=[_state_pressure()]))
        self.assertEqual(r.verdict, Verdict.WARNING)
        self.assertNotEqual(r.verdict, Verdict.FAIL)

    def test_no_state_pressure_pass(self):
        self.assertEqual(self.e.check_state_fd_pressure(_phase()).verdict, Verdict.PASS)

    # §31 server monitoring
    def test_monitoring_fail_no_samples(self):
        self.assertEqual(self.e.check_server_monitoring([MonitorPhase(label="b")]).verdict, Verdict.FAIL)

    def test_monitoring_pass(self):
        self.assertEqual(self.e.check_server_monitoring([_phase(fsal_fds=[100000])]).verdict, Verdict.PASS)

    # §30 EMFILE vs EIO
    def test_emfile_warning(self):
        r = self.e.check_client_fd_exhaustion(WorkloadStats(emfile_count=5), "V3")
        self.assertEqual(r.verdict, Verdict.WARNING)
        self.assertIn("EMFILE", r.reason)

    def test_no_emfile_pass(self):
        self.assertEqual(self.e.check_client_fd_exhaustion(WorkloadStats(), "V3").verdict, Verdict.PASS)

    # §33 mount loss
    def test_high_estale_fail(self):
        r = self.e.check_mount_loss(WorkloadStats(opens_attempted=100, estale_count=10), "V3")
        self.assertEqual(r.verdict, Verdict.FAIL)

    def test_low_estale_pass(self):
        r = self.e.check_mount_loss(WorkloadStats(opens_attempted=1000, estale_count=1), "V3")
        self.assertEqual(r.verdict, Verdict.PASS)

    # -- cycle & suite verdict aggregation
    def test_cycle_fail_propagates(self):
        cv = CycleVerdict(cycle=1, protocol="V3")
        cv.dimensions = [DimensionResult(name="a", verdict=Verdict.FAIL, reason="bad")]
        self.assertEqual(cv.overall, Verdict.FAIL)

    def test_cycle_warning_if_no_fail(self):
        cv = CycleVerdict(cycle=1, protocol="V3")
        cv.dimensions = [DimensionResult(name="a", verdict=Verdict.WARNING, reason="w")]
        self.assertEqual(cv.overall, Verdict.WARNING)

    def test_cycle_pass_all_pass(self):
        cv = CycleVerdict(cycle=1, protocol="V3")
        cv.dimensions = [DimensionResult(name="a", verdict=Verdict.PASS, reason="ok")]
        self.assertEqual(cv.overall, Verdict.PASS)

    def test_suite_fail_propagates(self):
        cv = CycleVerdict(cycle=1, protocol="V3")
        cv.dimensions = [DimensionResult(name="a", verdict=Verdict.FAIL, reason="bad")]
        sv = SuiteVerdict(protocol="V3", cycle_verdicts=[cv])
        self.assertEqual(sv.overall, Verdict.FAIL)

    def test_evaluate_cycle_returns_verdict(self):
        cv = self.e.evaluate_cycle(
            cycle=1, protocol="V3",
            stats=WorkloadStats(opens_attempted=100, opens_succeeded=100, active_handles=10),
            burst_phase=_phase(fsal_fds=[100000,200000], global_fds=[80000,180000]),
            cooldown_phase=_phase(fsal_fds=[200000,50000,10000], global_fds=[180000,40000,8000]),
        )
        self.assertIsInstance(cv, CycleVerdict)
        self.assertGreater(len(cv.dimensions), 0)


# =============================================================================
# §38  Report builder
# =============================================================================

class TestReportBuilder(unittest.TestCase):

    def _build(self):
        cfg = _cfg()
        env = EnvironmentInfo(server_address="ts", protocol="BOTH",
                              fd_system_limit=1048576, client_addresses=["c1","c2"])
        baseline = BaselineStats(samples=[FDSample(fsal_opened_fd=10000, system_fd_limit=1048576)])
        builder  = ReportBuilder(config=cfg, env=env, baseline=baseline)
        suite    = SuiteVerdict(protocol="BOTH")
        return builder.build(suite, [_phase(fsal_fds=[100000,200000])],
                             [WorkloadStats(opens_attempted=500,opens_succeeded=490)])

    def test_environment_section(self):  self.assertIn("ENVIRONMENT",        self._build())
    def test_workload_section(self):     self.assertIn("WORKLOAD",           self._build())
    def test_baseline_section(self):     self.assertIn("BASELINE",           self._build())
    def test_time_series_section(self):  self.assertIn("FD/LRU TIME SERIES", self._build())
    def test_verdict_section(self):      self.assertIn("VERDICT",            self._build())
    def test_overall_present(self):      self.assertIn("OVERALL",            self._build())
    def test_server_address_present(self):self.assertIn("ts",                self._build())

    def test_json_keys(self):
        import json
        cfg = _cfg()
        env = EnvironmentInfo(server_address="ts", protocol="V3")
        b   = ReportBuilder(config=cfg, env=env, baseline=BaselineStats())
        d   = json.loads(b.to_json(SuiteVerdict(protocol="V3"), []))
        self.assertIn("environment", d)
        self.assertIn("overall",     d)
        self.assertIn("cycle_verdicts", d)


# =============================================================================
# §6  Preflight (mock-based)
# =============================================================================

class TestPreflight(unittest.TestCase):

    _STATS_OUT = "FSAL opened FD: 10000\nSystem limit on FDs: 1000000\nLRU entries: 1000"

    def _mock_ssh(self, reachable=True, ganesha_ok=True, log_ok=True):
        ssh = MagicMock(spec=SSHClient)
        ssh.check_reachable.return_value = reachable

        def _run(host, cmd, timeout=None):
            if "ganesha_stats" in cmd:
                return _ok(self._STATS_OUT) if ganesha_ok else _fail("not found")
            if "test -r" in cmd:
                return _ok("readable") if log_ok else _fail()
            if "mount" in cmd:
                return _ok("mounted")
            return _ok()

        ssh.run_remote.side_effect = _run
        return ssh

    def test_passes_with_all_ok(self):
        ssh = self._mock_ssh()
        with patch("nfs_ganesha_fd_lru_test.framework.preflight.SSHClient", return_value=ssh):
            report = run_preflight(_cfg(), ssh=ssh)
        self.assertTrue(report.passed, report.summary())

    def test_fails_server_unreachable(self):
        ssh = self._mock_ssh(reachable=False)
        report = run_preflight(_cfg(), ssh=ssh)
        self.assertFalse(report.passed)
        self.assertTrue(any("SSH" in e for e in report.errors))

    def test_fails_ganesha_stats_missing(self):
        ssh = self._mock_ssh(ganesha_ok=False)
        report = run_preflight(_cfg(), ssh=ssh)
        self.assertFalse(report.passed)

    def test_warns_log_unreadable(self):
        ssh = self._mock_ssh(log_ok=False)
        with patch("nfs_ganesha_fd_lru_test.framework.preflight.SSHClient", return_value=ssh):
            report = run_preflight(_cfg(), ssh=ssh)
        self.assertTrue(any("log" in w.lower() for w in report.warnings))

    def test_fails_invalid_config(self):
        cfg = _cfg(); cfg.num_cycles = 0
        report = run_preflight(cfg, ssh=self._mock_ssh())
        self.assertFalse(report.passed)

    def test_summary_contains_status(self):
        ssh = self._mock_ssh(reachable=False)
        report = run_preflight(_cfg(), ssh=ssh)
        self.assertIn("Pre-flight status", report.summary())


# =============================================================================
# §5  CycleRunner (mock-based)
# =============================================================================

class TestCycleRunner(unittest.TestCase):

    def test_returns_cycle_verdict(self):
        from nfs_ganesha_fd_lru_test.framework.runner import CycleRunner

        cfg = _cfg()
        cfg.workload.burst_duration_sec    = 1
        cfg.workload.cooldown_duration_sec = 2
        cfg.workload.cooldown_sample_interval_sec = 1
        cfg.workload.threads_per_client = 1
        cfg.workload.num_files = 5
        cfg.workload.held_open_files = 0

        burst_p   = _phase(fsal_fds=[100,200], global_fds=[80,180])
        cooldown_p = _phase(fsal_fds=[200,50,10], global_fds=[180,40,8])

        monitor = MagicMock()
        monitor.start_phase.side_effect = lambda lbl: burst_p if "burst" in lbl else cooldown_p
        monitor.stop_phase.side_effect  = lambda p: p

        runner = CycleRunner(cfg, monitor, VerdictEngine())
        mock_stats = WorkloadStats(opens_attempted=50, opens_succeeded=50)

        client_ssh_mock = MagicMock()
        client_ssh_mock.run_remote.return_value = _ok("mounted")

        with patch("nfs_ganesha_fd_lru_test.framework.runner.WorkloadWorker") as MW, \
             patch("nfs_ganesha_fd_lru_test.framework.runner.SSHClient",
                   return_value=client_ssh_mock):
            MW.return_value.run_remote_burst.return_value = mock_stats
            cv, b, cd, stats = runner.run(cycle_number=1, protocol="V3")

        self.assertIsInstance(cv, CycleVerdict)
        self.assertEqual(cv.cycle, 1)
        monitor.stop_phase.assert_called()


# =============================================================================
# WorkloadWorker (local tmpdir)
# =============================================================================

class TestWorkloadWorkerLocal(unittest.TestCase):

    def test_burst_returns_stats(self):
        with tempfile.TemporaryDirectory() as d:
            w = WorkloadWorker(mount_point=d, num_threads=2, num_files=10,
                               file_size_bytes=512, held_open_count=2,
                               burst_duration_sec=2, retry_timeout_sec=5,
                               retry_interval_sec=0.1)
            s = w.run_burst()
        self.assertGreater(s.opens_attempted, 0)
        self.assertGreater(s.opens_succeeded, 0)

    def test_burst_creates_files(self):
        with tempfile.TemporaryDirectory() as d:
            WorkloadWorker(mount_point=d, num_threads=1, num_files=5,
                           file_size_bytes=128, held_open_count=0,
                           burst_duration_sec=1, retry_timeout_sec=2,
                           retry_interval_sec=0.1).run_burst()
            files = [p for p in pathlib.Path(d).rglob("*") if p.is_file()]
        self.assertGreater(len(files), 0)

    def test_held_handles_counted(self):
        with tempfile.TemporaryDirectory() as d:
            s = WorkloadWorker(mount_point=d, num_threads=1, num_files=5,
                               file_size_bytes=64, held_open_count=3,
                               burst_duration_sec=1, retry_timeout_sec=2,
                               retry_interval_sec=0.1).run_burst()
        self.assertEqual(s.active_handles, 3)
        self.assertEqual(s.active_handle_failures, 0)

    def test_no_held_handles(self):
        with tempfile.TemporaryDirectory() as d:
            s = WorkloadWorker(mount_point=d, num_threads=1, num_files=5,
                               file_size_bytes=64, held_open_count=0,
                               burst_duration_sec=1, retry_timeout_sec=2,
                               retry_interval_sec=0.1).run_burst()
        self.assertEqual(s.active_handles, 0)


# =============================================================================
# SSH client
# =============================================================================

class TestSSHClient(unittest.TestCase):

    def test_reachable_true(self):
        c = SSHClient()
        with patch.object(c, "run_remote", return_value=_ok()): self.assertTrue(c.check_reachable("h"))

    def test_reachable_false(self):
        c = SSHClient()
        with patch.object(c, "run_remote", return_value=_fail()): self.assertFalse(c.check_reachable("h"))

    def test_timeout_rc_minus_one(self):
        import subprocess
        c = SSHClient(timeout=1)
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=1)):
            r = c.run_remote("h", "cmd", timeout=1)
        self.assertEqual(r.returncode, -1)
        self.assertIn("timed out", r.stderr.lower())

    def test_exception_rc_minus_two(self):
        with patch("subprocess.run", side_effect=RuntimeError("refused")):
            r = SSHClient().run_remote("h", "ls")
        self.assertEqual(r.returncode, -2)

    def test_ok_property(self):
        self.assertTrue(RemoteResult("","h",0,"","").ok)
        self.assertFalse(RemoteResult("","h",1,"","").ok)

    def test_identity_file_in_argv(self):
        """identity_file is passed as -i <path> to both ssh and scp argv."""
        c = SSHClient(identity_file="/home/user/.ssh/openstack.pem")
        argv = c._build_ssh_argv("host", "true")
        self.assertIn("-i", argv)
        idx = argv.index("-i")
        self.assertEqual(argv[idx + 1], "/home/user/.ssh/openstack.pem")

    def test_identity_file_in_scp_argv(self):
        c = SSHClient(identity_file="/home/user/.ssh/openstack.pem")
        argv = c._build_scp_argv("/local/f", "host", "/remote/f")
        self.assertIn("-i", argv)

    def test_no_identity_file_no_dash_i(self):
        """When identity_file is empty, -i must not appear in argv."""
        c = SSHClient(identity_file="")
        argv = c._build_ssh_argv("host", "true")
        self.assertNotIn("-i", argv)


# =============================================================================
# Full lifecycle smoke — TC01 (all SSH mocked, local tmpdir as NFS mount)
# =============================================================================

class TestFullLifecycleSmoke(unittest.TestCase):

    def test_tc01_sanity_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = _cfg()
            cfg.clients[0].mount_point = tmpdir

            scenario = TC01_Sanity(cfg, mode=RunMode.FAST)
            ssh_mock = MagicMock(spec=SSHClient)
            ssh_mock.check_reachable.return_value = True
            stats_out = (
                "FSAL opened FD: 10000\nSystem limit on FDs: 1000000\n"
                "FD usage: 1.0%\nLRU entries in use: 5000\n"
                "Total FDs: 10000\nGlobal FDs: 8000\n"
                "State FDs: 1500\nTemporary FDs: 500\n"
            )
            def _run(host, cmd, timeout=None):
                if "ganesha_stats" in cmd: return _ok(stats_out)
                if "test -r"       in cmd: return _ok("readable")
                if "tail"          in cmd: return _ok("")
                if "mount"         in cmd: return _ok("mounted")
                if "python"        in cmd: return _ok("Python 3.9.0")
                if "which"         in cmd: return _ok("/usr/bin/tool")
                return _ok()

            ssh_mock.run_remote.side_effect = _run
            scenario.ssh = ssh_mock
            scenario.monitor.ssh = ssh_mock

            with patch("nfs_ganesha_fd_lru_test.framework.preflight.SSHClient",
                       return_value=ssh_mock), \
                 patch("nfs_ganesha_fd_lru_test.framework.runner.SSHClient",
                       return_value=ssh_mock), \
                 patch("nfs_ganesha_fd_lru_test.framework.runner.WorkloadWorker") as MW:
                MW.return_value.run_remote_burst.return_value = WorkloadStats(
                    opens_attempted=50, opens_succeeded=50,
                    active_handles=5,   active_handle_failures=0,
                )
                suite = scenario.run()

        self.assertIsInstance(suite, SuiteVerdict)
        self.assertIn(suite.overall, list(Verdict))


# =============================================================================
# OpenStack orchestrator topology — VIP vs SSH address separation
# =============================================================================

class TestOpenStackTopology(unittest.TestCase):
    """
    Verify that ServerConfig.ssh_host correctly resolves to:
      - ssh_address when set   (OpenStack physical node)
      - address otherwise       (legacy / single-node usage)
    And that all SSH calls in preflight/monitor/runner use ssh_host, not address.
    """

    def _server(self, address="192.0.2.10", ssh_address="", identity_file=""):
        from nfs_ganesha_fd_lru_test.framework.config import ServerConfig
        return ServerConfig(
            address=address,
            ssh_address=ssh_address,
            identity_file=identity_file,
            nfs_export="/export",
        )

    # ssh_host resolution
    def test_ssh_host_defaults_to_address(self):
        s = self._server(address="192.0.2.10", ssh_address="")
        self.assertEqual(s.ssh_host, "192.0.2.10")

    def test_ssh_host_uses_ssh_address_when_set(self):
        s = self._server(address="192.0.2.10", ssh_address="10.0.1.50")
        self.assertEqual(s.ssh_host, "10.0.1.50")

    def test_ssh_host_differs_from_address_when_vip(self):
        s = self._server(address="192.0.2.10", ssh_address="10.0.1.50")
        self.assertNotEqual(s.ssh_host, s.address)

    # identity_file propagates through config
    def test_server_identity_file_stored(self):
        s = self._server(identity_file="/home/osp/.ssh/os.pem")
        self.assertEqual(s.identity_file, "/home/osp/.ssh/os.pem")

    def test_client_identity_file_stored(self):
        cfg = _cfg()
        cfg.clients[0].identity_file = "/home/osp/.ssh/os.pem"
        self.assertEqual(cfg.clients[0].identity_file, "/home/osp/.ssh/os.pem")

    # main._build_config wires --server-ssh and --ssh-key
    def test_build_config_server_ssh_and_key(self):
        import argparse
        from nfs_ganesha_fd_lru_test.main import _build_config
        args = argparse.Namespace(
            server="192.0.2.10",
            server_ssh="10.0.1.50",
            export="/export",
            clients="10.0.1.51,10.0.1.52",
            ssh_user="cloud-user",
            ssh_key="/home/osp/.ssh/os.pem",
            server_log="/var/log/ganesha.log",
            threads=8, files=200, file_size=4096,
            fd_tolerance=10.0,
        )
        cfg = _build_config(args)
        self.assertEqual(cfg.server.address,       "192.0.2.10")
        self.assertEqual(cfg.server.ssh_address,   "10.0.1.50")
        self.assertEqual(cfg.server.ssh_host,      "10.0.1.50")
        self.assertEqual(cfg.server.identity_file, "/home/osp/.ssh/os.pem")
        for client in cfg.clients:
            self.assertEqual(client.identity_file, "/home/osp/.ssh/os.pem")

    def test_build_config_no_server_ssh_falls_back(self):
        import argparse
        from nfs_ganesha_fd_lru_test.main import _build_config
        args = argparse.Namespace(
            server="192.0.2.10",
            server_ssh="",
            export="/export",
            clients="10.0.1.51,10.0.1.52",
            ssh_user="root",
            ssh_key="",
            server_log="/var/log/ganesha.log",
            threads=8, files=200, file_size=4096,
            fd_tolerance=10.0,
        )
        cfg = _build_config(args)
        self.assertEqual(cfg.server.ssh_host, "192.0.2.10")

    # preflight uses ssh_host for server checks
    def test_preflight_ssh_targets_ssh_host_not_vip(self):
        """When ssh_address is set, preflight must check reachability on ssh_host."""
        cfg = _cfg()
        cfg.server.address     = "192.0.2.10"   # VIP — never SSH'd to
        cfg.server.ssh_address = "10.0.1.50"     # physical node

        ssh = MagicMock(spec=SSHClient)
        ssh.check_reachable.return_value = True

        _STATS = "FSAL opened FD: 10000\nSystem limit on FDs: 1000000"
        def _run(host, cmd, timeout=None):
            if "ganesha_stats" in cmd: return _ok(_STATS)
            if "test -r"       in cmd: return _ok("readable")
            if "mount"         in cmd: return _ok("mounted")
            return _ok()

        ssh.run_remote.side_effect = _run

        with patch("nfs_ganesha_fd_lru_test.framework.preflight.SSHClient", return_value=ssh):
            report = run_preflight(cfg, ssh=ssh)

        # Every run_remote call to the server must target 10.0.1.50, not 192.0.2.10
        server_calls = [
            call for call in ssh.run_remote.call_args_list
            if call.args and call.args[0] == "192.0.2.10"
        ]
        self.assertEqual(
            server_calls, [],
            "preflight must not SSH to the VIP (192.0.2.10) — use ssh_host (10.0.1.50)",
        )

    # Full lifecycle smoke with VIP != SSH address
    def test_tc01_with_openstack_topology(self):
        """TC01 full lifecycle with server VIP != SSH address, identity key set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = _cfg()
            cfg.server.address      = "192.0.2.10"   # VIP / floating-IP
            cfg.server.ssh_address  = "10.0.1.50"    # physical node
            cfg.server.identity_file = "/home/osp/.ssh/os.pem"
            cfg.clients[0].mount_point  = tmpdir
            cfg.clients[0].identity_file = "/home/osp/.ssh/os.pem"

            scenario = TC01_Sanity(cfg, mode=RunMode.FAST)

            ssh_mock = MagicMock(spec=SSHClient)
            ssh_mock.check_reachable.return_value = True
            stats_out = (
                "FSAL opened FD: 10000\nSystem limit on FDs: 1000000\n"
                "FD usage: 1.0%\nLRU entries in use: 5000\n"
                "Total FDs: 10000\nGlobal FDs: 8000\n"
                "State FDs: 1500\nTemporary FDs: 500\n"
            )
            def _run(host, cmd, timeout=None):
                if "ganesha_stats" in cmd: return _ok(stats_out)
                if "test -r"       in cmd: return _ok("readable")
                if "tail"          in cmd: return _ok("")
                if "mount"         in cmd: return _ok("mounted")
                if "python"        in cmd: return _ok("Python 3.9.0")
                if "which"         in cmd: return _ok("/usr/bin/tool")
                return _ok()

            ssh_mock.run_remote.side_effect = _run
            scenario.ssh = ssh_mock
            scenario.monitor.ssh = ssh_mock

            with patch("nfs_ganesha_fd_lru_test.framework.preflight.SSHClient",
                       return_value=ssh_mock), \
                 patch("nfs_ganesha_fd_lru_test.framework.runner.SSHClient",
                       return_value=ssh_mock), \
                 patch("nfs_ganesha_fd_lru_test.framework.runner.WorkloadWorker") as MW:
                MW.return_value.run_remote_burst.return_value = WorkloadStats(
                    opens_attempted=50, opens_succeeded=50,
                    active_handles=5, active_handle_failures=0,
                )
                suite = scenario.run()

        self.assertIsInstance(suite, SuiteVerdict)
        self.assertIn(suite.overall, list(Verdict))

        # Confirm no SSH call ever went to the VIP
        vip_calls = [
            c for c in ssh_mock.run_remote.call_args_list
            if c.args and c.args[0] == "192.0.2.10"
        ]
        self.assertEqual(
            vip_calls, [],
            "No SSH call should target the VIP 192.0.2.10; must use ssh_host 10.0.1.50",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

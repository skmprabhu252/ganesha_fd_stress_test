# framework/

Core infrastructure used by all test scenarios.

---

## Modules

### `config.py`

Configuration dataclasses for the entire test suite.

| Class | Purpose |
|-------|---------|
| `ProtocolMode` | Constants: `V3`, `V4`, `BOTH`; `validate()` normalises user input |
| `ServerConfig` | Ganesha server address, SSH access, log/stats paths, optional VIP split (`ssh_address`) |
| `ClientConfig` | Client node address, mount point, SSH credentials, role (`controller`/`worker`) |
| `WorkloadConfig` | Thread count, file count, file size, burst/cooldown durations, held-open count, retry parameters |
| `TestConfig` | Top-level config combining server, clients, workload, verdict thresholds, and `target_fd_ratio` |

**Key `TestConfig` fields**

| Field | Default | Description |
|-------|---------|-------------|
| `protocol` | `BOTH` | Protocol mode used for the run |
| `num_cycles` | `6` | Number of burst–cooldown cycles |
| `num_cycles_override` | `None` | If set, overrides the mode-default cycle count |
| `fd_tolerance_pct` | `10.0` | Max allowed settled-FD growth % across cycles |
| `fd_accounting_tolerance` | `100` | Allowed discrepancy in `total ≈ global + state + temp` |
| `target_fd_ratio` | `0.95` | Target open FDs relative to server FD limit. Set `> 1.0` (e.g. `1.50`) for negative stress tests that intentionally exceed the hard limit |
| `scenario` | `""` | Scenario filter; empty = run all |

**Helper methods on `TestConfig`**

| Method | Returns |
|--------|---------|
| `controller()` | The single `ClientConfig` with `role="controller"` |
| `workers()` | All `ClientConfig` entries with `role != "controller"` |
| `validate()` | Raises `ValueError` on any invalid field combination |

`make_default_test_config()` constructs a ready-to-use `TestConfig` for unit
testing with localhost-style addresses.

---

### `fd_stats.py`

Parses `ganesha_stats inode` output into structured `FDSample` objects.

**`FDSample` fields**

| Field | Description |
|-------|-------------|
| `fsal_opened_fd` | Total FSAL-opened FDs — the authoritative `global_fd + state_fd + temp_fd` sum |
| `system_fd_limit` | System FD limit reported by Ganesha |
| `fd_usage_pct` | FD usage as a percentage (computed from `fsal_opened_fd / system_fd_limit` when the stats output carries a text label instead of a number) |
| `fd_usage_label` | Raw text label from `ganesha_stats` (e.g. `"Above High Water Mark"`, `"Hard Limit reached"`) |
| `lru_entries_in_use` | Total inode-cache entries (superset of `fsal_opened_fd` — FDs may be reclaimed while inodes stay cached) |
| `chunks_in_use` | Chunk-cache entries in use |
| `global_fd` | Reclaimable FDs managed by the LRU (NFSv3 + reclaimable NFSv4) |
| `state_fd` | NFSv4 open-state FDs (closed by client `CLOSE`, never by LRU) |
| `temp_fd` | Short-lived FDs reclaimed quickly by the LRU |
| `total_fd` | Per-category total (backfilled from `fsal_opened_fd` when absent) |

**Key behaviours**

- Handles multiple Ganesha build variants: field name aliases (`FSAL opened FD count`,
  `FSAL opened FD`), comma-formatted numbers, text usage labels instead of
  numeric percentages.
- `fd_usage_pct` is computed from `fsal_opened_fd / system_fd_limit` when a text
  label is present.
- `effective_total_fd` property: returns `total_fd` when populated, else
  `fsal_opened_fd` (zero is a valid state — all FDs reclaimed by LRU).
- `fd_accounting_check` property: validates
  `fsal_opened_fd == global_fd + state_fd + temp_fd`.
- `BaselineStats` — wraps pre-workload samples and reports stability (< 10 % spread).

---

### `log_parser.py`

Parses Ganesha log lines for FD/LRU-specific events.

**`LogEventKind` values**

| Kind | Ganesha signal |
|------|----------------|
| `HARD_LIMIT` | Hard FD limit exceeded — reaper woken |
| `HIGH_WATERMARK` | FD count crossed HWM — LRU thread woken |
| `FUTILITY` | LRU cannot keep up with open rate |
| `STATE_FD_PRESSURE` | State FDs above threshold (GPFS pattern: `State FDs (N) exceed hiwat(M)`) |
| `GANESHA_RESTART` | Ganesha process start/restart detected |
| `FD_COUNT_DIAG` | Periodic FD breakdown diagnostic |
| `GENERIC_WARNING` | Other FD/LRU warning not matched by the above |

**Classification order** — a line is classified by the first matching pattern:

1. `GANESHA_RESTART`
2. `HARD_LIMIT` — checked before `HIGH_WATERMARK` because hard-limit lines also
   contain the phrase "waking LRU thread" which would otherwise trigger a false
   `HIGH_WATERMARK` match
3. `STATE_FD_PRESSURE`
4. `FUTILITY`
5. `HIGH_WATERMARK`
6. `FD_COUNT_DIAG`

**FD number extraction** — matches both Ganesha log formats:

```
# Standard format:
total=18000 global=18000 state=0 temp=0

# GPFS/RHEL9 format (with _fds suffix and optional parenthetical):
total_fds=18000 (was 12002), global_fds=18000 (was 12002), state_fds=0, temp_fds=0

# State FD pressure (GPFS custom branch):
State FDs (19016) exceed hiwat(12000)
```

**Restart detection** — matches canonical startup banners
(`NFS SERVER INITIALIZED`, `NFS STARTUP`, `nfs_start`,
`NFS-Ganesha Release`, `Initializing memory and logging`,
`ganesha_init_complete`, `Loading parameters from`)
and explicitly excludes ordinary per-thread/per-RPC log lines containing
the process name (`gpfs.ganesha.nfsd-NNNN[svc_0]`), preventing false-positive
restart counts.

---

### `monitor.py`

Background thread that polls `ganesha_stats` and tails the Ganesha log during
burst and cooldown phases.

**`ServerMonitor`**

| Method | Description |
|--------|-------------|
| `calibrate_server_time()` | Runs `date +%s` on the server via SSH and stores the result as `_test_start_time`. Anchors the log-event filter to the **server's own clock**, eliminating controller-vs-server clock skew. Falls back to controller `time.time()` with a warning if SSH fails. Must be called once before the first `start_phase()`. |
| `start_phase(label)` | Starts a daemon polling thread and returns a new `MonitorPhase`. |
| `stop_phase(phase)` | Signals the thread to stop, joins it (30 s timeout), records `end_time`. |
| `collect_baseline(num_samples, interval_sec)` | Collects FD snapshots before the workload starts. Returns a `MonitorPhase` labelled `"baseline"`. |

**`MonitorPhase` derived properties**

| Property | Description |
|----------|-------------|
| `peak_fsal_fd` | Maximum `fsal_opened_fd` across all samples |
| `settled_fsal_fd` | Last sample's `fsal_opened_fd` (representative post-cooldown level) |
| `peak_global_fd` / `settled_global_fd` | Peak and settled global (reclaimable) FD counts |
| `peak_lru_entries` / `settled_lru_entries` | Peak and settled inode-cache entry counts |
| `high_watermark_reached` | True if any `HIGH_WATERMARK` log event **or** any sample with `fd_usage_label` matching `"above high water mark"` / `"hard limit reached"` |
| `hard_limit_reached` | True if any `HARD_LIMIT` log event **or** any sample with `fd_usage_label` matching `"hard limit"` |
| `futility_detected` | True if any `FUTILITY` event |
| `state_fd_pressure_detected` | True if any `STATE_FD_PRESSURE` event |
| `ganesha_restarted` | True if any `GANESHA_RESTART` event |
| `lru_made_progress(protocol, burst_phase)` | True if reclaimable FDs fell > 10 % from the burst peak to cooldown end. When `burst_phase` is provided, peak is taken from the burst (correct reference point). |
| `fd_settled()` | True when the last three samples are within 10 % spread. |

**Event deduplication** — three layers:

1. **Poll-window repeats** — `tail -n N` re-reads the same lines every poll;
   deduplicated by exact `raw_line`.
2. **Per-thread fan-out** — when Ganesha hits a hard limit, every service thread
   (`svc_N`) independently logs the same message. Deduplicated by
   `(kind, second_bucket, thread-normalised line)` using `_THREAD_RE`:
   - `[svc_1197]` and `[svc_35]` both normalise to `[svc_]`
   - Fixed-name threads (`[fd_lru]`, `[reaper]`, `[main]`) are unchanged
3. **Restart fan-out** — additionally collapsed by `(minute_bucket, epoch, node)`.

**Clock skew handling** — `_test_start_time` is the **server-side epoch** set by
`calibrate_server_time()`. Log timestamps are parsed from the Ganesha log (server
time), so the comparison is in the same time domain regardless of
controller-vs-server clock offset. Before calibration, `_test_start_time` is
`None` and all events are accepted (safe default for unit tests).

---

### `preflight.py`

Validates the environment before any workload is applied.

**`PreflightReport`** accumulates `[OK]`, `[WARN]`, and `[FAIL]` entries and
exposes a `passed` property (`True` when no errors). `summary()` produces a
human-readable block printed before each scenario.

**Checks performed by `run_preflight(config)`**

| Check | Severity on failure |
|-------|---------------------|
| Server reachable via SSH (`ssh_host`) | FAIL |
| `ganesha_stats inode` works and returns valid output | FAIL |
| Ganesha log path readable | WARN |
| Ganesha binary found (`ganesha.nfsd` or `gpfs.ganesha.nfsd`) | WARN |
| Each client node reachable via SSH | FAIL |
| Python 3 available on each client | WARN |
| `mount`, `umount`, `df`, `stat` available on each client | WARN |
| NFS export mountable from each client | WARN |

A `PreflightError` is raised (and the scenario aborts) only when `errors` is
non-empty after all checks.

---

### `report.py`

Builds a human-readable plain-text test report from suite verdicts, phase data,
and environment info. Written to stdout or `--report-file`.

**Report sections**

| Section | Content |
|---------|---------|
| 1. Environment | Server address, export, protocol, Ganesha version, kernel, FD limit, clients |
| 2. Workload Configuration | Cycles, threads, files, burst/cooldown durations, held-open count, retry timeout |
| 3. Baseline FD Statistics | Per-sample table of FD counts before workload; stability indicator |
| 4. FD/LRU Time Series | Per-phase table: Total FD, Global, State, Temp, Usage %, LRU cache |
| 5. Ganesha Log Events | Correlated events with timestamp, kind, FD counters (`total/global/state/temp`) |
| 6. Workload Counters | Aggregate opens, closes, creates, reads, writes, error breakdown |
| 7. Verdict | Per-cycle and suite-level dimension results; flat worst-per-dimension summary |

Also exposes `to_json()` for machine-readable output.

**Event deduplication across phases** — the report deduplicates log events by a
content key `(kind, normalised_line)` across burst and cooldown phases so that
the same logical event is not printed twice in the log-events section.

---

### `runner.py`

Implements the burst → cooldown → verdict cycle and the `BaseScenario` base class.

**`CycleRunner.run(cycle_number, protocol)`** — one complete cycle:

1. Log cycle start
2. Mount NFS on every client via SSH  
   (`<mount_point>/v3` for V3, `<mount_point>/v4` for V4)
3. **Clean up workload directories** from the previous cycle (logged at INFO)
4. Start burst monitor phase
5. Launch workload concurrently on every client — for `BOTH` protocol, V3 and V4
   workers run concurrently per client against separate mount points
6. Collect and merge per-client workload statistics
7. Stop burst monitor
8. Start cooldown monitor phase; sleep for `cooldown_duration_sec`
9. Stop cooldown monitor
10. Unmount NFS on all clients (always, in `finally`)
11. Evaluate and return `(CycleVerdict, burst_phase, cooldown_phase, stats)`

**`BaseScenario.run()`** — full scenario lifecycle:

1. `setup_extra_config()` — apply mode-specific workload scaling
2. Pre-flight checks — abort on failure
3. Collect environment info (OS, Ganesha version, FD limit)
4. Collect baseline FD samples (5 × 3 s)
5. `setup_pressure_config(fd_limit, num_clients)` — scale files/threads to
   `config.target_fd_ratio × fd_limit`
6. `self.monitor.calibrate_server_time()` — anchor log filter to server clock
7. Iterate cycles 1 … `config.num_cycles`:
   - Run `CycleRunner.run()`
   - Call `post_cycle_hook(cycle, cv)` — scenario-specific adaptive scaling
   - Stop early on Ganesha restart
8. Evaluate suite verdict; build and emit final report

---

### `ssh_client.py`

Thin wrapper around `subprocess` for `ssh` and `scp` commands.

**`RemoteResult`** fields: `command`, `host`, `returncode`, `stdout`, `stderr`,
`elapsed_sec`, `ok` (property: `returncode == 0`).

| Method | Description |
|--------|-------------|
| `run_remote(host, cmd, timeout)` | Run a shell command on a remote host; returns `RemoteResult` |
| `copy_to_remote(local, host, remote, timeout)` | SCP a local file to a remote host |
| `is_reachable(host, timeout)` | Returns `True` if SSH connects and exits cleanly |

When `identity_file` is set on the `SSHClient`, `-i <path>` is injected into
every `ssh` and `scp` argument list. This supports OpenStack tenant keys
(`~/.ssh/openstack.pem`) without modifying system SSH config.

---

### `verdict.py`

Evaluates all evidence from a cycle and produces per-dimension
PASS / WARNING / FAIL / INCONCLUSIVE results.

**Reaper-aware verdict logic**

`check_lru_reclamation` and `check_high_watermark` are built around Ganesha's
reaper design:

```
Hard limit (100%) → triggers aggressive reap
HWM       ( 90%) → reaper target — reaper stops once FDs drop below this
LWM       (~10%) → reaper floor (only reached during prolonged idle)
```

- **PASS**: settled FDs < HWM (90 % of `system_fd_limit`) after cooldown
- **FAIL**: settled FDs ≥ HWM after cooldown (reaper did not finish)
- FDs settling between HWM and LWM is **normal** — the reaper does not drive
  FDs to LWM during a burst window
- When `system_fd_limit` is unavailable, falls back to a relative-drop heuristic
  (≥ 50 % reclaimed = PASS, ≥ 20 % = WARNING, < 20 % = FAIL)

**All verdict dimensions**

| Dimension | Passes when |
|-----------|-------------|
| `workload_completion` | Persistent open failures < 1 % of attempts |
| `active_handle_protection` | All held-open handles remain valid (no ESTALE/EBADF) |
| `ganesha_no_restart` | No `GANESHA_RESTART` events |
| `lru_reclamation` | Settled reclaimable FDs < HWM (90 % of fd_limit) after cooldown |
| `high_watermark_handling` | HWM reached (WARNING expected) and reaper brought FDs below HWM |
| `hard_limit_handling` | Hard limit reached (WARNING expected) and client recovered |
| `futility_detection` | Futility events detected but FDs subsequently settled and reclaimed |
| `fd_settled_after_cooldown` | Last three FD samples within 10 % spread |
| `fd_retention_across_cycles` | Settled FD growth across cycles ≤ `fd_tolerance_pct` |
| `fd_accounting` | `total ≈ global + state + temp` in ≤ 50 % of samples |
| `server_monitoring` | At least one `ganesha_stats` sample collected |
| `no_mount_loss` | ESTALE errors < 5 % of open attempts |
| `client_fd_exhaustion` | No EMFILE (client FD exhaustion); distinguishes from server pressure |
| `v4_state_fd_closure` | (V4/BOTH) State FDs released by clients ≥ 80 % after cooldown |
| `state_fd_pressure` | (V4/BOTH) State FD pressure events classified as WARNING, not FAIL |

---

### `workload.py`

NFS burst workload engine.

**`WorkloadStats`** fields (all int counters, merged across threads):

| Field | Description |
|-------|-------------|
| `opens_attempted` | Total open calls issued |
| `opens_succeeded` | Opens that returned a valid fd |
| `opens_failed` | Opens that failed after all retries |
| `opens_retried` / `opens_eventually_ok` | Transient-error retry counters |
| `closes` / `creates` / `reads` / `writes` / `dir_ops` | Filesystem op counters |
| `active_handles` / `active_handle_failures` | Held-open file count and ESTALE/EBADF failures |
| `emfile_count` | Client-side FD exhaustion (EMFILE) — distinguished from server pressure |
| `eio_count` | Server-side I/O errors (EIO) |
| `estale_count` | Stale NFS handle errors (ESTALE) |
| `enfile_count` | System FD table full (ENFILE) |
| `other_errors` | Uncategorised errors |

**`WorkloadWorker`**

- `run_burst()` — in-process burst (controller node, unit tests).
- `run_remote_burst(ssh, host)` — deploys a self-contained Python worker script
  to the client via SCP and runs it via SSH. The remote script has **no framework
  dependencies** (stdlib only). Config is passed as a JSON file to avoid
  shell-quoting issues.
- Each client writes to a unique server-side path
  (`client_<id>_thread_NNN_<proto>`) so Ganesha opens a distinct FD per client
  instead of reusing a shared global FD.
- Bounded retry for transient `EIO` / `ENFILE` errors (configurable
  `retry_timeout_sec` and `retry_interval_sec`).
- Dynamic soft-limit elevation on the client (`resource.setrlimit`) to match the
  hard FD limit, preventing accidental EMFILE exhaustion during high-ratio runs.

**`HeldHandle`**

Opens a file and keeps it open throughout the burst. Calls `validate()` mid-burst
and post-burst; records `active_handle_failures` on ESTALE/EBADF. `close()` is
idempotent and safe to call multiple times.

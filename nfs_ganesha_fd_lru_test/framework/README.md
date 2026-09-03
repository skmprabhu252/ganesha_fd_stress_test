# framework/

Core infrastructure used by all test scenarios.

---

## Modules

### `config.py`

Configuration dataclasses for the entire test suite.

| Class | Purpose |
|-------|---------|
| `ProtocolMode` | Constants: `V3`, `V4`, `BOTH` |
| `ServerConfig` | Ganesha server address, SSH access, log/stats paths, optional VIP split |
| `ClientConfig` | Client node address, mount point, SSH credentials, role (`controller`/`worker`) |
| `WorkloadConfig` | Thread count, file count, file size, burst/cooldown durations, retry parameters |
| `TestConfig` | Top-level config combining server, clients, workload, and verdict thresholds |

---

### `fd_stats.py`

Parses `ganesha_stats inode` output into structured `FDSample` objects.

**`FDSample` fields**

| Field | Description |
|-------|-------------|
| `fsal_opened_fd` | Total FSAL-opened FDs (`global + state + temp`) |
| `system_fd_limit` | System FD limit reported by Ganesha |
| `fd_usage_pct` | FD usage as a percentage (computed when a text label is reported) |
| `fd_usage_label` | Raw text label from `ganesha_stats` (e.g. `"Above High Water Mark"`, `"Hard Limit reached"`) |
| `lru_entries_in_use` | Total inode-cache entries (superset of `fsal_opened_fd` — FDs may be reclaimed while inodes stay cached) |
| `global_fd` | Reclaimable FDs managed by the LRU (NFSv3 + reclaimable NFSv4) |
| `state_fd` | NFSv4 open-state FDs (closed by client `CLOSE`, never by LRU) |
| `temp_fd` | Short-lived FDs reclaimed quickly by the LRU |

**Key behaviours**
- Handles multiple Ganesha build variants: field name aliases, comma-formatted numbers,
  text usage labels instead of numeric percentages.
- `fd_usage_pct` is computed from `fsal_opened_fd / system_fd_limit` when a text label
  is present instead of a numeric value.
- `fd_accounting_check` property: validates `fsal_opened_fd == global_fd + state_fd + temp_fd`.
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
| `STATE_FD_PRESSURE` | State FDs above threshold |
| `GANESHA_RESTART` | Ganesha process start/restart detected |
| `FD_COUNT_DIAG` | Periodic FD breakdown diagnostic |

**Classification order** — a line is classified by the first matching pattern:

1. `GANESHA_RESTART`
2. `HARD_LIMIT` — checked before `HIGH_WATERMARK` because hard-limit lines also
   contain the phrase "waking LRU thread" which would otherwise trigger a false
   `HIGH_WATERMARK` match
3. `STATE_FD_PRESSURE`
4. `FUTILITY`
5. `HIGH_WATERMARK`
6. `FD_COUNT_DIAG`

**FD number extraction** — `_FD_NUMS` matches both Ganesha log formats:

```
# Standard format:
total=18000 global=18000 state=0 temp=0

# GPFS/RHEL9 format (with _fds suffix and optional parenthetical):
total_fds=18000 (was 12002), global_fds=18000 (was 12002), state_fds=0, temp_fds=0
```

**Restart detection** — matches the canonical startup banner
(`NFS SERVER INITIALIZED`, `NFS STARTUP`, `nfs_start`, etc.) and explicitly
excludes ordinary per-thread/per-RPC log lines that contain the process name,
preventing false-positive restart counts.

---

### `monitor.py`

Background thread that polls `ganesha_stats` and tails the Ganesha log during
burst and cooldown phases.

**`ServerMonitor`**

| Method | Description |
|--------|-------------|
| `calibrate_server_time()` | Runs `date +%s` on the server via SSH and stores the result as `_test_start_time`. This anchors the log-event filter to the **server's own clock**, eliminating controller-vs-server clock skew. Falls back to controller `time.time()` with a warning if SSH fails. Must be called once before the first `start_phase()`. |
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
| `high_watermark_reached` | True if any `HIGH_WATERMARK` log event **or** any sample with `fd_usage_label` containing `"above high water mark"` / `"hard limit reached"` |
| `hard_limit_reached` | True if any `HARD_LIMIT` log event **or** any sample with `fd_usage_label` containing `"hard limit"` |
| `futility_detected` | True if any `FUTILITY` event |
| `state_fd_pressure_detected` | True if any `STATE_FD_PRESSURE` event |
| `ganesha_restarted` | True if any `GANESHA_RESTART` event |
| `lru_made_progress(protocol, burst_phase)` | True if reclaimable FDs fell > 10 % from the burst peak to cooldown end. When `burst_phase` is provided, peak is taken from the burst (correct reference point). |
| `fd_settled()` | True when the last three samples are within 10 % spread. |

**Event deduplication** — two layers:

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
time), so the comparison is in the same time domain regardless of controller-vs-server
clock offset. Before calibration, `_test_start_time` is `None` and all events are
accepted (safe default for unit tests).

---

### `preflight.py`

Validates the environment before any workload is applied:

- Server reachable via SSH
- `ganesha_stats` command works and returns valid output
- Ganesha log readable
- All client nodes reachable via SSH
- Python 3, `mount`, `umount`, `df`, `stat` available on each client
- NFS export mountable from each client

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
| 6. Workload Counters | Aggregate opens, closes, creates, reads, writes, errors |
| 7. Verdict | Per-cycle and suite-level dimension results; flat worst-per-dimension summary |

Also exposes `to_json()` for machine-readable output.

---

### `runner.py`

Implements the burst → cooldown → verdict cycle and the `BaseScenario` base class.

**`CycleRunner.run(cycle_number, protocol)`** — one complete cycle:

1. Log cycle start
2. Mount NFS on every client via SSH
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
5. `setup_pressure_config(fd_limit, num_clients)` — scale files/threads to target FD pressure
6. **`self.monitor.calibrate_server_time()`** — anchor log filter to server clock
7. Iterate cycles 1 … `config.num_cycles`:
   - Run `CycleRunner.run()`
   - Call `post_cycle_hook(cycle, cv)` — scenario-specific adaptive scaling
   - Stop early on Ganesha restart
8. Evaluate suite verdict; build and emit final report

---

### `ssh_client.py`

Thin wrapper around `subprocess` for `ssh` and `scp` commands.

| Method | Description |
|--------|-------------|
| `run_remote(host, cmd, timeout)` | Run a shell command on a remote host; returns `RemoteResult(ok, stdout, stderr, returncode)` |
| `copy_to_remote(local, host, remote, timeout)` | SCP a file to a remote host |
| `is_reachable(host, timeout)` | Returns True if SSH connects and exits cleanly |

---

### `verdict.py`

Evaluates all evidence from a cycle and produces per-dimension PASS/WARNING/FAIL/INCONCLUSIVE results.

**Reaper-aware verdict logic**

`check_lru_reclamation` and `check_high_watermark` are built around Ganesha's reaper design:

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

---

### `workload.py`

NFS burst workload engine.

- `WorkloadWorker.run_burst()` — in-process burst (for local/controller node).
- `WorkloadWorker.run_remote_burst(ssh, host)` — deploys a self-contained Python
  worker script to the client via SCP and runs it via SSH. The remote script has
  **no framework dependencies** (stdlib only). Config is passed as a JSON file
  to avoid shell-quoting issues.
- Each client writes to a unique server-side path (`client_<id>_thread_NNN_<proto>`)
  so Ganesha opens a distinct FD per client instead of reusing a shared global FD.
- Error classification: `EMFILE` (client FD exhaustion), `EIO`, `ESTALE`, `ENFILE`, `OTHER`.
- Bounded retry for transient `EIO`/`ENFILE` errors (configurable timeout and interval).
- `HeldHandle` — opens a file and validates it mid-burst and post-burst; reports
  `active_handle_failures` on ESTALE/EBADF.

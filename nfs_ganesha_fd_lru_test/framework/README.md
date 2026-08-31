# framework/

Core infrastructure used by all test scenarios.

---

## Modules

### `config.py`
Configuration dataclasses for the entire test suite.

| Class | Purpose |
|-------|---------|
| `ProtocolMode` | Constants: `V3`, `V4`, `BOTH` |
| `ServerConfig` | Ganesha server address, SSH access, log/stats paths |
| `ClientConfig` | Client node address, mount point, SSH credentials |
| `WorkloadConfig` | Thread count, file count, burst/cooldown durations |
| `TestConfig` | Top-level config combining all of the above |

---

### `fd_stats.py`
Parses `ganesha_stats inode` output into structured `FDSample` objects.

- Handles multiple Ganesha build variants (field name aliases, text labels vs numeric percentages).
- Provides the `fd_accounting_check` property: validates `fsal_opened_fd == global_fd + state_fd + temp_fd`.
- `BaselineStats` — pre-workload FD stability check.

---

### `log_parser.py`
Parses Ganesha log lines for FD/LRU-specific events introduced by Patch 1247084.

| `LogEventKind` | Ganesha Log Signal |
|----------------|--------------------|
| `HIGH_WATERMARK` | FD pressure detected, LRU thread woken |
| `HARD_LIMIT` | Configured hard FD limit exceeded |
| `FUTILITY` | LRU cannot keep up with open rate |
| `STATE_FD_PRESSURE` | State FDs above threshold |
| `GANESHA_RESTART` | Ganesha process start/restart detected |
| `FD_COUNT_DIAG` | Periodic FD breakdown diagnostic |

---

### `monitor.py`
Background thread that polls `ganesha_stats` and tails the Ganesha log during
burst and cooldown phases.

- `ServerMonitor.start_phase(label)` → starts polling, returns a `MonitorPhase`.
- `ServerMonitor.stop_phase(phase)` → stops polling, finalises the phase.
- `MonitorPhase` exposes derived metrics: `peak_fsal_fd`, `settled_fsal_fd`,
  `lru_made_progress()`, `fd_settled()`, `high_watermark_reached`, etc.

---

### `preflight.py`
Validates the environment before any workload is applied:

- Server reachable via SSH
- `ganesha_stats` command works
- Ganesha log readable
- All client nodes reachable via SSH
- NFS export mountable

---

### `report.py`
Builds a human-readable plain-text test report from suite verdicts, phase data,
and environment info. Written to stdout or `--report-file`.

---

### `runner.py`
Implements the burst → cooldown → verdict cycle and the `BaseScenario` base class.

- `CycleRunner.run()` — mounts NFS, runs workload across all clients concurrently,
  runs cooldown, unmounts, evaluates verdict.
- `BaseScenario` — inherited by all TC-XX scenario classes. Handles pre-flight,
  baseline collection, pressure scaling, cycle loop, Ganesha-restart fast-stop,
  and final report generation.

---

### `ssh_client.py`
Thin wrapper around `subprocess` for `ssh` and `scp` commands.

- `SSHClient.run_remote(host, cmd)` — run a shell command on a remote host.
- `SSHClient.copy_to_remote(local, host, remote)` — SCP a file to a remote host.

---

### `verdict.py`
Evaluates all evidence from a cycle and produces per-dimension verdicts.

| Dimension | What is checked |
|-----------|----------------|
| `workload_completion` | < 1 % persistent open failures |
| `active_handle_protection` | Held-open handles not invalidated by LRU |
| `lru_reclamation` | ≥ 50 % of peak FDs reclaimed after cooldown |
| `high_watermark_handling` | HWM reached and LRU made progress |
| `hard_limit_handling` | Hard-limit reached and client recovered via retry |
| `futility_detection` | Futility events handled; no combined failure signal |
| `fd_settled_after_cooldown` | FD count stabilises within cooldown window |
| `fd_retention_across_cycles` | No monotonic FD growth across cycles |
| `fd_accounting` | `total ≈ global + state + temp` throughout |
| `ganesha_no_restart` | No Ganesha restart during test |
| `server_monitoring` | `ganesha_stats` remained reachable |
| `no_mount_loss` | ESTALE rate below threshold |
| `client_fd_exhaustion` | EMFILE distinguished from server-side pressure |
| `v4_state_fd_closure` | (V4 only) State FDs released by clients after cooldown |

---

### `workload.py`
NFS burst workload engine.

- `WorkloadWorker.run_burst()` — in-process burst (used for local/controller node).
- `WorkloadWorker.run_remote_burst(ssh, host)` — deploys a self-contained Python
  worker script to the client via SCP and runs it via SSH. The remote script has
  **no framework dependencies** (stdlib only).
- Classifies errors: `EMFILE` (client FD exhaustion), `EIO`, `ESTALE`, `ENFILE`,
  `OTHER`.
- Implements bounded retry for transient `EIO`/`ENFILE` errors.
- `HeldHandle` — validates held-open file descriptors mid-burst and post-burst.

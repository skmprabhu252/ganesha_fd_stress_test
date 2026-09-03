# NFS-Ganesha FD/LRU Stress and Reclamation Test Framework

Automated test suite that validates NFS-Ganesha's file-descriptor (FD) lifecycle
and LRU reclamation behaviour under controlled pressure. The framework supports
NFSv3, NFSv4, and mixed-protocol workloads executed across multiple client nodes
via SSH.

---

## Test Scenarios

| ID   | Name              | Protocol | Description |
|------|-------------------|----------|-------------|
| TC01 | Sanity            | Any      | Environment validation + minimal smoke test |
| TC02 | NFSv3 Full Stress | V3       | Complete V3 FD/LRU lifecycle — watermark, reaper, reclamation, retention, active handles |
| TC03 | NFSv4 Full Stress | V4       | V4 FD/LRU lifecycle + state-FD closure validation |
| TC04 | Mixed Stress      | V3+V4    | Concurrent V3+V4 workload, dual-category FD pressure |

## Run Modes

| Mode   | Cycles | Thread × | File × | Burst  | Cooldown | Held-open | Use Case |
|--------|--------|----------|--------|--------|----------|-----------|----------|
| fast   | 1      | 1×       | 0.5×   | 20 s   | 30 s     | 5         | CI gate / quick smoke |
| normal | 6      | 2×       | 2×     | 60 s   | 90 s     | 20        | Standard FVT (default) |
| soak   | 12     | 4×       | 4×     | 120 s  | 180 s    | 50        | Regression / overnight |

Use `--cycles N` to override the cycle count for any mode without changing other scaling parameters.

---

## Requirements

- Python ≥ 3.8 on the **orchestrator node** (where this tool runs)
- `ssh` and `scp` available on the orchestrator node
- Password-less SSH access (key-based) from the orchestrator to the Ganesha
  server and all client nodes
- NFS clients must be able to mount from the Ganesha server
- `ganesha_stats` command available on the Ganesha server

---

## Installation

```bash
pip install .
```

Or run directly without installing:

```bash
python -m nfs_ganesha_fd_lru_test.main --help
```

---

## Quick Start

### Run all scenarios (normal mode)

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server  ganesha-node1 \
    --export  /export \
    --clients client-1,client-2,client-3
```

### Single scenario — fast mode (CI)

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server   ganesha-node1 \
    --export   /export \
    --clients  client-1,client-2,client-3 \
    --scenario TC01 --mode fast
```

### V3 stress — normal mode, 3 cycles

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server   ganesha-node1 \
    --export   /export \
    --clients  client-1,client-2,client-3 \
    --scenario TC02 --mode normal --cycles 3
```

### V4 stress — soak mode

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server   ganesha-node1 \
    --export   /export \
    --clients  client-1,client-2,client-3 \
    --scenario TC03 --mode soak -v
```

### OpenStack / VIP topology

When the Ganesha server is behind a VIP (floating-IP) and the real physical
node must be reached separately for SSH:

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server     192.0.2.10 \
    --server-ssh 10.0.1.50 \
    --export     /export \
    --clients    10.0.1.51,10.0.1.52 \
    --ssh-user   cloud-user \
    --ssh-key    ~/.ssh/openstack.pem \
    --scenario   TC01
```

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | _(required)_ | Ganesha server hostname/IP for NFS mounts |
| `--export` | _(required)_ | NFS export path (e.g. `/export`) |
| `--clients` | _(required)_ | Comma-separated client nodes. First = controller |
| `--scenario` | _(all)_ | One of `TC01 TC02 TC03 TC04`. Empty = run all |
| `--mode` | `normal` | `fast` / `normal` / `soak` — controls workload size and default cycle count |
| `--cycles` | `0` | Override cycle count (`0` = use mode default: fast=1, normal=6, soak=12) |
| `--server-ssh` | `--server` | Physical node IP for SSH when server is behind a VIP |
| `--ssh-user` | `root` | SSH user for server and all clients |
| `--ssh-key` | _(none)_ | Path to SSH private key on the orchestrator |
| `--threads` | `8` | Base threads per client (scaled by mode multiplier) |
| `--files` | `200` | Base files per thread (scaled by mode multiplier) |
| `--file-size` | `4096` | File size in bytes |
| `--fd-tolerance` | `10.0` | Max allowed settled-FD growth % across cycles |
| `--server-log` | `/var/log/ganesha.log` | Path to Ganesha log on the server |
| `--report-file` | _(stdout)_ | Write final report to this file |
| `-v` / `--verbose` | `False` | Enable DEBUG logging |

---

## Ganesha Reaper Design

The framework's verdict logic is built around Ganesha's LRU reaper thread design:

```
100% ── Hard Limit ──────── triggers aggressive FD reap
 90% ── High Water Mark ─── reaper TARGET — reaper wakes here and stops here
        ↑ FDs settle in this zone after a burst (normal and correct)
 10% ── Low Water Mark ──── reaper floor (only reached during prolonged idle)
  0% ─────────────────────── no FDs open
```

- When the **hard limit** (100 %) is hit, the reaper thread wakes and aggressively
  closes LRU entries.
- The reaper stops once FDs drop **below the high-water mark** (90 %).
- It does **not** drive FDs to the low-water mark during a burst window.
- FDs settling between HWM and LWM after a burst is **expected and correct**.

---

## Verdict Levels

| Verdict | Meaning |
|---------|---------|
| `PASS` | Check passed |
| `WARNING` | Expected pressure event (HWM, hard limit, futility) observed and correctly handled |
| `INCONCLUSIVE` | A required condition could not be exercised (e.g. HWM not reached — workload pressure insufficient) |
| `FAIL` | Incorrect or unrecovered behaviour detected |

### Verdict dimensions evaluated per cycle

| Dimension | What is checked |
|-----------|----------------|
| `workload_completion` | < 1 % persistent open failures |
| `active_handle_protection` | Held-open handles not invalidated by LRU (ESTALE/EBADF = immediate FAIL) |
| `lru_reclamation` | Reaper brought settled FDs below HWM (90 % of fd_limit) after cooldown |
| `high_watermark_handling` | HWM reached (WARNING) and reaper subsequently brought FDs below HWM |
| `hard_limit_handling` | Hard limit reached and client recovered (no persistent failures) |
| `futility_detection` | Futility events handled; fails only when combined with no reclamation + no settle |
| `fd_settled_after_cooldown` | FD count stabilises within cooldown window (HWM–LWM range is normal) |
| `fd_retention_across_cycles` | No monotonic FD growth across cycles beyond tolerance |
| `fd_accounting` | `total ≈ global + state + temp` throughout |
| `ganesha_no_restart` | No Ganesha restart during test |
| `server_monitoring` | `ganesha_stats` remained reachable throughout |
| `no_mount_loss` | ESTALE rate below 5 % of opens |
| `client_fd_exhaustion` | EMFILE (client FD exhaustion) distinguished from server-side pressure |
| `v4_state_fd_closure` | (V4/BOTH only) State FDs released by clients after cooldown |

---

## Project Structure

```
nfs_ganesha_fd_lru_test/
├── main.py                  # CLI entry-point
├── framework/               # Core test infrastructure
│   ├── config.py            # Configuration dataclasses
│   ├── fd_stats.py          # ganesha_stats parser + FDSample model
│   ├── log_parser.py        # Ganesha log event parser
│   ├── monitor.py           # Background server monitor (FD + log)
│   ├── preflight.py         # Pre-flight environment checks
│   ├── report.py            # Human-readable report builder
│   ├── runner.py            # Cycle runner + BaseScenario
│   ├── ssh_client.py        # SSH/SCP helpers
│   ├── verdict.py           # Verdict engine (per-dimension + suite)
│   └── workload.py          # NFS burst workload + remote worker script
├── scenarios/               # TC01–TC04 test scenario implementations
│   ├── mode.py              # RunMode + ModeProfile definitions
│   ├── registry.py          # Scenario registry
│   ├── tc01_sanity.py
│   ├── tc02_v3_stress.py
│   ├── tc03_v4_stress.py
│   └── tc04_mixed_stress.py
└── tests/                   # Unit tests (pytest)
    └── test_fd_lru_framework.py
```

---

## Running Unit Tests

```bash
pytest
# or verbosely:
python -m pytest nfs_ganesha_fd_lru_test/tests/test_fd_lru_framework.py -v
```

---

## License

Apache-2.0

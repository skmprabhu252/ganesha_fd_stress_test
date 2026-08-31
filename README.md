# NFS-Ganesha FD/LRU Stress and Reclamation Test Framework

Automated test suite that validates NFS-Ganesha's file-descriptor (FD) lifecycle
and LRU reclamation behaviour under controlled pressure. The framework supports
NFSv3, NFSv4, and mixed-protocol workloads executed across multiple client nodes
via SSH.

---

## Test Scenarios

| ID   | Name            | Protocol | Description |
|------|-----------------|----------|-------------|
| TC01 | Sanity          | Any      | Environment validation + minimal smoke test |
| TC02 | NFSv3 Full Stress | V3     | Complete V3 FD/LRU lifecycle — watermark, reclamation, retention, active handles |
| TC03 | NFSv4 Full Stress | V4     | V4 FD/LRU lifecycle + state-FD closure validation |
| TC04 | Mixed Stress    | V3+V4    | Concurrent V3+V4 workload, dual-category FD pressure |

## Run Modes

| Mode   | Cycles | Workload | Approx. Duration | Use Case |
|--------|--------|----------|------------------|----------|
| fast   | 1      | ×0.5     | ~2–5 min         | CI gate / quick smoke |
| normal | 6      | ×2       | ~30 min          | Standard FVT (default) |
| soak   | 12     | ×4       | ~90 min          | Regression / overnight |

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
    --server  ganesha-node1 \
    --export  /export \
    --clients client-1,client-2,client-3 \
    --scenario TC01 --mode fast
```

### V4 stress — soak mode

```bash
python -m nfs_ganesha_fd_lru_test.main \
    --server  ganesha-node1 \
    --export  /export \
    --clients client-1,client-2,client-3 \
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
| `--mode` | `normal` | `fast` / `normal` / `soak` |
| `--server-ssh` | `--server` | Physical node IP for SSH when server is behind a VIP |
| `--ssh-user` | `root` | SSH user for server and all clients |
| `--ssh-key` | _(none)_ | Path to SSH private key |
| `--threads` | `8` | Base threads per client (scaled by mode) |
| `--files` | `200` | Base files per thread (scaled by mode) |
| `--file-size` | `4096` | File size in bytes |
| `--fd-tolerance` | `10.0` | Max allowed settled-FD growth % across cycles |
| `--server-log` | `/var/log/ganesha.log` | Path to Ganesha log on the server |
| `--report-file` | _(stdout)_ | Write final report to this file |
| `-v` / `--verbose` | `False` | Enable DEBUG logging |

---

## Verdict Levels

| Verdict | Meaning |
|---------|---------|
| `PASS` | All checks passed |
| `WARNING` | Expected pressure events (high-watermark, futility) observed and correctly handled |
| `INCONCLUSIVE` | A required condition could not be exercised (e.g. high-watermark not reached) |
| `FAIL` | Incorrect or unrecovered behaviour detected |

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
python -m pytest nfs_ganesha_fd_lru_test/tests/test_fd_lru_framework.py -v 2>&1 |
```

---

## License

Apache-2.0

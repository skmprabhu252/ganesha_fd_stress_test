# scenarios/

Each file in this directory implements one test scenario that inherits from
`framework.runner.BaseScenario`. Scenarios are registered in `registry.py`
and selected via the `--scenario` CLI flag.

---

## `mode.py` — Run Modes

Defines workload scaling multipliers for each run mode.

| Mode   | Cycles | Thread ×  | File ×  | Burst  | Cooldown | Held-open | Retry timeout |
|--------|--------|-----------|---------|--------|----------|-----------|---------------|
| fast   | 1      | 1×        | 0.5×    | 20 s   | 30 s     | 5         | 10 s          |
| normal | 6      | 2×        | 2×      | 60 s   | 90 s     | 20        | 30 s          |
| soak   | 12     | 4×        | 4×      | 120 s  | 180 s    | 50        | 60 s          |

`RunMode.validate(mode)` normalises and validates the string.  
`ModeProfile.for_mode(mode)` returns the corresponding `ModeProfile` dataclass.

---

## `registry.py` — Scenario Registry

```python
ALL_SCENARIOS  # ordered list of all 4 scenario classes
SCENARIO_MAP   # {"TC01": TC01_Sanity, "TC02": TC02_NFSv3_Stress, ...}
get_scenario(scenario_id, config, mode)  # instantiate by ID
```

---

## TC01 — Sanity (`tc01_sanity.py`)

**Protocol**: `BOTH` (verifies both V3 and V4 mount paths exist)  
**Default mode**: `fast` (always uses the fast profile regardless of `--mode`)  
**Cycles**: always exactly 1

A lightweight environment validation run. Verifies:
- SSH connectivity to server and all client nodes
- `ganesha_stats inode` returns valid output
- Ganesha log is accessible
- NFS mounts work for both V3 and V4
- A minimal file-open/close workload completes without errors
- FD usage rises and settles back
- No Ganesha restart
- FD accounting is consistent (`total ≈ global + state + temp`)
- Held-open handles remain valid throughout

Intended as a CI gate before running TC02–TC04. A FAIL here means there is an
environment or configuration problem that must be fixed first.

---

## TC02 — NFSv3 Full Stress (`tc02_v3_stress.py`)

**Protocol**: `V3` (forced)

Exercises the complete NFSv3 FD/LRU lifecycle in a single scenario, consolidated
from the original TC01, TC04, TC07, TC10, TC11, TC12, TC16.

### Validation dimensions

| Area | Check |
|------|-------|
| Workload | V3 completion (< 1 % persistent failures); EMFILE vs EIO distinction; ESTALE rate |
| FD pressure | High-watermark detection + LRU wakeup; hard-limit + client retry recovery |
| LRU reclamation | ≥ 50 % peak reclaimed → PASS; 20–50 % → WARNING; < 20 % → FAIL |
| Futility | Correlated with no-reclamation + no-settle → FAIL |
| Cooldown | FD settles within window; no monotonic growth across cycles |
| Active handles | Held-open V3 files validated mid- and post-burst; ESTALE/EBADF → immediate FAIL |
| Server health | No restart; `ganesha_stats` always reachable; FD accounting identity |

### Pressure scaling

After baseline collection, `setup_pressure_config(fd_limit, num_clients)` scales
`threads_per_client` and `num_files` so that the estimated peak concurrent open
FD count reaches `config.target_fd_ratio × fd_limit` (default `0.95`).

```
peak_fds ≈ num_clients × threads × files_per_thread = target_fd_ratio × fd_limit
```

`post_cycle_hook()` scales `num_files` up by 25 % each cycle when the
high-watermark was not reached (INCONCLUSIVE), ensuring subsequent cycles
hit the target pressure. `fast` mode bypasses both scaling paths entirely.

Pass `--target-fd-ratio 1.50` on the CLI (mapped to
`config.target_fd_ratio`) to run a negative test that targets 150 % of the
server FD limit and exercises hard-limit / futility recovery.

---

## TC03 — NFSv4 Full Stress (`tc03_v4_stress.py`)

**Protocol**: `V4` (forced)

Consolidated from original TC02, TC05, TC08, TC10, TC11, TC12, TC13, TC14, TC16.

Same lifecycle as TC02 but targeting NFSv4 semantics:

- **LRU reclamation** is evaluated against `global_fd` only. State FDs are
  excluded — they are closed by the client `CLOSE` operation, not by the LRU
  thread. Futility validation is bypassed for the same reason.
- **`v4_state_fd_closure`** dimension: verifies that state FDs are released by
  clients after the burst workload finishes (≥ 80 % reduction).
- **`state_fd_pressure`** dimension: state FD pressure log events
  (`STATE_FD_PRESSURE`) are classified as WARNING/DIAGNOSTIC, not FAIL.
- Active V4 handle protection: open-state records on held-open handles must be
  preserved by Ganesha throughout the burst.
- Hard-limit recovery is more complex because state FDs also count against the
  total FD limit.

### Pressure scaling

Same algorithm as TC02 (`config.target_fd_ratio × fd_limit`). TC03 additionally
enforces a minimum `held_open_files` of 20 (to generate meaningful V4 state FD
pressure regardless of mode profile).

---

## TC04 — Mixed Stress (`tc04_mixed_stress.py`)

**Protocol**: `BOTH` (V3 + V4 concurrently)

Consolidated from original TC03, TC06, TC09, TC10, TC11, TC12, TC13, TC15, TC18.

Runs V3 and V4 workload workers **concurrently** on each client against separate
mount points (`<mount_point>/v3` and `<mount_point>/v4`).

### Why mixed is the most important test

- Both V3 and V4 contribute FD pressure simultaneously.
- The LRU must correctly reclaim reclaimable global FDs while preserving state
  FDs belonging to active V4 connections.
- Futility is more likely because FD open rate comes from two protocol paths at
  the same time.
- Active-handle protection must work for both V3 and V4 handles concurrently —
  neither type may be reclaimed.
- State FD pressure from V4 must not be misclassified as a global LRU failure
  when V3 global FDs are also high.
- The test must not PASS if one protocol silently fails while the other continues.

### Validation dimensions

Same dimensions as TC02 + TC03 combined, plus:

| Additional check | Description |
|-----------------|-------------|
| Both V3 and V4 workload completion | Validated independently — neither protocol may silently fail |
| Cross-protocol interference | Neither protocol may starve the other of FD budget |
| Dual-category FD classification | `global_fd` (V3/reclaimable) vs `state_fd` (V4/state) pressure correctly separated |
| Both V3 and V4 active handles | Held-open handles for both protocols validated mid-burst and post-burst |

### Pressure scaling

Because two workers run per client (`_WORKERS_PER_CLIENT = 2`), the total
worker count is `num_clients × 2`. The target is still
`config.target_fd_ratio × fd_limit`, distributed evenly across all
`total_workers`:

```
files_per_thread = target_fds // (total_workers × threads)
```

TC04 also enforces a minimum `held_open_files` of 20 to exercise both V3 and
V4 active-handle protection simultaneously.

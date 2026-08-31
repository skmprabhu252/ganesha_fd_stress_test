# scenarios/

Each file in this directory implements one test scenario that inherits from
`framework.runner.BaseScenario`. Scenarios are registered in `registry.py`
and selected via the `--scenario` CLI flag.

---

## `mode.py` — Run Modes

Defines workload scaling multipliers for each run mode.

| Mode   | Cycles | Thread ×  | File ×  | Burst  | Cooldown | Held-open |
|--------|--------|-----------|---------|--------|----------|-----------|
| fast   | 1      | 1×        | 0.5×    | 20 s   | 30 s     | 5         |
| normal | 6      | 2×        | 2×      | 60 s   | 90 s     | 20        |
| soak   | 12     | 4×        | 4×      | 120 s  | 180 s    | 50        |

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

**Protocol**: configurable (default: `BOTH`)

A lightweight environment validation run. Verifies:
- NFS mounts are accessible on all clients
- `ganesha_stats` is reachable and returns valid data
- A minimal file-open/close workload completes without errors
- Ganesha remains stable (no restart)

Intended as a CI gate before running the full stress scenarios.

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
After baseline collection, `setup_pressure_config()` scales `num_files` so that
`num_clients × threads × files_per_thread ≈ 95 %` of the live FD limit.  
`post_cycle_hook()` scales files up by 25 % each cycle when the high-watermark
was not reached (INCONCLUSIVE).

---

## TC03 — NFSv4 Full Stress (`tc03_v4_stress.py`)

**Protocol**: `V4` (forced)

Same lifecycle as TC02 but targeting NFSv4 semantics:
- LRU reclamation evaluated against `global_fd` only (state FDs are excluded —
  they are closed by the client CLOSE operation, not the LRU).
- Additional `v4_state_fd_closure` dimension: state FDs must be released by
  clients after the burst workload finishes.

---

## TC04 — Mixed Stress (`tc04_mixed_stress.py`)

**Protocol**: `BOTH` (V3 + V4 concurrently)

Runs V3 and V4 workload workers concurrently on each client against separate
mount points (`<mount_point>/v3` and `<mount_point>/v4`).

Validates:
- Both protocol paths under simultaneous FD pressure
- Correct separation of state FDs (V4) from reclaimable FDs (V3 + global V4)
- Active-handle protection across both protocol categories
- No cross-protocol interference in LRU reclamation

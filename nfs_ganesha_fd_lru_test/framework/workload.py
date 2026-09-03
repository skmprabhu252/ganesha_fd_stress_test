"""
NFS workload engine.

WorkloadWorker executes burst-phase filesystem operations on a locally
mounted NFS export.  It is designed to run on each participating client
(controller + workers) either in-process or launched remotely via SSH.

Key responsibilities
--------------------
- Open / create / read / write / close files across a configurable number
  of threads.
- Keep a subset of files intentionally held-open throughout the burst to
  simulate active/referenced handles.
- Validate held-open handles periodically (stale handle → functional failure).
- Accumulate per-thread counters and merge them into WorkloadStats.
- Support bounded retry on EMFILE / ENFILE / EIO / ENOENT.
- Distinguish client-side FD exhaustion (EMFILE) from server-side
  pressure (EIO, ENFILE, ESTALE).
"""

from __future__ import annotations

import errno
import json
import logging
import os
import pathlib
try:
    import resource
except ImportError:
    resource = None
import random
import string
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-thread / aggregate statistics
# ---------------------------------------------------------------------------

@dataclass
class WorkloadStats:
    opens_attempted: int  = 0
    opens_succeeded: int  = 0
    opens_failed: int     = 0
    opens_retried: int    = 0
    opens_eventually_ok: int = 0
    closes: int           = 0
    creates: int          = 0
    reads: int            = 0
    writes: int           = 0
    dir_ops: int          = 0
    active_handles: int   = 0
    active_handle_failures: int = 0
    # error breakdown
    emfile_count: int     = 0   # client-side FD exhaustion
    eio_count: int        = 0   # server-side I/O error
    estale_count: int     = 0   # stale NFS handle
    enfile_count: int     = 0   # system FD table full (server)
    other_errors: int     = 0

    def merge(self, other: "WorkloadStats") -> None:
        for f in self.__dataclass_fields__:
            setattr(self, f, getattr(self, f) + getattr(other, f))

    def as_dict(self) -> Dict[str, int]:
        return {f: getattr(self, f) for f in self.__dataclass_fields__}


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

def classify_oserror(exc: OSError) -> str:
    """Return a string category for an OSError."""
    code = exc.errno
    if code == errno.EMFILE:
        return "EMFILE"         # client process FD exhausted
    if code == errno.ENFILE:
        return "ENFILE"         # system FD table full
    if code == errno.EIO:
        return "EIO"            # generic I/O error (may be server-side)
    if code in (errno.ESTALE, 116):  # 116 = ESTALE on some kernels
        return "ESTALE"
    if code in (errno.EBADF,):
        return "EBADF"
    return "OTHER"


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------

def with_retry(
    fn,
    *,
    timeout_sec: float,
    interval_sec: float,
    retryable_categories: Tuple[str, ...] = ("EIO", "ENFILE"),
    stats: Optional[WorkloadStats] = None,
):
    """
    Call fn(); on retryable OSError retry until timeout_sec is exceeded.

    Returns the function's return value on success.
    Raises OSError on final failure.
    """
    deadline = time.monotonic() + timeout_sec
    attempts = 0
    last_exc: Optional[OSError] = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if attempts > 0 and stats is not None:
                stats.opens_eventually_ok += 1
            return result
        except OSError as exc:
            cat = classify_oserror(exc)
            if cat not in retryable_categories:
                raise
            last_exc = exc
            attempts += 1
            if stats is not None:
                stats.opens_retried += 1
            time.sleep(interval_sec)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Held-open handle validator
# ---------------------------------------------------------------------------

class HeldHandle:
    """Wraps an open file descriptor that must remain valid throughout the burst."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fd: Optional[int] = None
        self._failed = False

    def open(self) -> bool:
        try:
            self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
            # Write a sentinel
            os.write(self._fd, b"HELD\n")
            return True
        except OSError as exc:
            logger.error("HeldHandle open failed %s: %s", self.path, exc)
            self._failed = True
            return False

    def validate(self, stats: WorkloadStats) -> bool:
        """Attempt a read; return False and record failure on stale/bad FD."""
        if self._fd is None or self._failed:
            return False
        try:
            os.lseek(self._fd, 0, os.SEEK_SET)
            data = os.read(self._fd, 8)
            return True
        except OSError as exc:
            cat = classify_oserror(exc)
            logger.error(
                "HeldHandle validation FAILED path=%s error=%s(%s)",
                self.path, cat, exc.errno,
            )
            stats.active_handle_failures += 1
            self._failed = True
            return False

    def close(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

    @property
    def is_valid(self) -> bool:
        return self._fd is not None and not self._failed


# ---------------------------------------------------------------------------
# Worker thread body
# ---------------------------------------------------------------------------

def _random_name(prefix: str = "f", length: int = 8) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase, k=length))


def _run_thread(
    *,
    thread_id: int,
    mount_point: str,
    subdir: str,
    stop_event: threading.Event,
    stats: WorkloadStats,
    num_files: int,
    file_size: int,
    retry_timeout: float,
    retry_interval: float,
    protocol: str = "v3",
) -> None:
    """
    Body of a single workload thread.

    Continuously creates, opens, reads, writes, and closes files under
    *mount_point*/*subdir* until *stop_event* is set.
    """
    base = pathlib.Path(mount_point) / subdir
    base.mkdir(parents=True, exist_ok=True)
    stats.dir_ops += 1

    payload = os.urandom(min(file_size, 65536))
    file_names = [str(base / _random_name()) for _ in range(num_files)]

    # Pre-create files
    for fname in file_names:
        try:
            with open(fname, "wb") as fh:
                fh.write(payload)
            stats.creates += 1
        except OSError as exc:
            stats.opens_failed += 1
            cat = classify_oserror(exc)
            _bump_error_counter(stats, cat)

    open_fhs = {}
    is_v4 = protocol.lower() == "v4"

    while not stop_event.is_set():
        fname = random.choice(file_names)
        fh = None
        try:
            if is_v4 and fname in open_fhs:
                fh = open_fhs[fname]
            else:
                stats.opens_attempted += 1
                def _open():
                    return open(fname, "r+b")

                try:
                    fh = with_retry(
                        _open,
                        timeout_sec=retry_timeout,
                        interval_sec=retry_interval,
                        stats=stats,
                    )
                except OSError as exc:
                    if exc.errno == errno.EMFILE and is_v4 and open_fhs:
                        # Cache eviction: close the oldest open file descriptor to free a slot on the client
                        oldest_name, oldest_fh = next(iter(open_fhs.items()))
                        try:
                            oldest_fh.close()
                            stats.closes += 1
                        except OSError:
                            pass
                        del open_fhs[oldest_name]
                        # Retry the open operation exactly once
                        fh = with_retry(
                            _open,
                            timeout_sec=retry_timeout,
                            interval_sec=retry_interval,
                            stats=stats,
                        )
                    else:
                        raise

                stats.opens_succeeded += 1
                if is_v4:
                    open_fhs[fname] = fh

            # read
            fh.seek(0)
            _ = fh.read(min(file_size, 4096))
            stats.reads += 1

            # write
            fh.seek(0)
            fh.write(payload[: min(file_size, 64)])
            stats.writes += 1

        except OSError as exc:
            stats.opens_failed += 1
            cat = classify_oserror(exc)
            _bump_error_counter(stats, cat)
            logger.debug("thread %d open error %s: %s", thread_id, cat, exc)
            if is_v4 and fname in open_fhs:
                try:
                    open_fhs[fname].close()
                except OSError:
                    pass
                del open_fhs[fname]
        finally:
            if not is_v4 and fh is not None:
                try:
                    fh.close()
                    stats.closes += 1
                except OSError:
                    pass

    if is_v4:
        for fh in open_fhs.values():
            try:
                fh.close()
                stats.closes += 1
            except OSError:
                pass


def _bump_error_counter(stats: WorkloadStats, category: str) -> None:
    if category == "EMFILE":
        stats.emfile_count += 1
    elif category == "EIO":
        stats.eio_count += 1
    elif category == "ESTALE":
        stats.estale_count += 1
    elif category == "ENFILE":
        stats.enfile_count += 1
    else:
        stats.other_errors += 1


# ---------------------------------------------------------------------------
# Self-contained remote worker script
# ---------------------------------------------------------------------------
#
# This script is deployed to each client node via SCP and executed via SSH.
# It accepts a JSON configuration on stdin and writes a JSON WorkloadStats
# dict to stdout when the burst completes.
# It has NO imports from the framework — only stdlib.

_REMOTE_WORKER_SCRIPT = r'''#!/usr/bin/env python3
"""Self-contained NFS burst worker.  Reads JSON config from stdin, writes JSON stats to stdout."""
import errno, json, os, pathlib, random, string, sys, threading, time
try:
    import resource
except ImportError:
    resource = None

def _random_name(prefix="f", length=8):
    return prefix + "".join(random.choices(string.ascii_lowercase, k=length))

def _classify(exc):
    c = exc.errno
    if c == errno.EMFILE:  return "EMFILE"
    if c == errno.ENFILE:  return "ENFILE"
    if c == errno.EIO:     return "EIO"
    if c in (errno.ESTALE, 116): return "ESTALE"
    return "OTHER"

def _run_thread(tid, mount_point, subdir, stop_event, stats, num_files, file_size, retry_timeout, retry_interval, protocol="v3"):
    base = pathlib.Path(mount_point) / subdir
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        # Cannot create working directory — mount is not accessible.
        # Record the error and exit immediately; spinning open() calls on
        # non-existent files would just produce millions of ENOENT failures.
        stats["opens_failed"] += 1
        _bump(stats, _classify(exc))
        return
    stats["dir_ops"] += 1
    payload = os.urandom(min(file_size, 65536))
    file_names = [str(base / _random_name()) for _ in range(num_files)]
    create_failures = 0
    for fname in file_names:
        try:
            with open(fname, "wb") as fh:
                fh.write(payload)
            stats["creates"] += 1
        except OSError as exc:
            create_failures += 1
            stats["opens_failed"] += 1
            _bump(stats, _classify(exc))
    if create_failures == num_files:
        # Every pre-creation failed — mount is inaccessible or path is wrong.
        # Abort rather than spin-looping on ENOENT for the entire burst duration.
        return
    # Only loop on files that were successfully created
    file_names = [f for f in file_names
                  if os.path.exists(f)]  # noqa: PTH110
    if not file_names:
        return
    open_fhs = {}
    is_v4 = protocol.lower() == "v4"
    while not stop_event.is_set():
        fname = random.choice(file_names)
        fh = None
        try:
            if is_v4 and fname in open_fhs:
                fh = open_fhs[fname]
            else:
                stats["opens_attempted"] += 1
                deadline = time.monotonic() + retry_timeout
                attempts = 0
                last_exc = None
                while time.monotonic() < deadline:
                    try:
                        fh = open(fname, "r+b"); break
                    except OSError as exc:
                        cat = _classify(exc)
                        if cat == "EMFILE" and is_v4 and open_fhs:
                            # Cache eviction: close oldest to free slot and retry immediately
                            oldest_name, oldest_fh = next(iter(open_fhs.items()))
                            try: oldest_fh.close(); stats["closes"] += 1
                            except OSError: pass
                            del open_fhs[oldest_name]
                            continue
                        if cat not in ("EIO", "ENFILE"):
                            raise
                        last_exc = exc; attempts += 1
                        stats["opens_retried"] += 1
                        time.sleep(retry_interval)
                else:
                    raise last_exc
                if attempts > 0:
                    stats["opens_eventually_ok"] += 1
                stats["opens_succeeded"] += 1
                if is_v4:
                    open_fhs[fname] = fh
            fh.seek(0); _ = fh.read(min(file_size, 4096)); stats["reads"] += 1
            fh.seek(0); fh.write(payload[:min(file_size, 64)]); stats["writes"] += 1
        except OSError as exc:
            stats["opens_failed"] += 1
            _bump(stats, _classify(exc))
            if is_v4 and fname in open_fhs:
                try: open_fhs[fname].close()
                except OSError: pass
                del open_fhs[fname]
        finally:
            if not is_v4 and fh is not None:
                try: fh.close(); stats["closes"] += 1
                except OSError: pass
    if is_v4:
        for fh in open_fhs.values():
            try: fh.close(); stats["closes"] += 1
            except OSError: pass

def _bump(stats, cat):
    if cat == "EMFILE":   stats["emfile_count"] += 1
    elif cat == "EIO":    stats["eio_count"] += 1
    elif cat == "ESTALE": stats["estale_count"] += 1
    elif cat == "ENFILE": stats["enfile_count"] += 1
    else:                 stats["other_errors"] += 1

def main():
    if resource is not None:
        try:
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        except Exception:
            pass

    cfg = json.load(sys.stdin)
    mount_point      = cfg["mount_point"]
    num_threads      = cfg["num_threads"]
    num_files        = cfg["num_files"]
    file_size        = cfg["file_size_bytes"]
    held_open_count  = cfg["held_open_count"]
    burst_duration   = cfg["burst_duration_sec"]
    retry_timeout    = cfg["retry_timeout_sec"]
    retry_interval   = cfg["retry_interval_sec"]
    protocol         = cfg["protocol"]
    # client_id makes every client write to a unique server-side path so Ganesha
    # opens a distinct FD per client instead of sharing one global FD.
    client_id        = cfg.get("client_id", "")

    agg = {k: 0 for k in (
        "opens_attempted","opens_succeeded","opens_failed","opens_retried",
        "opens_eventually_ok","closes","creates","reads","writes","dir_ops",
        "active_handles","active_handle_failures",
        "emfile_count","eio_count","estale_count","enfile_count","other_errors",
    )}

    # Open held handles in a client-unique subdirectory
    held_subdir = f"_held_open_{client_id}" if client_id else "_held_open"
    held_dir = pathlib.Path(mount_point) / held_subdir
    held_dir.mkdir(parents=True, exist_ok=True)
    held_fds = []
    for i in range(held_open_count):
        path = str(held_dir / f"held_{i:04d}.dat")
        try:
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
            os.write(fd, b"HELD\n")
            held_fds.append((fd, path))
        except OSError:
            agg["active_handle_failures"] += 1
    agg["active_handles"] = len(held_fds)

    stop_event = threading.Event()
    thread_stats = [{k: 0 for k in agg} for _ in range(num_threads)]
    threads = []
    for tid in range(num_threads):
        # Embed client_id in the subdirectory so every client opens distinct files
        subdir = (f"client_{client_id}_thread_{tid:03d}_{protocol}"
                  if client_id else f"thread_{tid:03d}_{protocol}")
        t = threading.Thread(
            target=_run_thread,
            kwargs=dict(tid=tid, mount_point=mount_point, subdir=subdir,
                        stop_event=stop_event, stats=thread_stats[tid],
                        num_files=num_files, file_size=file_size,
                        retry_timeout=retry_timeout, retry_interval=retry_interval,
                        protocol=protocol),
            daemon=True,
        )
        t.start(); threads.append(t)

    # Mid-burst held-handle validation
    half = burst_duration / 2.0
    time.sleep(half)
    for fd, path in held_fds:
        try:
            os.lseek(fd, 0, os.SEEK_SET); os.read(fd, 8)
        except OSError:
            agg["active_handle_failures"] += 1
    time.sleep(half)

    stop_event.set()
    for t in threads:
        t.join(timeout=max(retry_timeout + 5, 10))

    # Post-burst held-handle validation
    for fd, path in held_fds:
        try:
            os.lseek(fd, 0, os.SEEK_SET); os.read(fd, 8)
        except OSError:
            agg["active_handle_failures"] += 1

    # Merge thread stats
    for ts in thread_stats:
        for k in agg:
            agg[k] += ts[k]
    agg["active_handles"] = len(held_fds)

    # Close held handles
    for fd, _ in held_fds:
        try: os.close(fd)
        except OSError: pass

    print(json.dumps(agg))

if __name__ == "__main__":
    main()
'''


# ---------------------------------------------------------------------------
# Workload controller
# ---------------------------------------------------------------------------

class WorkloadWorker:
    """
    Manages the burst lifecycle on a single client.

    Parameters
    ----------
    mount_point:
        Locally accessible NFS mount.
    num_threads:
        Concurrent workload threads.
    num_files:
        Files per thread.
    file_size_bytes:
        Bytes to write per file.
    held_open_count:
        Number of files to keep open throughout the burst.
    burst_duration_sec:
        How long the burst runs.
    retry_timeout_sec / retry_interval_sec:
        Retry parameters for transient errors.
    """

    def __init__(
        self,
        *,
        mount_point: str,
        num_threads: int = 8,
        num_files: int = 500,
        file_size_bytes: int = 4096,
        held_open_count: int = 20,
        burst_duration_sec: float = 60.0,
        retry_timeout_sec: float = 30.0,
        retry_interval_sec: float = 2.0,
        protocol: str = "V3",
        client_id: str = "",
    ) -> None:
        self.mount_point = mount_point
        self.num_threads = num_threads
        self.num_files = num_files
        self.file_size_bytes = file_size_bytes
        self.held_open_count = held_open_count
        self.burst_duration_sec = burst_duration_sec
        self.retry_timeout_sec = retry_timeout_sec
        self.retry_interval_sec = retry_interval_sec
        self.protocol = protocol
        # Per-client identifier embedded in subdirectory names so that every
        # client writes to a distinct path on the NFS server.  Without this,
        # two clients opening the same filename cause Ganesha to reuse a single
        # global FD rather than opening a separate FD for each client, which
        # prevents the test from reaching the high-watermark.
        self.client_id = client_id or ""

        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._thread_stats: List[WorkloadStats] = []
        self._held_handles: List[HeldHandle] = []
        self.aggregate_stats = WorkloadStats()

    # ------------------------------------------------------------------
    # Held handles
    # ------------------------------------------------------------------

    def _open_held_handles(self) -> None:
        # Use a client-specific subdirectory so held-open files are unique per client.
        held_subdir = f"_held_open_{self.client_id}" if self.client_id else "_held_open"
        held_dir = pathlib.Path(self.mount_point) / held_subdir
        held_dir.mkdir(parents=True, exist_ok=True)
        for i in range(self.held_open_count):
            path = str(held_dir / f"held_{i:04d}.dat")
            h = HeldHandle(path)
            if h.open():
                self._held_handles.append(h)
            else:
                logger.warning("Could not open held handle #%d: %s", i, path)
        self.aggregate_stats.active_handles = len(self._held_handles)

    def _validate_held_handles(self) -> int:
        """Return number of failed validations."""
        failures = 0
        for h in self._held_handles:
            if not h.validate(self.aggregate_stats):
                failures += 1
        return failures

    def _close_held_handles(self) -> None:
        for h in self._held_handles:
            h.close()
        self._held_handles.clear()

    # ------------------------------------------------------------------
    # Burst lifecycle
    # ------------------------------------------------------------------

    def run_burst(self) -> WorkloadStats:
        """
        Execute one burst cycle.

        1. Open held handles.
        """
        if resource is not None:
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            except Exception:
                pass

        """
        2. Launch worker threads.
        3. Let workload run for burst_duration_sec.
        4. Validate held handles.
        5. Stop threads.
        6. Close held handles.
        7. Return merged stats.
        """
        self._stop_event.clear()
        self._threads.clear()
        self._thread_stats.clear()
        self.aggregate_stats = WorkloadStats()

        # Open held handles
        self._open_held_handles()

        # Launch threads
        for tid in range(self.num_threads):
            ts = WorkloadStats()
            self._thread_stats.append(ts)
            # Embed client_id in the subdirectory so every client writes to a
            # unique path on the server — Ganesha must open a distinct FD per
            # client rather than sharing one global FD for the same filename.
            subdir = (
                f"client_{self.client_id}_thread_{tid:03d}_{self.protocol}"
                if self.client_id
                else f"thread_{tid:03d}_{self.protocol}"
            )
            t = threading.Thread(
                target=_run_thread,
                kwargs=dict(
                    thread_id=tid,
                    mount_point=self.mount_point,
                    subdir=subdir,
                    stop_event=self._stop_event,
                    stats=ts,
                    num_files=self.num_files,
                    file_size=self.file_size_bytes,
                    retry_timeout=self.retry_timeout_sec,
                    retry_interval=self.retry_interval_sec,
                    protocol=self.protocol,
                ),
                daemon=True,
                name=f"workload-{tid}",
            )
            t.start()
            self._threads.append(t)

        logger.info("Burst started: %d threads, duration=%.0fs", self.num_threads, self.burst_duration_sec)

        # Run for burst_duration_sec, validating held handles mid-way
        half_way = self.burst_duration_sec / 2.0
        time.sleep(half_way)
        held_failures_mid = self._validate_held_handles()
        if held_failures_mid:
            logger.error("HELD HANDLE FAILURE MID-BURST: %d handles failed", held_failures_mid)
        time.sleep(half_way)

        # Stop threads
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=max(self.retry_timeout_sec + 5, 10))

        # Validate held handles after burst
        held_failures_post = self._validate_held_handles()
        if held_failures_post:
            logger.error("HELD HANDLE FAILURE POST-BURST: %d handles failed", held_failures_post)

        # Merge stats
        for ts in self._thread_stats:
            self.aggregate_stats.merge(ts)
        self.aggregate_stats.active_handles = len(self._held_handles)

        # Close held handles
        self._close_held_handles()

        return self.aggregate_stats

    def run_remote_burst(self, ssh, host: str) -> "WorkloadStats":
        """
        Deploy the worker script to *host* via SCP and execute it via SSH.

        The script runs the full burst on the remote client against its
        locally mounted NFS share (mount_point must already be mounted).
        Returns a WorkloadStats populated from the JSON the script prints
        to stdout.

        Parameters
        ----------
        ssh:
            An :class:`~.ssh_client.SSHClient` configured for *host*.
        host:
            Remote client IP / hostname.
        """
        cfg = {
            "mount_point":      self.mount_point,
            "num_threads":      self.num_threads,
            "num_files":        self.num_files,
            "file_size_bytes":  self.file_size_bytes,
            "held_open_count":  self.held_open_count,
            "burst_duration_sec":  self.burst_duration_sec,
            "retry_timeout_sec":   self.retry_timeout_sec,
            "retry_interval_sec":  self.retry_interval_sec,
            "protocol":            self.protocol,
            "client_id":           self.client_id,
        }
        cfg_json = json.dumps(cfg)

        # Write both the worker script and its config to local temp files,
        # then SCP both to the client.  Using a config file avoids all shell
        # quoting problems (mount paths with spaces, special chars, etc.).
        remote_script = "/tmp/_fd_lru_worker.py"
        remote_cfg    = "/tmp/_fd_lru_cfg.json"

        local_script = local_cfg = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False
            ) as tmp:
                tmp.write(_REMOTE_WORKER_SCRIPT)
                local_script = tmp.name

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as tmp:
                tmp.write(cfg_json)
                local_cfg = tmp.name

            for local, remote in ((local_script, remote_script),
                                   (local_cfg,    remote_cfg)):
                scp_result = ssh.copy_to_remote(local, host, remote, timeout=30)
                if not scp_result.ok:
                    raise RuntimeError(
                        f"Failed to SCP {remote} to {host}: {scp_result.stderr}"
                    )
        finally:
            for f in (local_script, local_cfg):
                if f:
                    try:
                        os.unlink(f)
                    except OSError:
                        pass

        # Run the script: feed config via stdin redirection from the config file
        ssh_timeout = self.burst_duration_sec + self.retry_timeout_sec + 60
        cmd = f"python3 {remote_script} < {remote_cfg}"
        result = ssh.run_remote(host, cmd, timeout=ssh_timeout)

        # Log stderr from the remote worker so failures are visible
        if result.stderr:
            logger.warning(
                "Remote worker stderr from %s:\n%s", host, result.stderr
            )

        if not result.ok:
            raise RuntimeError(
                f"Remote worker failed on {host} (rc={result.returncode}): "
                f"{result.stderr or '(no stderr)'}"
            )

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Could not parse remote worker output from {host}: {exc}\n"
                f"stdout={result.stdout!r}"
            ) from exc

        stats = WorkloadStats()
        for field_name in stats.__dataclass_fields__:
            if field_name in data:
                setattr(stats, field_name, data[field_name])
        return stats

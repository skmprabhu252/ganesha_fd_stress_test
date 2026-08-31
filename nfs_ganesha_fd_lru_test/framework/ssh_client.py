"""
SSH transport helper used throughout the test framework.

Wraps subprocess-based SSH execution and provides:
  - run_remote()        synchronous remote command
  - copy_to_remote()    scp a local file to a remote host
  - check_reachable()   lightweight connectivity probe
"""

from __future__ import annotations

import shlex
import subprocess
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class RemoteResult:
    command: str
    host: str
    returncode: int
    stdout: str
    stderr: str
    elapsed_sec: float = 0.0

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def __repr__(self) -> str:  # pragma: no cover
        status = "OK" if self.ok else f"rc={self.returncode}"
        return (
            f"RemoteResult({status}, host={self.host!r}, "
            f"cmd={self.command!r}, elapsed={self.elapsed_sec:.2f}s)"
        )


# ---------------------------------------------------------------------------
# SSH helper
# ---------------------------------------------------------------------------

class SSHClient:
    """
    Thin subprocess-based SSH wrapper.

    Parameters
    ----------
    user:
        SSH login user (e.g. "root").
    ssh_opts:
        Extra SSH options string (e.g. ``-o StrictHostKeyChecking=no``).
    identity_file:
        Path to a private key file (``-i /path/to/key``).  When the
        orchestrator node carries a non-default key (e.g. the OpenStack
        tenant key) set this so every SSH/SCP call uses it automatically.
    timeout:
        Per-command timeout in seconds.  None = no timeout.
    """

    def __init__(
        self,
        user: str = "root",
        ssh_opts: str = "-o StrictHostKeyChecking=no -o BatchMode=yes",
        identity_file: str = "",
        timeout: Optional[float] = 60.0,
    ) -> None:
        self.user = user
        self.ssh_opts = ssh_opts
        self.identity_file = identity_file
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _identity_args(self) -> List[str]:
        return ["-i", self.identity_file] if self.identity_file else []

    def _build_ssh_argv(self, host: str, remote_cmd: str) -> List[str]:
        return (
            ["ssh"]
            + shlex.split(self.ssh_opts)
            + self._identity_args()
            + [f"{self.user}@{host}", remote_cmd]
        )

    def _build_scp_argv(self, local_path: str, host: str, remote_path: str) -> List[str]:
        return (
            ["scp"]
            + shlex.split(self.ssh_opts)
            + self._identity_args()
            + [local_path, f"{self.user}@{host}:{remote_path}"]
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_remote(
        self,
        host: str,
        command: str,
        timeout: Optional[float] = None,
        env: Optional[dict] = None,
    ) -> RemoteResult:
        """Execute *command* on *host* via SSH.  Returns a RemoteResult."""
        argv = self._build_ssh_argv(host, command)
        effective_timeout = timeout if timeout is not None else self.timeout
        t0 = time.monotonic()
        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                env=env,
            )
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=command,
                host=host,
                returncode=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
                elapsed_sec=elapsed,
            )
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=command,
                host=host,
                returncode=-1,
                stdout="",
                stderr=f"SSH command timed out after {effective_timeout}s",
                elapsed_sec=elapsed,
            )
        except Exception as exc:  # pylint: disable=broad-except
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=command,
                host=host,
                returncode=-2,
                stdout="",
                stderr=str(exc),
                elapsed_sec=elapsed,
            )

    def copy_to_remote(
        self,
        local_path: str,
        host: str,
        remote_path: str,
        timeout: Optional[float] = None,
    ) -> RemoteResult:
        """SCP *local_path* to *host*:*remote_path*."""
        argv = self._build_scp_argv(local_path, host, remote_path)
        effective_timeout = timeout if timeout is not None else self.timeout
        t0 = time.monotonic()
        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
            )
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=" ".join(argv),
                host=host,
                returncode=proc.returncode,
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
                elapsed_sec=elapsed,
            )
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=" ".join(argv),
                host=host,
                returncode=-1,
                stdout="",
                stderr=f"SCP timed out after {effective_timeout}s",
                elapsed_sec=elapsed,
            )
        except Exception as exc:  # pylint: disable=broad-except
            elapsed = time.monotonic() - t0
            return RemoteResult(
                command=" ".join(argv),
                host=host,
                returncode=-2,
                stdout="",
                stderr=str(exc),
                elapsed_sec=elapsed,
            )

    def check_reachable(self, host: str, timeout: float = 10.0) -> bool:
        """Return True if we can execute a simple 'true' on *host*."""
        result = self.run_remote(host, "true", timeout=timeout)
        return result.ok

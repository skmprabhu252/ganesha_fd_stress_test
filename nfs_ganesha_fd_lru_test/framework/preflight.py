"""
Environment pre-flight validation.

Checks that all mandatory preconditions are met before any stress
workload is generated.  A failed preflight raises PreflightError.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from .config import ClientConfig, ProtocolMode, ServerConfig, TestConfig
from .ssh_client import SSHClient

logger = logging.getLogger(__name__)


class PreflightError(RuntimeError):
    """Raised when a mandatory preflight check fails."""


@dataclass
class PreflightReport:
    checks: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    def ok(self, msg: str) -> None:
        self.checks.append(f"  [OK]   {msg}")
        logger.debug("preflight OK: %s", msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(f"  [WARN] {msg}")
        logger.warning("preflight WARN: %s", msg)

    def fail(self, msg: str) -> None:
        self.errors.append(f"  [FAIL] {msg}")
        logger.error("preflight FAIL: %s", msg)

    def summary(self) -> str:
        lines = ["=== Pre-flight Report ==="]
        lines += self.checks
        lines += self.warnings
        lines += self.errors
        status = "PASSED" if self.passed else f"FAILED ({len(self.errors)} error(s))"
        lines.append(f"\nPre-flight status: {status}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_server_ssh(ssh: SSHClient, server: ServerConfig, report: PreflightReport) -> bool:
    """Verify SSH connectivity to the Ganesha server management address."""
    if ssh.check_reachable(server.ssh_host):
        label = f"{server.ssh_host}" + (
            f" (VIP={server.address})" if server.ssh_host != server.address else ""
        )
        report.ok(f"SSH reachable: {label}")
        return True
    report.fail(
        f"Cannot SSH to Ganesha server: {server.ssh_host}"
        + (f" (VIP={server.address})" if server.ssh_host != server.address else "")
    )
    return False


def _check_ganesha_stats(ssh: SSHClient, server: ServerConfig, report: PreflightReport) -> bool:
    """Verify ganesha_stats inode is available and returns valid output."""
    result = ssh.run_remote(server.ssh_host, server.ganesha_stats_cmd, timeout=15)
    if not result.ok:
        report.fail(
            f"ganesha_stats inode failed on {server.ssh_host}: {result.stderr}"
        )
        return False
    # Minimal sanity: output must mention "FD" or "fd" or "inode"
    combined = result.stdout + result.stderr
    if not any(kw in combined.lower() for kw in ("fd", "inode", "lru")):
        report.fail(
            f"ganesha_stats output on {server.ssh_host} does not look like "
            f"inode stats: {combined[:200]!r}"
        )
        return False

    # Log the raw output at DEBUG level so operators can inspect field names
    # when troubleshooting.  INFO would clutter normal runs.
    logger.debug(
        "ganesha_stats raw output from %s:\n%s",
        server.ssh_host,
        result.stdout or "(empty stdout)",
    )

    report.ok(f"ganesha_stats inode returned valid output on {server.ssh_host}")
    return True


def _check_ganesha_log(ssh: SSHClient, server: ServerConfig, report: PreflightReport) -> None:
    """Verify the Ganesha log file exists and is readable."""
    result = ssh.run_remote(
        server.ssh_host, f"test -r {server.ganesha_log_path} && echo readable", timeout=10
    )
    if result.ok and "readable" in result.stdout:
        report.ok(f"Ganesha log readable: {server.ganesha_log_path}")
    else:
        report.warn(
            f"Ganesha log not accessible at {server.ganesha_log_path} — "
            "log-based validation will be limited"
        )


def _check_client_ssh(ssh: SSHClient, client: ClientConfig, report: PreflightReport) -> bool:
    """Verify SSH connectivity to a worker client."""
    if ssh.check_reachable(client.address):
        report.ok(f"SSH reachable: {client.address} ({client.role})")
        return True
    report.fail(f"Cannot SSH to client: {client.address} ({client.role})")
    return False


def _check_client_python(ssh: SSHClient, client: ClientConfig, report: PreflightReport) -> None:
    """Verify Python 3 runtime is available on client."""
    for cmd in ("python3 --version", "python --version"):
        res = ssh.run_remote(client.address, cmd, timeout=10)
        if res.ok and "Python 3" in res.stdout + res.stderr:
            report.ok(f"Python 3 available on {client.address}")
            return
    report.warn(f"Python 3 not confirmed on {client.address}")


def _check_nfs_capability(
    ssh: SSHClient,
    client: ClientConfig,
    server: ServerConfig,
    protocol: str,
    report: PreflightReport,
) -> None:
    """
    Verify that the required NFS protocol can actually mount from the client.

    Uses a temporary mount attempt rather than assuming it will work.
    """
    needs_v3 = protocol in (ProtocolMode.V3, ProtocolMode.BOTH)
    needs_v4 = protocol in (ProtocolMode.V4, ProtocolMode.BOTH)

    tmp_mount = f"/tmp/_ganesha_preflight_mount_{client.address.replace('.', '_')}"

    def _try_mount(ver: str) -> bool:
        mnt_cmd = (
            f"mkdir -p {tmp_mount} && "
            f"mount -t nfs -o vers={ver},ro {server.address}:{server.nfs_export} {tmp_mount} && "
            f"echo mounted && "
            f"umount {tmp_mount} 2>/dev/null; "
            f"rmdir {tmp_mount} 2>/dev/null"
        )
        res = ssh.run_remote(client.address, mnt_cmd, timeout=30)
        return res.ok and "mounted" in res.stdout

    if needs_v3:
        if _try_mount("3"):
            report.ok(f"NFSv3 mount verified from {client.address}")
        else:
            report.fail(f"NFSv3 mount failed from {client.address} to {server.address}:{server.nfs_export}")

    if needs_v4:
        if _try_mount("4"):
            report.ok(f"NFSv4 mount verified from {client.address}")
        else:
            report.fail(f"NFSv4 mount failed from {client.address} to {server.address}:{server.nfs_export}")


def _check_client_tools(ssh: SSHClient, client: ClientConfig, report: PreflightReport) -> None:
    """Verify common tools are available on a client."""
    for tool in ("mount", "umount", "df", "stat"):
        res = ssh.run_remote(client.address, f"which {tool} || command -v {tool}", timeout=10)
        if res.ok:
            report.ok(f"{tool} available on {client.address}")
        else:
            report.warn(f"{tool} not found on {client.address}")


# ---------------------------------------------------------------------------
# Main preflight runner
# ---------------------------------------------------------------------------

def run_preflight(config: TestConfig, ssh: Optional[SSHClient] = None) -> PreflightReport:
    """
    Run all pre-flight checks.

    Returns a :class:`PreflightReport`.  Does not raise; callers should
    inspect ``report.passed``.
    """
    if ssh is None:
        ssh = SSHClient(
            user=config.server.ssh_user,
            ssh_opts=config.server.ssh_opts,
            identity_file=config.server.identity_file,
        )

    report = PreflightReport()

    # Config self-validation
    try:
        config.validate()
        report.ok("Configuration validation passed")
    except (ValueError, TypeError) as exc:
        report.fail(f"Configuration validation failed: {exc}")
        return report   # cannot proceed without valid config

    # Server checks
    server_reachable = _check_server_ssh(ssh, config.server, report)
    if server_reachable:
        _check_ganesha_stats(ssh, config.server, report)
        _check_ganesha_log(ssh, config.server, report)

    # Client checks
    for client in config.clients:
        client_ssh = SSHClient(
            user=client.ssh_user,
            ssh_opts=client.ssh_opts,
            identity_file=client.identity_file,
        )
        client_reachable = _check_client_ssh(client_ssh, client, report)
        if client_reachable:
            _check_client_python(client_ssh, client, report)
            _check_client_tools(client_ssh, client, report)
            if server_reachable:
                _check_nfs_capability(
                    client_ssh, client, config.server, config.protocol, report
                )

    return report

"""
Configuration dataclasses and validation for the NFS-Ganesha FD/LRU test framework.
"""

from __future__ import annotations

import dataclasses
import re
from typing import List, Optional


# ---------------------------------------------------------------------------
# Protocol mode
# ---------------------------------------------------------------------------

class ProtocolMode:
    V3   = "V3"
    V4   = "V4"
    BOTH = "BOTH"
    _VALID = {V3, V4, BOTH}

    @classmethod
    def validate(cls, value: str) -> str:
        v = value.upper()
        if v not in cls._VALID:
            raise ValueError(
                f"Invalid protocol mode '{value}'. Must be one of: "
                f"{', '.join(sorted(cls._VALID))}"
            )
        return v


# ---------------------------------------------------------------------------
# Server config
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ServerConfig:
    address: str                         # VIP / floating-IP used for NFS mounts
    ssh_user: str = "root"
    ssh_opts: str = "-o StrictHostKeyChecking=no -o BatchMode=yes"
    nfs_export: str = "/export"
    ganesha_log_path: str = "/var/log/ganesha.log"
    node_id: str = ""                    # optional Ganesha node ID
    ganesha_stats_cmd: str = "ganesha_stats inode"
    # Physical node address used for SSH management access.
    # When running from an OpenStack orchestrator node the Ganesha server is
    # reached by its real IP/hostname for SSH while the VIP (address) is only
    # used for NFS mounts.  Defaults to address when not set.
    ssh_address: str = ""
    # Path to the SSH private key on the orchestrator node.
    # Set this when the key is not the default ~/.ssh/id_rsa, e.g. the
    # OpenStack tenant key downloaded as ~/.ssh/openstack.pem.
    identity_file: str = ""

    @property
    def ssh_host(self) -> str:
        """The host to SSH into for server management (stats, logs, etc.)."""
        return self.ssh_address if self.ssh_address else self.address

    def validate(self) -> None:
        if not self.address:
            raise ValueError("server.address must not be empty")
        if not self.nfs_export.startswith("/"):
            raise ValueError("server.nfs_export must be an absolute path")
        if not self.ssh_user:
            raise ValueError("server.ssh_user must not be empty")


# ---------------------------------------------------------------------------
# Client config
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ClientConfig:
    address: str
    ssh_user: str = "root"
    ssh_opts: str = "-o StrictHostKeyChecking=no -o BatchMode=yes"
    mount_point: str = "/mnt/fd_stress"
    role: str = "worker"                 # "controller" | "worker"
    # Inherited from the global --ssh-key when set; can be overridden per client.
    identity_file: str = ""

    def validate(self) -> None:
        if not self.address:
            raise ValueError("client.address must not be empty")
        if not self.mount_point.startswith("/"):
            raise ValueError("client.mount_point must be an absolute path")
        if self.role not in ("controller", "worker"):
            raise ValueError(f"client.role must be 'controller' or 'worker', got '{self.role}'")


# ---------------------------------------------------------------------------
# Workload config
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class WorkloadConfig:
    threads_per_client: int  = 8
    num_files: int           = 500
    file_size_bytes: int     = 4096
    num_directories: int     = 10
    files_per_directory: int = 50
    held_open_files: int     = 20        # files kept open during burst
    burst_duration_sec: int  = 60
    cooldown_duration_sec: int = 90
    cooldown_sample_interval_sec: int = 10
    retry_timeout_sec: int   = 30
    retry_interval_sec: int  = 2

    def validate(self) -> None:
        if self.threads_per_client < 1:
            raise ValueError("workload.threads_per_client must be >= 1")
        if self.num_files < 1:
            raise ValueError("workload.num_files must be >= 1")
        if self.file_size_bytes < 1:
            raise ValueError("workload.file_size_bytes must be >= 1")
        if self.num_directories < 1:
            raise ValueError("workload.num_directories must be >= 1")
        if self.files_per_directory < 1:
            raise ValueError("workload.files_per_directory must be >= 1")
        if self.held_open_files < 0:
            raise ValueError("workload.held_open_files must be >= 0")
        if self.burst_duration_sec <= 0:
            raise ValueError("workload.burst_duration_sec must be > 0")
        if self.cooldown_duration_sec <= 0:
            raise ValueError("workload.cooldown_duration_sec must be > 0")
        if self.cooldown_sample_interval_sec <= 0:
            raise ValueError("workload.cooldown_sample_interval_sec must be > 0")
        if self.cooldown_sample_interval_sec >= self.cooldown_duration_sec:
            raise ValueError(
                "workload.cooldown_sample_interval_sec must be less than "
                "workload.cooldown_duration_sec"
            )
        if self.retry_timeout_sec <= 0:
            raise ValueError("workload.retry_timeout_sec must be > 0")
        if self.retry_interval_sec <= 0:
            raise ValueError("workload.retry_interval_sec must be > 0")


# ---------------------------------------------------------------------------
# Test config
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class TestConfig:
    server: ServerConfig
    clients: List[ClientConfig]
    workload: WorkloadConfig
    protocol: str = ProtocolMode.BOTH
    num_cycles: int = 6
    fd_tolerance_pct: float = 10.0       # % increase allowed across settled cycles
    fd_accounting_tolerance: int = 100   # allowed discrepancy total ≈ global+state+temp
    scenario: str = ""                   # e.g. "TC01", "TC04" — empty = all

    def validate(self) -> None:
        self.protocol = ProtocolMode.validate(self.protocol)
        if self.num_cycles < 1:
            raise ValueError("test.num_cycles must be >= 1")
        if self.fd_tolerance_pct < 0:
            raise ValueError("test.fd_tolerance_pct must be >= 0")
        if self.fd_accounting_tolerance < 0:
            raise ValueError("test.fd_accounting_tolerance must be >= 0")
        self.server.validate()
        if not self.clients:
            raise ValueError("test.clients must contain at least one entry")
        controller_count = sum(1 for c in self.clients if c.role == "controller")
        if controller_count != 1:
            raise ValueError("Exactly one client must have role='controller'")
        for client in self.clients:
            client.validate()
        self.workload.validate()

    def controller(self) -> ClientConfig:
        for c in self.clients:
            if c.role == "controller":
                return c
        raise RuntimeError("No controller client configured")

    def workers(self) -> List[ClientConfig]:
        return [c for c in self.clients if c.role != "controller"]


# ---------------------------------------------------------------------------
# Default (localhost / demo) config for unit testing
# ---------------------------------------------------------------------------

def make_default_test_config(
    server_addr: str = "ganesha-server",
    client_addrs: Optional[List[str]] = None,
    protocol: str = ProtocolMode.BOTH,
    num_cycles: int = 6,
) -> TestConfig:
    if client_addrs is None:
        client_addrs = ["client-1", "client-2", "client-3"]

    clients = []
    for i, addr in enumerate(client_addrs):
        role = "controller" if i == 0 else "worker"
        clients.append(ClientConfig(address=addr, role=role))

    return TestConfig(
        server=ServerConfig(address=server_addr),
        clients=clients,
        workload=WorkloadConfig(),
        protocol=protocol,
        num_cycles=num_cycles,
    )

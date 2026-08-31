#!/usr/bin/env python3
"""
CLI entry-point — NFS-Ganesha FD/LRU Stress and Reclamation Test Framework.

4-scenario design:
  TC01  Sanity        — environment validation + minimal smoke test
  TC02  NFSv3 Stress  — complete V3 FD/LRU lifecycle
  TC03  NFSv4 Stress  — complete V4 FD/LRU lifecycle + state-FD
  TC04  Mixed Stress  — concurrent V3+V4, active handles, dual-category FD

Run modes:
  fast    1 cycle,  small workload  (~2–5 min)   CI gate
  normal  6 cycles, default         (~30 min)    standard FVT  [default]
  soak    12 cycles, large workload (~90 min)    regression

Usage examples
--------------
# Run all 4 scenarios in normal mode:
  python -m nfs_ganesha_fd_lru_test.main \\
      --server ganesha-node1 --export /export \\
      --clients client-1,client-2,client-3

# Quick sanity only:
  python -m nfs_ganesha_fd_lru_test.main \\
      --server ganesha-node1 --export /export \\
      --clients client-1,client-2,client-3 \\
      --scenario TC01

# V4 stress in soak mode:
  python -m nfs_ganesha_fd_lru_test.main \\
      --server ganesha-node1 --export /export \\
      --clients client-1,client-2,client-3 \\
      --scenario TC03 --mode soak -v

# OpenStack orchestrator node (VIP != physical server node):
#   --server      : the VIP / floating-IP used for NFS mounts
#   --server-ssh  : the real physical node IP/hostname used for SSH
#                   (ganesha_stats, log tailing, ganesha.nfsd --version)
#   --ssh-key     : path to the OpenStack tenant private key on this node
  python -m nfs_ganesha_fd_lru_test.main \\
      --server     192.0.2.10 \\
      --server-ssh 10.0.1.50 \\
      --export     /export \\
      --clients    10.0.1.51,10.0.1.52 \\
      --ssh-user   cloud-user \\
      --ssh-key    ~/.ssh/openstack.pem \\
      --scenario   TC01
"""

from __future__ import annotations

import argparse
import copy
import logging
import sys

from .framework.config import (
    ClientConfig, ProtocolMode, ServerConfig, TestConfig, WorkloadConfig,
)
from .framework.verdict import Verdict
from .scenarios.mode    import RunMode
from .scenarios.registry import ALL_SCENARIOS, SCENARIO_MAP, get_scenario


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="NFS-Ganesha FD/LRU Stress and Reclamation Test Framework",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Required
    p.add_argument("--server",  required=True, help="Ganesha server hostname/IP")
    p.add_argument("--export",  required=True, help="NFS export path (e.g. /export)")
    p.add_argument("--clients", required=True,
                   help="Comma-separated client hostnames. First = controller.")

    # Test scope
    p.add_argument("--scenario", default="",
                   help="Run one scenario: TC01 TC02 TC03 TC04. Empty = run all.")
    p.add_argument("--mode", default=RunMode.NORMAL,
                   choices=[RunMode.FAST, RunMode.NORMAL, RunMode.SOAK],
                   help="Execution mode controlling workload size and cycle count.")

    # SSH / server
    p.add_argument("--ssh-user",   default="root")
    p.add_argument("--server-log", default="/var/log/ganesha.log")
    p.add_argument(
        "--server-ssh", default="",
        metavar="HOST",
        help=(
            "Physical node hostname/IP to SSH into for ganesha_stats and log access. "
            "Use when --server is a VIP/floating-IP (OpenStack scale cluster). "
            "Defaults to --server when not set."
        ),
    )
    p.add_argument(
        "--ssh-key", default="",
        metavar="PATH",
        help=(
            "Path to SSH private key on this orchestrator node "
            "(e.g. ~/.ssh/openstack.pem). Applied to all SSH/SCP calls."
        ),
    )

    # Workload base values (scaled by mode)
    p.add_argument("--threads",   type=int, default=8,    help="Base threads per client")
    p.add_argument("--files",     type=int, default=200,  help="Base files per thread")
    p.add_argument("--file-size", type=int, default=4096, help="File size in bytes")

    # Thresholds
    p.add_argument("--fd-tolerance", type=float, default=10.0,
                   help="Allowed settled-FD increase %% across cycles")

    # Output
    p.add_argument("--report-file", default="",
                   help="Write final report to this file (stdout if empty)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def _build_config(args) -> TestConfig:
    addrs = [a.strip() for a in args.clients.split(",") if a.strip()]
    if not addrs:
        raise ValueError("--clients must contain at least one address")
    ssh_key = getattr(args, "ssh_key", "") or ""
    clients = [
        ClientConfig(
            address=a,
            ssh_user=args.ssh_user,
            role="controller" if i == 0 else "worker",
            identity_file=ssh_key,
        )
        for i, a in enumerate(addrs)
    ]
    return TestConfig(
        server=ServerConfig(
            address=args.server,
            ssh_user=args.ssh_user,
            nfs_export=args.export,
            ganesha_log_path=args.server_log,
            ssh_address=getattr(args, "server_ssh", "") or "",
            identity_file=ssh_key,
        ),
        clients=clients,
        workload=WorkloadConfig(
            threads_per_client=args.threads,
            num_files=args.files,
            file_size_bytes=args.file_size,
        ),
        fd_tolerance_pct=args.fd_tolerance,
    )


def main(argv=None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        config = _build_config(args)
        config.validate()
    except (ValueError, TypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    ids_to_run = (
        [args.scenario.upper()]
        if args.scenario
        else [cls.SCENARIO_ID for cls in ALL_SCENARIOS]
    )
    if args.scenario and args.scenario.upper() not in SCENARIO_MAP:
        print(f"ERROR: Unknown scenario '{args.scenario}'. "
              f"Valid: {list(SCENARIO_MAP)}", file=sys.stderr)
        return 2

    print(f"\n{'='*62}")
    print(f"  NFS-Ganesha FD/LRU Test  |  mode={args.mode.upper()}")
    print(f"  Server : {args.server}{args.export}")
    print(f"  Clients: {', '.join(c.address for c in config.clients)}")
    print(f"  Running: {', '.join(ids_to_run)}")
    print(f"{'='*62}\n")

    overall_exit = 0
    for sc_id in ids_to_run:
        cls = SCENARIO_MAP[sc_id]
        print(f"\n{'─'*62}")
        print(f"  {sc_id}  {cls.DESCRIPTION}")
        print(f"{'─'*62}")
        sc_config = copy.deepcopy(config)
        scenario  = get_scenario(sc_id, sc_config, mode=args.mode)
        try:
            suite = scenario.run()
            status = suite.overall.value
            print(f"\n  ► {sc_id} result: {status}")
            if suite.overall == Verdict.FAIL:
                overall_exit = 1
        except Exception as exc:  # pylint: disable=broad-except
            print(f"  ► {sc_id} EXCEPTION: {exc}", file=sys.stderr)
            overall_exit = 1
            if args.verbose:
                import traceback
                traceback.print_exc()

    print(f"\n{'='*62}")
    print(f"  Overall: {'PASS' if overall_exit == 0 else 'FAIL'}")
    print(f"{'='*62}\n")
    return overall_exit


if __name__ == "__main__":
    sys.exit(main())

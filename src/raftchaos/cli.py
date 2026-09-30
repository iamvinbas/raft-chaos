"""Command line interface: run one seed, or hunt for failing seeds across many."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

from .bugs import BUG_NAMES, Bugs
from .sim import SimConfig, run_simulation


def _probe(args: tuple[int, SimConfig]) -> tuple[int, str | None, str]:
    seed, config = args
    result = run_simulation(seed, config)
    if result.violation is None:
        return seed, None, ""
    return seed, result.violation.kind, str(result.violation)


def hunt(config: SimConfig, start: int, count: int, jobs: int) -> tuple[int, str | None, str, int]:
    """Scan seeds in order. Returns (seed, kind, message, seeds_tried) for the first failure."""
    seeds = [(s, config) for s in range(start, start + count)]
    if jobs <= 1:
        for tried, item in enumerate(seeds, 1):
            seed, kind, message = _probe(item)
            if kind is not None:
                return seed, kind, message, tried
        return -1, None, "", count
    pool = ProcessPoolExecutor(max_workers=jobs)
    try:
        for tried, (seed, kind, message) in enumerate(pool.map(_probe, seeds, chunksize=4), 1):
            if kind is not None:
                return seed, kind, message, tried
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return -1, None, "", count


def _config_from(args: argparse.Namespace, bug: str | None) -> SimConfig:
    config = SimConfig(n_nodes=args.nodes)
    if getattr(args, "duration", None):
        config = replace(config, duration_ms=args.duration)
    if bug and bug != "none":
        config = replace(config, bugs=Bugs.only(bug))
    return config


def cmd_run(args: argparse.Namespace) -> int:
    config = _config_from(args, args.bug)
    result = run_simulation(args.seed, config, trace=args.trace)
    if args.trace:
        print("\n".join(result.trace))
    if args.history:
        for op in result.history:
            end = "?" if op.response is None else str(op.response)
            what = f"put {op.key}={op.value}" if op.kind == "put" else f"get {op.key}->{op.result}"
            print(f"  op{op.id:<4} [{op.invoke:>5} .. {end:>5}] {what}")
    print(f"seed {args.seed}, {config.n_nodes} nodes, bug={args.bug or 'none'}")
    print("stats:", ", ".join(f"{k}={v}" for k, v in sorted(result.stats.items())))
    if result.violation is None:
        print("OK: all invariants held and the history is linearizable")
        return 0
    print(f"VIOLATION {result.violation}")
    print(
        f"reproduce: raftchaos run --seed {args.seed} --nodes {args.nodes}"
        + (f" --bug {args.bug}" if args.bug else "")
        + " --trace"
    )
    return 1


def cmd_hunt(args: argparse.Namespace) -> int:
    targets = ["none", *BUG_NAMES] if args.all else [args.bug or "none"]
    print(f"{'bug':26} {'seed':>6} {'tried':>6} {'time':>7}  violation")
    exit_code = 0
    for bug in targets:
        started = time.perf_counter()
        seed, kind, _msg, tried = hunt(_config_from(args, bug), args.start, args.seeds, args.jobs)
        elapsed = time.perf_counter() - started
        found = kind is not None
        if bug == "none" and found:
            exit_code = 1  # a correct node must never fail
        if bug != "none" and not found:
            exit_code = 1
        seed_col = str(seed) if found else "-"
        print(f"{bug:26} {seed_col:>6} {tried:>6} {elapsed:>6.1f}s  {kind or 'none found'}")
    return exit_code


def cmd_bugs(_args: argparse.Namespace) -> int:
    print("\n".join(BUG_NAMES))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="raftchaos", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run one deterministic simulation")
    run.add_argument("--seed", type=int, required=True)
    run.add_argument("--nodes", type=int, default=3)
    run.add_argument("--bug", choices=BUG_NAMES)
    run.add_argument("--duration", type=int, help="milliseconds of load and faults")
    run.add_argument("--trace", action="store_true", help="print nemesis actions")
    run.add_argument("--history", action="store_true", help="print the client operation history")
    run.set_defaults(func=cmd_run)

    hunt_p = sub.add_parser("hunt", help="search seeds for a failing run")
    hunt_p.add_argument("--bug", choices=BUG_NAMES)
    hunt_p.add_argument("--all", action="store_true", help="control run plus every injected bug")
    hunt_p.add_argument("--seeds", type=int, default=200, help="how many seeds to try")
    hunt_p.add_argument("--start", type=int, default=0)
    hunt_p.add_argument("--nodes", type=int, default=3)
    hunt_p.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 4) // 4))
    hunt_p.set_defaults(func=cmd_hunt)

    bugs = sub.add_parser("bugs", help="list injectable bugs")
    bugs.set_defaults(func=cmd_bugs)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))

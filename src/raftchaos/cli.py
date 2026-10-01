"""Command line interface: run one seed, or hunt for failing seeds across many."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

from .anomaly import analyze
from .bugs import BUG_NAMES, DEFAULT_SNAPSHOT_EVERY, SNAPSHOT_BUGS, Bugs
from .metrics import compute_metrics, to_prometheus
from .sim import SimConfig, run_simulation
from .slo import evaluate, render_report
from .timeline_svg import render_timeline

PROFILES = ("default", "adversarial")


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
    make = SimConfig.adversarial if args.profile == "adversarial" else SimConfig
    config = make(n_nodes=args.nodes)
    if getattr(args, "duration", None):
        config = replace(config, duration_ms=args.duration)
    if getattr(args, "no_nemesis", False):
        config = replace(config, nemesis=False)
    if bug and bug != "none":
        config = replace(config, bugs=Bugs.only(bug))
    if getattr(args, "pre_vote", False):
        config = replace(config, raft=replace(config.raft, pre_vote=True))
    every = getattr(args, "snapshot_every", 0) or 0
    if not every and bug in SNAPSHOT_BUGS:
        every = DEFAULT_SNAPSHOT_EVERY  # a snapshot bug needs snapshots to show up at all
    if every:
        config = replace(config, raft=replace(config.raft, snapshot_every=every))
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
        + (f" --profile {args.profile}" if args.profile != "default" else "")
        + (f" --snapshot-every {args.snapshot_every}" if args.snapshot_every else "")
        + " --trace"
    )
    return 1


def cmd_hunt(args: argparse.Namespace) -> int:
    targets = ["none", *BUG_NAMES] if args.all else [args.bug or "none"]
    width = max(len(name) for name in ("none", *BUG_NAMES)) + 1
    print(f"{'bug':{width}} {'seed':>6} {'tried':>6} {'time':>7}  violation")
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
        print(f"{bug:{width}} {seed_col:>6} {tried:>6} {elapsed:>6.1f}s  {kind or 'none found'}")
    return exit_code


def cmd_metrics(args: argparse.Namespace) -> int:
    config = _config_from(args, args.bug)
    result = run_simulation(args.seed, config)
    labels = {"seed": str(args.seed), "nodes": str(args.nodes)}
    print(to_prometheus(compute_metrics(result), labels), end="")
    return 0


def cmd_slo(args: argparse.Namespace) -> int:
    """Same seed twice: a steady-state baseline, then with the nemesis on."""
    missed = False
    for label, no_nemesis in (("steady state (no faults)", True), ("under chaos", False)):
        args.no_nemesis = no_nemesis
        result = run_simulation(args.seed, _config_from(args, args.bug))
        results = evaluate(compute_metrics(result))
        print(render_report(f"seed {args.seed}, {args.nodes} nodes, {label}", results))
        print()
        missed = missed or any(not r.met for r in results)
    return 1 if missed and args.strict else 0


def cmd_anomalies(args: argparse.Namespace) -> int:
    args.no_nemesis = False
    result = run_simulation(args.seed, _config_from(args, args.bug))
    detection = analyze(result)
    print(f"seed {args.seed}: {len(detection.anomalies)} of {detection.windows} windows flagged")
    for anomaly, causes in zip(detection.anomalies, detection.causes, strict=True):
        active = ", ".join(sorted({f.kind for f in causes})) or "none nearby"
        print(
            f"  {anomaly.start:>5}-{anomaly.end:<5}ms  {anomaly.reason:16} "
            f"score {anomaly.score:+.1f}  faults: {active}"
        )
    args.no_nemesis = True
    quiet = analyze(run_simulation(args.seed, _config_from(args, args.bug)))
    print(f"same seed without faults: {len(quiet.anomalies)} of {quiet.windows} windows flagged")
    return 0


def cmd_timeline(args: argparse.Namespace) -> int:
    result = run_simulation(args.seed, _config_from(args, args.bug))
    svg = render_timeline(result, args.title)
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(svg)
    outcome = f"violation: {result.violation}" if result.violation else "no violation"
    print(f"wrote {args.out} ({outcome})")
    return 0


def _parse_addr(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    if not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected host:port, got {text!r}")
    return host, int(port)


def _parse_nodes(text: str) -> list[tuple[str, int]]:
    return [_parse_addr(part) for part in text.split(",") if part]


def cmd_node(args: argparse.Namespace) -> int:
    import asyncio
    import logging
    from pathlib import Path

    from .node import RaftConfig
    from .runtime.server import NodeServer, serve_forever

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(name)s %(message)s")
    addresses = dict(enumerate(_parse_nodes(args.peers)))
    if args.id not in addresses:
        raise SystemExit(f"--id {args.id} is not in --peers ({len(addresses)} nodes)")
    listen = ("0.0.0.0", addresses[args.id][1]) if args.listen is None else args.listen
    restore = None
    if args.restore:
        from .runtime.backup import describe, load_backup

        restore, raw = load_backup(Path(args.restore))
        print(f"restoring from {describe(raw)}", flush=True)
    server = NodeServer(
        args.id,
        addresses,
        Path(args.data_dir),
        listen=listen,
        metrics_port=args.metrics_port,
        config=RaftConfig(pre_vote=not args.no_pre_vote, snapshot_every=args.snapshot_every),
    )
    if restore is not None:
        try:
            server.storage.seed(restore)
        except ValueError as exc:
            raise SystemExit(f"node {args.id}: {exc} in {args.data_dir}") from exc
    try:
        asyncio.run(serve_forever(server))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_kv(args: argparse.Namespace) -> int:
    import asyncio

    from .runtime.client import KvClient, Unavailable

    if args.action == "put" and args.value is None:
        raise SystemExit("kv put needs a value")
    client = KvClient(args.nodes)

    async def go() -> int:
        try:
            if args.action == "put":
                await client.put(args.key, args.value, args.timeout)
                print("OK")
            else:
                print(await client.get(args.key, args.timeout))
        except Unavailable as exc:
            print(f"unavailable: {exc}")
            return 1
        return 0

    return asyncio.run(go())


def cmd_status(args: argparse.Namespace) -> int:
    import asyncio

    from .runtime.client import fetch_status

    async def go() -> int:
        code = 0
        for i, address in enumerate(args.metrics):
            try:
                print(await fetch_status(address))
            except (OSError, asyncio.TimeoutError):
                print(f'{{"id": {i}, "role": "unreachable"}}')
                code = 1
        return code

    return asyncio.run(go())


def cmd_verify(args: argparse.Namespace) -> int:
    import asyncio

    from .runtime.verify import run_verify

    report = asyncio.run(run_verify(args.nodes, args.seconds, args.clients, seed=args.seed))
    print(
        f"{report.ops_ok} operations acknowledged, {report.ops_unknown} with unknown outcome, "
        f"{report.seconds:.1f}s"
    )
    if report.ok:
        print("OK: the history is linearizable")
        return 0
    print(f"VIOLATION: history of key {report.violation_key!r} is not linearizable")
    return 1


def cmd_viz(args: argparse.Namespace) -> int:
    import webbrowser
    from pathlib import Path

    from .viz import Scenario, build_html, demo_html, record

    if args.seed is None:
        html = demo_html()
        what = "demo scenarios"
    else:
        title = f"seed {args.seed}" + (f", bug {args.bug}" if args.bug else "")
        scenario = Scenario("run", title, title, "", args.seed, args.bug, args.profile, args.nodes)
        html = build_html([record(scenario, args.duration)])
        what = title
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out} ({what}, {len(html) // 1024} KB)")
    if args.open:
        webbrowser.open(out.resolve().as_uri())
    return 0


def cmd_experiment(args: argparse.Namespace) -> int:
    from .experiments import HEAL_AT, ISOLATE_AT, render, run_isolation

    seeds = range(args.start, args.start + args.seeds)
    print(
        f"one follower isolated at {ISOLATE_AT} ms, network healed at {HEAL_AT} ms, "
        f"{args.nodes} nodes, seeds {seeds.start}-{seeds.stop - 1}"
    )
    summaries = [run_isolation(seeds, pv, args.nodes) for pv in (False, True)]
    print(render(summaries))
    print(
        "disrupted: runs where the rejoining node forced an election on a healthy leader\n"
        "term +: how far the isolated node raised its term while cut off"
    )
    return 0 if all(s.safe == s.runs for s in summaries) else 1


def cmd_live(args: argparse.Namespace) -> int:
    import asyncio
    import logging
    import webbrowser
    from pathlib import Path

    from .live.bridge import Bridge, LiveServer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    compose = None if args.no_faults else Path(args.compose).resolve()
    if compose is not None and not compose.exists():
        raise SystemExit(f"{compose} not found; pass --compose or --no-faults")
    if len(args.nodes) != len(args.metrics):
        raise SystemExit("--nodes and --metrics must list the same number of nodes")
    bridge = Bridge(args.nodes, args.metrics, compose)
    server = LiveServer(bridge, "127.0.0.1", args.port)
    url = f"http://127.0.0.1:{args.port}/"
    print(f"live view on {url} (Ctrl+C to stop)")
    if args.open:
        webbrowser.open(url)
    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        pass
    return 0


def cmd_backup(args: argparse.Namespace) -> int:
    import asyncio
    import json
    from pathlib import Path

    from .runtime.backup import describe, fetch_backup, load_backup

    if args.action == "save":
        if not args.metrics or not args.out:
            raise SystemExit("backup save needs --metrics and --out")
        try:
            raw = asyncio.run(fetch_backup(args.metrics))
        except ConnectionError as exc:
            print(f"backup failed: {exc}")
            return 1
        out = Path(args.out)
        tmp = out.with_name(out.name + ".tmp")
        tmp.write_text(json.dumps(raw, indent=1) + "\n", encoding="utf-8")
        tmp.replace(out)  # never leave a half-written backup behind
        print(f"wrote {out}: {describe(raw)}")
        return 0
    if not args.file:
        raise SystemExit("backup show needs a file")
    _, raw = load_backup(Path(args.file))
    print(describe(raw))
    return 0


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
    run.add_argument("--profile", choices=PROFILES, default="default")
    run.add_argument("--pre-vote", action="store_true", help="enable the PreVote extension")
    run.add_argument("--snapshot-every", type=int, help="compact the log every N applied entries")
    run.add_argument("--duration", type=int, help="milliseconds of load and faults")
    run.add_argument("--trace", action="store_true", help="print nemesis actions")
    run.add_argument("--history", action="store_true", help="print the client operation history")
    run.set_defaults(func=cmd_run)

    hunt_p = sub.add_parser("hunt", help="search seeds for a failing run")
    hunt_p.add_argument("--bug", choices=BUG_NAMES)
    hunt_p.add_argument("--profile", choices=PROFILES, default="default")
    hunt_p.add_argument("--pre-vote", action="store_true", help="enable the PreVote extension")
    hunt_p.add_argument(
        "--snapshot-every", type=int, help="compact the log every N applied entries"
    )
    hunt_p.add_argument("--all", action="store_true", help="control run plus every injected bug")
    hunt_p.add_argument("--seeds", type=int, default=200, help="how many seeds to try")
    hunt_p.add_argument("--start", type=int, default=0)
    hunt_p.add_argument("--nodes", type=int, default=3)
    hunt_p.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 4) // 4))
    hunt_p.set_defaults(func=cmd_hunt)

    def add_sim_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--seed", type=int, required=True)
        p.add_argument("--nodes", type=int, default=3)
        p.add_argument("--bug", choices=BUG_NAMES)
        p.add_argument("--profile", choices=PROFILES, default="default")
        p.add_argument("--pre-vote", action="store_true", help="enable the PreVote extension")
        p.add_argument("--snapshot-every", type=int, help="compact the log every N applied entries")
        p.add_argument("--duration", type=int, help="milliseconds of load and faults")

    metrics = sub.add_parser("metrics", help="print run metrics in Prometheus text format")
    add_sim_args(metrics)
    metrics.set_defaults(func=cmd_metrics)

    slo = sub.add_parser("slo", help="SLO and error-budget report, steady state vs chaos")
    add_sim_args(slo)
    slo.add_argument("--strict", action="store_true", help="exit 1 if any SLO is missed")
    slo.set_defaults(func=cmd_slo)

    anomalies = sub.add_parser("anomalies", help="flag anomalous windows and attribute faults")
    add_sim_args(anomalies)
    anomalies.set_defaults(func=cmd_anomalies)

    timeline = sub.add_parser("timeline", help="draw a run as an SVG timeline")
    add_sim_args(timeline)
    timeline.add_argument("--out", required=True, help="output .svg path")
    timeline.add_argument("--title")
    timeline.set_defaults(func=cmd_timeline)

    node = sub.add_parser("node", help="run one real Raft node over TCP")
    node.add_argument("--id", type=int, required=True)
    node.add_argument("--peers", required=True, help="host:port of every node, ordered by id")
    node.add_argument("--listen", type=_parse_addr, help="default: 0.0.0.0:<own port>")
    node.add_argument("--data-dir", required=True)
    node.add_argument("--metrics-port", type=int)
    node.add_argument("--log-level", default="info")
    node.add_argument("--no-pre-vote", action="store_true", help="disable PreVote (on by default)")
    node.add_argument(
        "--restore",
        metavar="BACKUP",
        help="start this empty node from a backup file (give every node the same file)",
    )
    node.add_argument(
        "--snapshot-every",
        type=int,
        default=1000,
        help="compact the log every N applied entries (0 disables snapshots)",
    )
    node.set_defaults(func=cmd_node)

    kv = sub.add_parser("kv", help="read or write the replicated store")
    kv.add_argument("--nodes", type=_parse_nodes, required=True)
    kv.add_argument("--timeout", type=float, default=5.0)
    kv.add_argument("action", choices=("put", "get"))
    kv.add_argument("key")
    kv.add_argument("value", nargs="?")
    kv.set_defaults(func=cmd_kv)

    status = sub.add_parser("status", help="query /status on each node's metrics port")
    status.add_argument("--metrics", type=_parse_nodes, required=True)
    status.set_defaults(func=cmd_status)

    verify = sub.add_parser("verify", help="check a real cluster for linearizability")
    verify.add_argument("--nodes", type=_parse_nodes, required=True)
    verify.add_argument("--seconds", type=float, default=20.0)
    verify.add_argument("--clients", type=int, default=3)
    verify.add_argument("--seed", type=int)
    verify.set_defaults(func=cmd_verify)

    viz = sub.add_parser("viz", help="build the interactive web visualiser (one HTML file)")
    viz.add_argument("--out", required=True, help="output .html path")
    viz.add_argument("--seed", type=int, help="record this seed; omit for the demo scenarios")
    viz.add_argument("--nodes", type=int, default=3)
    viz.add_argument("--bug", choices=BUG_NAMES)
    viz.add_argument("--profile", choices=PROFILES, default="default")
    viz.add_argument("--duration", type=int, help="milliseconds of load and faults")
    viz.add_argument("--open", action="store_true", help="open the page in a browser")
    viz.set_defaults(func=cmd_viz)

    live = sub.add_parser("live", help="watch and break a running cluster in the browser")
    live.add_argument(
        "--nodes",
        type=_parse_nodes,
        default="127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102",
        help="client host:port of every node, ordered by id (default: the compose cluster)",
    )
    live.add_argument(
        "--metrics",
        type=_parse_nodes,
        default="127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102",
        help="metrics host:port of every node, same order",
    )
    live.add_argument("--compose", default="docker-compose.yml", help="compose file for faults")
    live.add_argument("--no-faults", action="store_true", help="watch only, no fault buttons")
    live.add_argument("--port", type=int, default=8080)
    live.add_argument("--open", action="store_true", help="open the page in a browser")
    live.set_defaults(func=cmd_live)

    experiment = sub.add_parser("experiment", help="controlled experiments")
    experiment.add_argument("name", choices=("prevote",))
    experiment.add_argument("--seeds", type=int, default=50)
    experiment.add_argument("--start", type=int, default=0)
    experiment.add_argument("--nodes", type=int, default=3)
    experiment.set_defaults(func=cmd_experiment)

    backup = sub.add_parser("backup", help="save a running cluster's data to a file, or show one")
    backup.add_argument("action", choices=("save", "show"))
    backup.add_argument("file", nargs="?", help="backup file to show")
    backup.add_argument("--metrics", type=_parse_nodes, help="metrics host:port of the nodes")
    backup.add_argument("--out", help="where to write the backup")
    backup.set_defaults(func=cmd_backup)

    bugs = sub.add_parser("bugs", help="list injectable bugs")
    bugs.set_defaults(func=cmd_bugs)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))

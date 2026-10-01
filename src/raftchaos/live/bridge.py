"""A local web server between the browser and a running cluster.

It polls every node's `/status`, serves the visualiser in live mode, drives a client workload
whose history can be checked for linearizability, and injects faults into the Docker Compose
cluster (kill, restart, isolate with iptables, slow down with tc netem, heal).

Because it can run `docker` commands, it only answers requests that come from its own page:
it binds to 127.0.0.1, rejects foreign `Host` headers (DNS rebinding) and requires a random
per-run token on every action (a cross-site page cannot read it or send the custom header).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..linearizability import Op, find_violation
from ..runtime.client import Address, KvClient, Unavailable
from ..viz import build_html

log = logging.getLogger("raftchaos.live")

POLL_S = 0.1
RAFT_PORT = 7000  # the port every node listens on inside the compose network
MAX_BODY = 4096
ALLOWED_HOSTS = ("127.0.0.1", "localhost")


@dataclass
class Workload:
    running: bool = False
    clients: int = 3
    ok: int = 0
    unknown: int = 0
    history: list[Op] = field(default_factory=list)
    recent: list[tuple[float, int]] = field(default_factory=list)  # (finished at, latency ms)
    tasks: list[asyncio.Task[None]] = field(default_factory=list)
    next_id: int = 0
    # Each load session writes its own keys. The history check only knows this session's
    # operations, so a write from anyone else to the same keys (an earlier bridge, a kv command)
    # would look like a value from nowhere and raise a false alarm.
    keys: tuple[str, ...] = ("x", "y")


class Bridge:
    def __init__(
        self,
        client_addrs: list[Address],
        status_addrs: list[Address],
        compose_file: Path | None,
    ) -> None:
        self.client_addrs = client_addrs
        self.status_addrs = status_addrs
        self.compose_file = compose_file
        self.n = len(status_addrs)
        self.token = secrets.token_urlsafe(18)
        self.started = time.monotonic()
        self.nodes: list[dict[str, Any]] = [{"id": i, "up": False} for i in range(self.n)]
        self.events: list[dict[str, Any]] = []
        self.isolated: set[int] = set()
        self.slow: set[int] = set()
        self.work = Workload()
        self.check: dict[str, Any] = {"status": "idle"}
        self.busy = False

    def now_ms(self) -> int:
        return int((time.monotonic() - self.started) * 1000)

    def event(self, kind: str, text: str, node: int | None = None) -> None:
        self.events.append({"t": self.now_ms(), "kind": kind, "text": text, "node": node})
        log.info("%s", text)

    # ---- polling -----------------------------------------------------------------------

    async def _status(self, i: int) -> dict[str, Any]:
        host, port = self.status_addrs[i]
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), 0.3)
        except (OSError, asyncio.TimeoutError):
            return {"id": i, "up": False}
        try:
            writer.write(b"GET /status HTTP/1.1\r\nHost: node\r\n\r\n")
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), 0.3)
            body = json.loads(raw.split(b"\r\n\r\n", 1)[1])
            body["up"] = True
            return dict(body)
        except (OSError, asyncio.TimeoutError, ValueError, IndexError):
            return {"id": i, "up": False}
        finally:
            writer.close()

    async def poll_forever(self) -> None:
        while True:
            self.nodes = list(await asyncio.gather(*(self._status(i) for i in range(self.n))))
            await asyncio.sleep(POLL_S)

    def leader(self) -> int | None:
        leaders = [n for n in self.nodes if n.get("up") and n.get("role") == "leader"]
        return max(leaders, key=lambda n: n["term"])["id"] if leaders else None

    def snapshot(self, since: int) -> dict[str, Any]:
        cutoff = time.monotonic() - 2.0
        recent = [lat for at, lat in self.work.recent if at >= cutoff]
        recent.sort()
        p99 = recent[max(0, int(len(recent) * 0.99) - 1)] if recent else None
        return {
            "t": self.now_ms(),
            "nodes": self.nodes,
            "isolated": sorted(self.isolated),
            "slow": sorted(self.slow),
            "events": self.events[since:],
            "event_count": len(self.events),
            "load": {
                "running": self.work.running,
                "clients": self.work.clients,
                "ok": self.work.ok,
                "unknown": self.work.unknown,
                "rate": round(len(recent) / 2.0, 1),
                "p99_ms": p99,
            },
            "check": self.check,
            "faults_enabled": self.compose_file is not None,
            "busy": self.busy,
        }

    # ---- workload ----------------------------------------------------------------------

    async def _client(self, wid: int) -> None:
        rng = random.Random(wid)
        client = KvClient(self.client_addrs, name=f"live-{wid}-{secrets.token_hex(3)}")
        t0 = self.started
        while self.work.running:
            self.work.next_id += 1
            op_id, key, is_put = self.work.next_id, rng.choice(self.work.keys), rng.random() < 0.6
            begin = time.monotonic()
            invoke = int((begin - t0) * 1000)
            try:
                if is_put:
                    await client.put(key, op_id, 2.0)
                    result = None
                else:
                    result = await client.get(key, 2.0)
            except Unavailable:
                self.work.unknown += 1
                if is_put:
                    self.work.history.append(Op(op_id, "put", key, op_id, None, invoke, None))
            else:
                end = time.monotonic()
                self.work.ok += 1
                self.work.recent.append((end, int((end - begin) * 1000)))
                value = op_id if is_put else None
                kind = "put" if is_put else "get"
                done = int((end - t0) * 1000)
                self.work.history.append(Op(op_id, kind, key, value, result, invoke, done))
            if len(self.work.recent) > 5000:
                del self.work.recent[:2500]
            await asyncio.sleep(rng.uniform(0.005, 0.03))

    async def start_load(self) -> None:
        if self.work.running:
            return
        self.work.running = True
        session = secrets.token_hex(2)
        self.work.keys = (f"x-{session}", f"y-{session}")
        self.work.history.clear()
        self.work.ok = self.work.unknown = 0
        self.check = {"status": "idle"}
        self.work.tasks = [asyncio.create_task(self._client(w)) for w in range(self.work.clients)]
        self.event(
            "load",
            f"load started: {self.work.clients} clients on keys {', '.join(self.work.keys)}",
        )

    async def stop_load(self) -> None:
        self.work.running = False
        await asyncio.gather(*self.work.tasks, return_exceptions=True)
        self.work.tasks = []
        self.event("load", f"load stopped after {self.work.ok} acknowledged operations")

    async def run_check(self) -> None:
        history = list(self.work.history)
        self.check = {"status": "running", "ops": len(history)}
        key = await asyncio.get_running_loop().run_in_executor(None, find_violation, history)
        acknowledged = sum(1 for op in history if op.response is not None)
        if key is None:
            self.check = {"status": "ok", "ops": acknowledged}
            self.event("check", f"history of {acknowledged} operations is linearizable")
        else:
            self.check = {"status": "violation", "ops": acknowledged, "key": key}
            self.event("violation", f"history of key {key!r} is NOT linearizable")

    # ---- faults ------------------------------------------------------------------------

    async def _compose(self, *args: str) -> tuple[int, str]:
        assert self.compose_file is not None
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "-f",
            str(self.compose_file),
            *args,
            cwd=self.compose_file.parent,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        return proc.returncode or 0, out.decode(errors="replace").strip()

    async def fault(self, action: str, node: int | None) -> str:
        if self.compose_file is None:
            return "faults are disabled (no docker-compose.yml)"
        if action == "kill_leader":
            node = self.leader()
            if node is None:
                return "no leader to kill right now"
            action = "kill"
        if action == "revive_all":
            down = [n["id"] for n in self.nodes if not n.get("up")]
            if not down:
                return "every node is already up"
            self.busy = True
            try:
                code, out = await self._compose("start", *(f"node{i}" for i in down))
            finally:
                self.busy = False
            if code != 0:
                self.event("error", f"revive failed: {out.splitlines()[-1] if out else code}")
                return out or f"exit {code}"
            names = ", ".join(str(i) for i in down)
            self.event("restart", f"node{'s' if len(down) > 1 else ''} {names} revived from disk")
            return "ok"
        if action != "heal" and (node is None or not 0 <= node < self.n):
            return "unknown node"
        target = -1 if node is None else node  # -1 only for "heal", which targets no node
        service = f"node{target}"
        self.busy = True
        try:
            if action == "kill":
                code, out = await self._compose("kill", "-s", "KILL", service)
                if code == 0:
                    self.isolated.discard(target)
                    self.slow.discard(target)
                    self.event("crash", f"kill -9 node {node}", node)
            elif action == "restart":
                code, out = await self._compose("start", service)
                if code == 0:
                    self.event("restart", f"node {node} revived from its volume", node)
            elif action == "isolate":
                # Drop Raft traffic by port, not by peer address: it works even when a peer is
                # down and its name does not resolve, and it applies as one step or not at all.
                script = (
                    f"iptables -A INPUT -p tcp --dport {RAFT_PORT} -j DROP && "
                    f"iptables -A OUTPUT -p tcp --dport {RAFT_PORT} -j DROP"
                )
                code, out = await self._compose("exec", "-T", service, "sh", "-c", script)
                if code == 0:
                    self.isolated.add(target)
                    self.event("partition", f"iptables cuts node {node} off from the cluster", node)
            elif action == "slow":
                netem = ["tc", "qdisc", "replace", "dev", "eth0", "root", "netem"]
                code, out = await self._compose(
                    "exec", "-T", service, *netem, "delay", "150ms", "50ms", "loss", "15%"
                )
                if code == 0:
                    self.slow.add(target)
                    self.event("flaky", f"tc netem on node {node}: 150 ms delay, 15% loss", node)
            elif action == "heal":
                for i in sorted(self.isolated | self.slow):
                    await self._compose("exec", "-T", f"node{i}", "iptables", "-F")
                    await self._compose(
                        "exec", "-T", f"node{i}", "tc", "qdisc", "del", "dev", "eth0", "root"
                    )
                self.isolated.clear()
                self.slow.clear()
                code, out = 0, ""
                self.event("heal", "network healed: iptables and tc rules removed")
            else:
                return "unknown action"
            if code != 0:
                self.event(
                    "error", f"{action} node {node} failed: {out.splitlines()[-1] if out else code}"
                )
                return out or f"exit {code}"
            return "ok"
        finally:
            self.busy = False


# ---- a tiny HTTP server -----------------------------------------------------------------


class LiveServer:
    def __init__(self, bridge: Bridge, host: str, port: int) -> None:
        self.bridge = bridge
        self.host = host
        self.port = port
        self.page = build_html(
            [],
            live={
                "token": bridge.token,
                "nodes": bridge.n,
                "clients": bridge.work.clients,
                "poll_ms": 150,
            },
        )

    async def serve(self) -> None:
        server = await asyncio.start_server(self._handle, self.host, self.port)
        tasks = [asyncio.create_task(self.bridge.poll_forever())]
        try:
            async with server:
                await server.serve_forever()
        finally:
            for task in tasks:
                task.cancel()
            if self.bridge.work.running:
                await self.bridge.stop_load()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            lines = head.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {
                k.strip().lower(): v.strip()
                for k, _, v in (line.partition(":") for line in lines[1:] if line)
            }
            host = headers.get("host", "").rsplit(":", 1)[0].strip("[]")
            if host not in ALLOWED_HOSTS:
                await self._reply(writer, 403, "text/plain", b"forbidden host\n")
                return
            length = int(headers.get("content-length", "0") or 0)
            if length > MAX_BODY:
                await self._reply(writer, 413, "text/plain", b"too large\n")
                return
            body = await reader.readexactly(length) if length else b""
            await self._route(writer, method, target, headers, body)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
            pass
        except (ValueError, OSError):
            with contextlib.suppress(OSError):
                await self._reply(writer, 400, "text/plain", b"bad request\n")
        finally:
            writer.close()

    async def _route(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        target: str,
        headers: dict[str, str],
        body: bytes,
    ) -> None:
        path, _, query = target.partition("?")
        if method == "GET" and path in ("/", "/index.html"):
            await self._reply(writer, 200, "text/html; charset=utf-8", self.page.encode())
            return
        if headers.get("x-token") != self.bridge.token:
            await self._reply(writer, 403, "text/plain", b"missing token\n")
            return
        if method == "GET" and path == "/api/snapshot":
            since = 0
            for part in query.split("&"):
                if part.startswith("since=") and part[6:].isdigit():
                    since = int(part[6:])
            data = json.dumps(self.bridge.snapshot(since)).encode()
            await self._reply(writer, 200, "application/json", data)
            return
        if method == "POST" and path == "/api/action":
            request = json.loads(body or b"{}")
            action = str(request.get("action", ""))
            node = request.get("node")
            node = int(node) if isinstance(node, int) else None
            if action == "load_start":
                await self.bridge.start_load()
                result = "ok"
            elif action == "load_stop":
                await self.bridge.stop_load()
                result = "ok"
            elif action == "check":
                asyncio.create_task(self.bridge.run_check())
                result = "ok"
            else:
                result = await self.bridge.fault(action, node)
            await self._reply(
                writer, 200, "application/json", json.dumps({"result": result}).encode()
            )
            return
        await self._reply(writer, 404, "text/plain", b"not found\n")

    async def _reply(self, writer: asyncio.StreamWriter, code: int, kind: str, data: bytes) -> None:
        reason = {
            200: "OK",
            400: "Bad Request",
            403: "Forbidden",
            404: "Not Found",
            413: "Too Large",
        }
        head = (
            f"HTTP/1.1 {code} {reason.get(code, 'Error')}\r\nContent-Type: {kind}\r\n"
            f"Content-Length: {len(data)}\r\nCache-Control: no-store\r\n"
            "X-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n"
        )
        writer.write(head.encode() + data)
        await writer.drain()

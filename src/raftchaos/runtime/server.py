"""A Raft node served over TCP.

The protocol logic is the same `RaftNode` the simulator tests. This module adds only what a
real deployment needs: sockets, a wall clock, a durable log, and an HTTP endpoint for
Prometheus. Every reply is sent after the state it depends on has been fsynced.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import socket
from dataclasses import dataclass, field
from pathlib import Path

from ..messages import Addr, ClientRequest, Message, Outbox
from ..node import RaftConfig, RaftNode, Role
from .codec import decode, encode
from .storage import FileStorage

log = logging.getLogger("raftchaos.node")

TICK_S = 0.01
# Abort a peer connection whose data goes unacknowledged this long. Without it, frames queued
# during a partition sit in TCP retransmission backoff and arrive seconds after the network heals.
UNACKED_TIMEOUT_MS = 1000
QUEUE_LIMIT = 512
MAX_FRAME = 1 << 20


@dataclass
class Counters:
    leader_changes: int = 0
    frames_in: int = 0
    frames_out: int = 0
    frames_dropped: int = 0
    bad_frames: int = 0
    fsyncs: int = 0


@dataclass
class _PeerLink:
    """Outgoing frames to one peer. If the peer is down, frames are dropped, like the network."""

    host: str
    port: int
    queue: asyncio.Queue[bytes] = field(default_factory=lambda: asyncio.Queue(QUEUE_LIMIT))
    task: asyncio.Task[None] | None = None


class NodeServer:
    def __init__(
        self,
        node_id: int,
        addresses: dict[int, tuple[str, int]],
        data_dir: Path,
        listen: tuple[str, int] | None = None,
        metrics_port: int | None = None,
        config: RaftConfig | None = None,
    ) -> None:
        self.id = node_id
        self.addresses = addresses
        self.listen = listen or addresses[node_id]
        self.metrics_port = metrics_port
        # A real service wants PreVote, and a log that does not grow forever.
        self.config = config or RaftConfig(pre_vote=True, snapshot_every=1000)
        self.storage = FileStorage(data_dir / f"node-{node_id}")
        self.counters = Counters()
        # Per peer: when we last heard from it, and frames exchanged. Feeds the live view.
        self.peer_heard: dict[int, int] = {}
        self.peer_in: dict[int, int] = {p: 0 for p in addresses if p != node_id}
        self.peer_out: dict[int, int] = {p: 0 for p in addresses if p != node_id}
        self.node: RaftNode | None = None
        self.links: dict[int, _PeerLink] = {}
        self.clients: dict[str, asyncio.StreamWriter] = {}
        self._servers: list[asyncio.Server] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._writers: set[asyncio.StreamWriter] = set()
        self._last_role = Role.FOLLOWER

    # ---- lifecycle ---------------------------------------------------------------------

    def _now(self) -> int:
        return int(asyncio.get_running_loop().time() * 1000)

    async def start(self) -> None:
        peers = [p for p in self.addresses if p != self.id]
        self.node = RaftNode(
            self.id,
            peers,
            random.Random(),
            now=self._now(),
            config=self.config,
            storage=self.storage.state,
        )
        for peer in peers:
            host, port = self.addresses[peer]
            link = _PeerLink(host, port)
            link.task = asyncio.create_task(self._pump(link))
            self.links[peer] = link
        host, port = self.listen
        self._servers.append(
            await asyncio.start_server(self._on_connection, host, port, limit=MAX_FRAME)
        )
        if self.metrics_port is not None:
            self._servers.append(
                await asyncio.start_server(self._on_http, self.listen[0], self.metrics_port)
            )
        self._tasks.append(asyncio.create_task(self._tick_loop()))
        log.info("node %d listening on %s:%d", self.id, host, port)

    async def stop(self) -> None:
        for task in [*self._tasks, *(link.task for link in self.links.values() if link.task)]:
            task.cancel()
        for server in self._servers:
            server.close()
        for writer in list(self._writers):
            writer.close()
        await asyncio.gather(
            *self._tasks,
            *(link.task for link in self.links.values() if link.task),
            return_exceptions=True,
        )
        for server in self._servers:
            await server.wait_closed()

    @property
    def port(self) -> int:
        """The bound port of the main listener (useful when started with port 0)."""
        return int(self._servers[0].sockets[0].getsockname()[1])

    # ---- the node driver ---------------------------------------------------------------

    async def _tick_loop(self) -> None:
        assert self.node is not None
        while True:
            await asyncio.sleep(TICK_S)
            outbox = self.node.tick(self._now())
            # An idle tick changes nothing durable; a tick that acts always sends something.
            self._flush(outbox, sync=bool(outbox) or self.node.role is not self._last_role)

    def _flush(self, outbox: Outbox, sync: bool = True) -> None:
        """Persist first, then send: a reply must never outrun the state behind it."""
        assert self.node is not None
        if sync and self.storage.sync():
            self.counters.fsyncs += 1
        if self.node.role is Role.LEADER and self._last_role is not Role.LEADER:
            self.counters.leader_changes += 1
        self._last_role = self.node.role
        for dst, msg in outbox:
            self._send(dst, msg)

    def _send(self, dst: Addr, msg: Message) -> None:
        frame = encode(self.id, msg)
        if isinstance(dst, str):
            writer = self.clients.get(dst)
            if writer is not None and not writer.is_closing():
                writer.write(frame)
                self.counters.frames_out += 1
            return
        link = self.links[dst]
        try:
            link.queue.put_nowait(frame)
            self.counters.frames_out += 1
            self.peer_out[dst] += 1
        except asyncio.QueueFull:
            self.counters.frames_dropped += 1

    async def _pump(self, link: _PeerLink) -> None:
        """Keep one outgoing connection to a peer alive and drain its queue into it."""
        while True:
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(link.host, link.port, limit=MAX_FRAME), timeout=1.0
                )
            except (OSError, asyncio.TimeoutError):
                self._discard(link)
                await asyncio.sleep(0.2)
                continue
            _bound_unacked_time(writer)
            try:
                while True:
                    writer.write(await link.queue.get())
                    await writer.drain()
            except (OSError, ConnectionError):
                pass
            finally:
                writer.close()
            self._discard(link)

    def _discard(self, link: _PeerLink) -> None:
        while not link.queue.empty():
            link.queue.get_nowait()
            self.counters.frames_dropped += 1

    # ---- inbound connections -----------------------------------------------------------

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        registered: set[str] = set()
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                self.counters.frames_in += 1
                try:
                    src, msg = decode(line)
                except ValueError:
                    self.counters.bad_frames += 1
                    continue
                if isinstance(msg, ClientRequest):
                    if not isinstance(src, str):
                        self.counters.bad_frames += 1
                        continue
                    self.clients[src] = writer
                    registered.add(src)
                elif not isinstance(src, int) or src not in self.addresses:
                    self.counters.bad_frames += 1
                    continue
                assert self.node is not None
                if isinstance(src, int):
                    self.peer_heard[src] = self._now()
                    self.peer_in[src] += 1
                self._flush(self.node.receive(src, msg, self._now()))
        except (OSError, ConnectionError, asyncio.LimitOverrunError, ValueError):
            pass
        finally:
            for client in registered:
                if self.clients.get(client) is writer:
                    del self.clients[client]
            self._writers.discard(writer)
            writer.close()

    # ---- observability -----------------------------------------------------------------

    def status(self) -> dict[str, object]:
        node = self.node
        assert node is not None
        now = self._now()
        return {
            "id": self.id,
            "role": node.role.value,
            "term": node.current_term,
            "leader": node.leader_id,
            "commit_index": node.commit_index,
            "last_applied": node.last_applied,
            "log_entries": node.last_index,
            "snapshot_index": node.snap_index,
            "pre_vote": self.config.pre_vote,
            "voted_for": node.storage.voted_for,
            "log_tail": [e.term for e in node.log[-12:]],
            "peers": {
                str(p): {
                    "heard_ms": now - self.peer_heard[p] if p in self.peer_heard else None,
                    "in": self.peer_in[p],
                    "out": self.peer_out[p],
                }
                for p in self.peer_in
            },
        }

    def prometheus(self) -> str:
        s = self.status()
        c = self.counters
        node = f'node="{self.id}"'
        lines = [
            "# TYPE raftchaos_node_term gauge",
            f"raftchaos_node_term{{{node}}} {s['term']}",
            "# TYPE raftchaos_node_is_leader gauge",
            f"raftchaos_node_is_leader{{{node}}} {int(s['role'] == 'leader')}",
            "# TYPE raftchaos_node_commit_index gauge",
            f"raftchaos_node_commit_index{{{node}}} {s['commit_index']}",
            "# TYPE raftchaos_node_log_entries gauge",
            f"raftchaos_node_log_entries{{{node}}} {s['log_entries']}",
            "# TYPE raftchaos_node_leader_changes_total counter",
            f"raftchaos_node_leader_changes_total{{{node}}} {c.leader_changes}",
            "# TYPE raftchaos_node_frames_total counter",
            f'raftchaos_node_frames_total{{{node},direction="in"}} {c.frames_in}',
            f'raftchaos_node_frames_total{{{node},direction="out"}} {c.frames_out}',
            f'raftchaos_node_frames_total{{{node},direction="dropped"}} {c.frames_dropped}',
            "# TYPE raftchaos_node_bad_frames_total counter",
            f"raftchaos_node_bad_frames_total{{{node}}} {c.bad_frames}",
            "# TYPE raftchaos_node_fsyncs_total counter",
            f"raftchaos_node_fsyncs_total{{{node}}} {c.fsyncs}",
        ]
        return "\n".join(lines) + "\n"

    async def _on_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await asyncio.wait_for(reader.readline(), timeout=2.0)
            parts = request.decode(errors="replace").split()
            path = parts[1] if len(parts) >= 2 else ""
            if path == "/metrics":
                body, kind, code = self.prometheus(), "text/plain; version=0.0.4", "200 OK"
            elif path == "/status":
                body, kind, code = json.dumps(self.status()) + "\n", "application/json", "200 OK"
            else:
                body, kind, code = "not found\n", "text/plain", "404 Not Found"
            data = body.encode()
            head = f"HTTP/1.1 {code}\r\nContent-Type: {kind}\r\nContent-Length: {len(data)}\r\n"
            writer.write(head.encode() + b"Connection: close\r\n\r\n" + data)
            await writer.drain()
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()


def _bound_unacked_time(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    option = getattr(socket, "TCP_USER_TIMEOUT", None)  # Linux only
    if sock is not None and option is not None:
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, option, UNACKED_TIMEOUT_MS)


async def serve_forever(server: NodeServer) -> None:
    await server.start()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.Event().wait()
    await server.stop()

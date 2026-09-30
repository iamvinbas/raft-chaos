"""Client for a real cluster. Retries reuse the request id, so the state machine deduplicates."""

from __future__ import annotations

import asyncio
import secrets
import time

from ..messages import ClientRequest, ClientResponse
from .codec import decode, encode
from .server import MAX_FRAME

Address = tuple[str, int]


class Unavailable(Exception):
    """No node acknowledged the operation before the deadline. Its outcome is unknown."""


class KvClient:
    def __init__(self, nodes: list[Address], name: str | None = None) -> None:
        self.nodes = nodes  # index in this list is the node id
        self.name = name or f"cli-{secrets.token_hex(4)}"
        self._req_id = 0
        self._target = 0

    async def _ask(self, index: int, request: ClientRequest, timeout: float) -> ClientResponse:
        host, port = self.nodes[index]
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, limit=MAX_FRAME), timeout=timeout
        )
        try:
            writer.write(encode(self.name, request))
            await writer.drain()
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                left = deadline - asyncio.get_running_loop().time()
                line = await asyncio.wait_for(reader.readline(), timeout=max(left, 0.001))
                if not line:
                    raise ConnectionError("node closed the connection")
                _, msg = decode(line)
                if not isinstance(msg, ClientResponse):
                    raise ValueError("unexpected reply")
                # A node answers on the client's latest connection, so a late reply to a
                # request we already gave up on can arrive here. It is not our answer.
                if msg.req_id == request.req_id:
                    return msg
        finally:
            writer.close()

    async def execute(self, command: tuple[object, ...], deadline_s: float = 5.0) -> object:
        """Send a command until a leader commits it. Raises `Unavailable` at the deadline."""
        self._req_id += 1
        request = ClientRequest(self._req_id, command)
        end = time.monotonic() + deadline_s
        attempt = 0
        while time.monotonic() < end:
            index = self._target % len(self.nodes)
            per_try = min(1.5, max(0.1, end - time.monotonic()))
            try:
                reply = await self._ask(index, request, per_try)
            except (OSError, asyncio.TimeoutError, ValueError, ConnectionError):
                self._target += 1
                await asyncio.sleep(min(0.05 * (attempt + 1), 0.3))
                attempt += 1
                continue
            if reply.ok:
                self._target = index
                return reply.result
            hint = reply.leader_hint
            self._target = hint if hint is not None and hint < len(self.nodes) else index + 1
            await asyncio.sleep(0.05)
            attempt += 1
        raise Unavailable(f"no answer within {deadline_s}s for {command[0]}")

    async def put(self, key: str, value: object, deadline_s: float = 5.0) -> None:
        await self.execute(("put", key, value), deadline_s)

    async def get(self, key: str, deadline_s: float = 5.0) -> object:
        return await self.execute(("get", key), deadline_s)


async def fetch_status(address: Address, timeout: float = 1.0) -> str:
    """GET /status from a node's metrics port."""
    reader, writer = await asyncio.wait_for(asyncio.open_connection(*address), timeout=timeout)
    try:
        writer.write(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=timeout)
        return raw.split(b"\r\n\r\n", 1)[-1].decode().strip()
    finally:
        writer.close()

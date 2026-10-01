import asyncio
import json
import re

from raftchaos.live.bridge import Bridge, LiveServer
from test_runtime import free_ports, leader_of, start_cluster, wait_for


async def http(port, method, path, host="127.0.0.1", token=None, body=None):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    data = json.dumps(body).encode() if body is not None else b""
    head = f"{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: {len(data)}\r\n"
    if token:
        head += f"X-Token: {token}\r\n"
    writer.write(head.encode() + b"\r\n" + data)
    await writer.drain()
    raw = await reader.read()
    writer.close()
    status = int(raw.split(b" ", 2)[1])
    return status, raw.split(b"\r\n\r\n", 1)[1]


def test_bridge_serves_live_state_and_refuses_foreign_requests(tmp_path):
    async def scenario():
        addresses, servers, ports = await start_cluster(tmp_path, metrics=True)
        client_addrs = [addresses[i] for i in range(3)]
        status_addrs = [("127.0.0.1", ports[3 + i]) for i in range(3)]
        bridge = Bridge(client_addrs, status_addrs, compose_file=None)
        live = LiveServer(bridge, "127.0.0.1", 0)
        (port,) = free_ports(1)
        srv = await asyncio.start_server(live._handle, "127.0.0.1", port)
        poller = asyncio.create_task(bridge.poll_forever())
        try:
            await wait_for(lambda: leader_of(servers) is not None)
            status, page = await http(port, "GET", "/")
            assert status == 200 and b'"live":' in page
            token = re.search(rb'"token":"([^"]+)"', page).group(1).decode()
            assert token == bridge.token

            # the guards: no token, a foreign Host (DNS rebinding), a forged token
            assert (await http(port, "GET", "/api/snapshot"))[0] == 403
            assert (await http(port, "GET", "/", host="evil.example"))[0] == 403
            forged = await http(
                port, "POST", "/api/action", token="x", body={"action": "kill_leader"}
            )
            assert forged[0] == 403

            await wait_for(lambda: bridge.leader() is not None)
            status, raw = await http(port, "GET", "/api/snapshot", token=token)
            snap = json.loads(raw)
            assert status == 200 and len(snap["nodes"]) == 3
            assert sum(n["role"] == "leader" for n in snap["nodes"]) == 1
            assert all("peers" in n and "log_tail" in n for n in snap["nodes"])
            assert snap["faults_enabled"] is False

            # without a compose file, fault buttons are refused but load and checks work
            _, raw = await http(
                port, "POST", "/api/action", token=token, body={"action": "kill_leader"}
            )
            assert "disabled" in json.loads(raw)["result"]
            _, raw = await http(
                port, "POST", "/api/action", token=token, body={"action": "revive_all"}
            )
            assert "disabled" in json.loads(raw)["result"]
            await http(port, "POST", "/api/action", token=token, body={"action": "load_start"})
            await wait_for(lambda: bridge.work.ok > 20)
            # the load writes keys of its own, so other writers cannot cause a false alarm
            assert bridge.work.keys != ("x", "y")
            assert {op.key for op in bridge.work.history} <= set(bridge.work.keys)
            await http(port, "POST", "/api/action", token=token, body={"action": "load_stop"})
            await http(port, "POST", "/api/action", token=token, body={"action": "check"})
            await wait_for(lambda: bridge.check["status"] in ("ok", "violation"))
            assert bridge.check["status"] == "ok"
            status, raw = await http(port, "GET", "/api/snapshot?since=0", token=token)
            kinds = [e["kind"] for e in json.loads(raw)["events"]]
            assert kinds == ["load", "load", "check"]
        finally:
            poller.cancel()
            srv.close()
            await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)

    asyncio.run(scenario())

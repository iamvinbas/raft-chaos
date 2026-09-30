"""Draw a run as an SVG: who was leader when, which faults were active, when clients succeeded.

Pure standard library, so the picture can be regenerated anywhere with `raftchaos timeline`.
"""

from __future__ import annotations

from html import escape

from .anomaly import fault_intervals
from .sim import RunResult

COLORS = {
    "leader": "#2e9e5b",
    "follower": "#c5d0e0",
    "candidate": "#e8a838",
    "down": "#3a3f4b",
}
LEFT, RIGHT, LANE_H, LANE_GAP = 92, 24, 20, 12
WIDTH = 1100


def _role_intervals(result: RunResult, node: int, end: int) -> list[tuple[int, int, str]]:
    events = [e for e in result.timeline if e.kind == "state" and e.node == node]
    spans = []
    for current, following in zip(events, [*events[1:], None], strict=False):
        stop = following.time if following is not None else end
        spans.append((current.time, stop, current.detail))
    return spans


def render_timeline(result: RunResult, title: str | None = None) -> str:
    end = max(result.end_time, 1)
    n = result.config.n_nodes
    plot_w = WIDTH - LEFT - RIGHT
    top = 70
    fault_y = top
    node_y = [top + 34 + i * (LANE_H + LANE_GAP) for i in range(n)]
    ops_y = node_y[-1] + LANE_H + 22
    height = ops_y + 96

    def x(t: float) -> float:
        return LEFT + plot_w * t / end

    out: list[str] = []
    add = out.append
    add(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" '
        f'width="{WIDTH}" height="{height}" font-family="Helvetica, Arial, sans-serif" '
        f'role="img" aria-label="Raft cluster timeline for seed {result.seed}">'
    )
    add(f"<title>{escape(title or f'Raft cluster timeline, seed {result.seed}')}</title>")
    add(f'<rect width="{WIDTH}" height="{height}" fill="#ffffff"/>')
    heading = title or f"Raft cluster timeline, seed {result.seed}"
    add(f'<text x="{LEFT}" y="30" font-size="18" font-weight="700" fill="#1f2430">')
    add(f"{escape(heading)}</text>")
    subtitle = f"{n} nodes, {result.config.duration_ms // 1000}s of faults then the network heals"
    add(f'<text x="{LEFT}" y="50" font-size="12" fill="#5b6475">{escape(subtitle)}</text>')

    # Time grid
    step = 1000
    for t in range(0, end + 1, step):
        add(
            f'<line x1="{x(t):.1f}" y1="{top - 6}" x2="{x(t):.1f}" y2="{ops_y + 22}" '
            'stroke="#e6e9ef" stroke-width="1"/>'
        )
        add(
            f'<text x="{x(t):.1f}" y="{ops_y + 38}" font-size="10" fill="#7a8394" '
            f'text-anchor="middle">{t // 1000}s</text>'
        )

    # Fault lane: network faults as bands.
    add(
        f'<text x="{LEFT - 10}" y="{fault_y + 14}" font-size="12" fill="#3a4152" text-anchor="end">'
    )
    add("network</text>")
    add(
        f'<rect x="{LEFT}" y="{fault_y}" width="{plot_w}" height="{LANE_H}" fill="#f6f7fa" rx="3"/>'
    )
    for fault in fault_intervals(result.timeline, result.config.duration_ms):
        if fault.kind in ("partition", "isolate", "flaky"):
            color = "#d6455d" if fault.kind != "flaky" else "#e8a838"
            width = max(2.0, x(fault.end) - x(fault.start))
            add(
                f'<rect x="{x(fault.start):.1f}" y="{fault_y}" width="{width:.1f}" '
                f'height="{LANE_H}" fill="{color}" opacity="0.85" rx="3">'
                f"<title>{escape(fault.kind)} {fault.start}-{fault.end} ms</title></rect>"
            )

    # Node lanes
    for i in range(n):
        y = node_y[i]
        add(f'<text x="{LEFT - 10}" y="{y + 14}" font-size="12" fill="#3a4152" text-anchor="end">')
        add(f"node {i}</text>")
        add(f'<rect x="{LEFT}" y="{y}" width="{plot_w}" height="{LANE_H}" fill="#f6f7fa" rx="3"/>')
        for start, stop, role in _role_intervals(result, i, end):
            width = max(0.8, x(stop) - x(start))
            add(
                f'<rect x="{x(start):.1f}" y="{y}" width="{width:.1f}" height="{LANE_H}" '
                f'fill="{COLORS.get(role, "#999")}"><title>node {i}: {role}, '
                f"{start}-{stop} ms</title></rect>"
            )
        for e in result.timeline:
            if e.node == i and e.kind in ("torn", "vote-crash"):
                px = x(e.time)
                add(
                    f'<path d="M{px - 4:.1f} {y - 5} L{px + 4:.1f} {y - 5} L{px:.1f} {y}Z" '
                    f'fill="#d6455d"><title>{escape(e.detail)}</title></path>'
                )

    # Client lane: one tick per successful operation.
    add(f'<text x="{LEFT - 10}" y="{ops_y + 14}" font-size="12" fill="#3a4152" text-anchor="end">')
    add("clients ok</text>")
    add(f'<rect x="{LEFT}" y="{ops_y}" width="{plot_w}" height="{LANE_H}" fill="#f6f7fa" rx="3"/>')
    for op in result.history:
        if op.response is not None:
            add(
                f'<line x1="{x(op.response):.1f}" y1="{ops_y + 3}" x2="{x(op.response):.1f}" '
                f'y2="{ops_y + LANE_H - 3}" stroke="#2e9e5b" stroke-width="1"/>'
            )

    # Violation marker
    if result.violation is not None:
        vx = x(min(result.violation.time, end))
        add(
            f'<line x1="{vx:.1f}" y1="{top - 10}" x2="{vx:.1f}" y2="{ops_y + 22}" '
            'stroke="#d6455d" stroke-width="2" stroke-dasharray="5 3"/>'
        )
        label = f"{result.violation.kind} violated at {result.violation.time} ms"
        anchor = "end" if vx > WIDTH * 0.6 else "start"
        dx = -6 if anchor == "end" else 6
        add(
            f'<text x="{vx + dx:.1f}" y="{top - 14}" font-size="12" font-weight="700" '
            f'fill="#d6455d" text-anchor="{anchor}">{escape(label)}</text>'
        )

    # Legend
    ly = height - 26
    lx: float = LEFT
    for name, color in (
        ("leader", COLORS["leader"]),
        ("follower", COLORS["follower"]),
        ("candidate", COLORS["candidate"]),
        ("crashed", COLORS["down"]),
        ("partition / isolated leader", "#d6455d"),
        ("lossy network", "#e8a838"),
    ):
        add(f'<rect x="{lx}" y="{ly - 10}" width="12" height="12" fill="{color}" rx="2"/>')
        add(f'<text x="{lx + 18}" y="{ly}" font-size="11" fill="#3a4152">{escape(name)}</text>')
        lx += 30 + 6.4 * len(name)
    add("</svg>")
    return "\n".join(out) + "\n"

#!/usr/bin/env python3
"""Generate the README diagrams for the hcom fork as self-contained SVGs."""
import html
import os
import sys

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))

BG = "#0d1117"
PANEL = "#161b22"
BORDER = "#30363d"
FG = "#e6edf3"
MUTED = "#8b949e"
BLUE = "#58a6ff"
GREEN = "#3fb950"
AMBER = "#d29922"
RED = "#f85149"
PURPLE = "#bc8cff"
MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace'
SANS = '-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif'
COLORS = {"blue": BLUE, "green": GREEN, "amber": AMBER, "red": RED, "purple": PURPLE, "muted": MUTED, "fg": FG}


def esc(s):
    return html.escape(s, quote=True)


class Svg:
    def __init__(self, w, h, title, desc):
        self.w, self.h = w, h
        self.parts = []
        self.title, self.desc = title, desc

    # --- primitives -------------------------------------------------------
    def rect(self, x, y, w, h, fill="none", stroke=None, rx=6, dash=None, sw=1):
        s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}"'
        if stroke:
            s += f' stroke="{stroke}" stroke-width="{sw}"'
        if dash:
            s += f' stroke-dasharray="{dash}"'
        self.parts.append(s + "/>")

    def circle(self, cx, cy, r, fill):
        self.parts.append(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}"/>')

    def text(self, x, y, spans, size=11, mono=True, anchor="start", weight="normal", color=FG, ls=None):
        if isinstance(spans, str):
            spans = [(spans, color)]
        fam = MONO if mono else SANS
        attrs = f'x="{x}" y="{y}" font-family=\'{fam}\' font-size="{size}" text-anchor="{anchor}" font-weight="{weight}"'
        if ls:
            attrs += f' letter-spacing="{ls}"'
        attrs += ' xml:space="preserve"'
        inner = "".join(f'<tspan fill="{c}">{esc(t)}</tspan>' for t, c in spans)
        self.parts.append(f"<text {attrs}>{inner}</text>")

    def line(self, x1, y1, x2, y2, color, dash=None, sw=1.4, arrow=True):
        s = f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"'
        if dash:
            s += f' stroke-dasharray="{dash}"'
        if arrow:
            s += f' marker-end="url(#arr-{self._key(color)})"'
        self.parts.append(s + "/>")

    def path(self, d, color, dash=None, sw=1.4, arrow=True):
        s = f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw}" stroke-linejoin="round"'
        if dash:
            s += f' stroke-dasharray="{dash}"'
        if arrow:
            s += f' marker-end="url(#arr-{self._key(color)})"'
        self.parts.append(s + "/>")

    @staticmethod
    def _key(color):
        for k, v in COLORS.items():
            if v == color:
                return k
        raise KeyError(color)

    # --- composites -------------------------------------------------------
    def panel(self, x, y, w, h, title):
        self.rect(x, y, w, h, fill=PANEL, stroke=BORDER, rx=8)
        self.rect(x, y, w, 26, fill="#1c2129", stroke=None, rx=8)
        self.rect(x, y + 14, w, 12, fill="#1c2129", stroke=None, rx=0)  # square lower corners of bar
        self.line(x, y + 26, x + w, y + 26, BORDER, sw=1, arrow=False)
        for i, c in enumerate(("#ff5f57", "#febc2e", "#28c840")):
            self.circle(x + 14 + i * 16, y + 13, 5, c)
        self.text(x + 62, y + 17, title, size=11, mono=False, color=MUTED)

    def box(self, x, y, w, h, color, lines, dash=None, fill=PANEL):
        self.rect(x, y, w, h, fill=fill, stroke=color, rx=6, dash=dash, sw=1.3)
        n = len(lines)
        # vertically distribute lines
        gap = 13
        total = sum(l[1] for l in lines) + gap * (n - 1) * 0.35
        cy = y + h / 2 - total / 2
        for text, size, col, weight in lines:
            cy += size
            self.text(x + w / 2, cy, text, size=size, mono=False, anchor="middle", weight=weight, color=col)
            cy += gap * 0.35

    def label(self, x, y, s, color=MUTED, size=10, anchor="middle", weight="normal", mono=False):
        self.text(x, y, s, size=size, mono=mono, anchor=anchor, weight=weight, color=color)

    # --- output -------------------------------------------------------------
    def render(self):
        defs = "".join(
            f'<marker id="arr-{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="{v}"/></marker>'
            for k, v in COLORS.items()
        )
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" viewBox="0 0 {self.w} {self.h}" role="img" aria-labelledby="t d">\n'
            f"<title id=\"t\">{esc(self.title)}</title>\n<desc id=\"d\">{esc(self.desc)}</desc>\n<defs>{defs}</defs>\n"
            f'<rect x="0.5" y="0.5" width="{self.w - 1}" height="{self.h - 1}" rx="14" fill="{BG}" stroke="{BORDER}"/>\n'
        )
        return head + "\n".join(self.parts) + "\n</svg>\n"


# ---------------------------------------------------------------------------
# Diagram 1: interactive review loop across two terminal windows
# ---------------------------------------------------------------------------
def review_loop():
    s = Svg(
        980,
        624,
        "hcom review loop across two terminal windows",
        "A developer agent in one terminal starts hcom review; hcom delivers structured review messages "
        "to a reviewer agent in another terminal and back, alternating awaiting_review and awaiting_developer "
        "until LGTM, the round limit, or cancel.",
    )
    s.text(28, 32, "hcom review  —  two agents, two windows, one loop", size=16, mono=False, weight="600")
    s.text(
        28, 50,
        "One human prompt starts it. Hooks deliver every structured message and wake the idle peer, so the loop advances by itself.",
        size=11.5, mono=False, color=MUTED,
    )

    ax, bx, py, pw, ph = 28, 548, 66, 404, 282
    s.panel(ax, py, pw, ph, "Terminal 1 · hcom claude · luna (developer)")
    s.panel(bx, py, pw, ph, "Terminal 2 · hcom codex · kova (reviewer)")
    y0, lh = py + 42, 17

    def lines(x, rows):
        for i, row in enumerate(rows):
            if row:
                s.text(x + 12, y0 + i * lh, row)

    lines(ax, [
        [("you › ", BLUE), ("ask kova to review my change, keep fixing", FG)],
        [("      until LGTM, at most 3 rounds", FG)],
        [("$ ", MUTED), ("hcom review start @kova --max-rounds 3 \\", FG)],
        [("    --name luna -- 'review: fix nil deref'", FG)],
        [("  rv-3f9a started · waiting for delivery …", MUTED)],
        None,
        [("[hcom-review rv-3f9a | round 1/3 | awaiting_developer]", AMBER)],
        [("  Reviewer requested changes: nil guard missing …", MUTED)],
        [("$ ", MUTED), ("hcom review fixed rv-3f9a --round 1 --name luna \\", FG)],
        [("    -- 'added nil guard + regression test'", FG)],
        [("  waiting for delivery …", MUTED)],
        None,
        [("[hcom-review rv-3f9a | round 2/3 | approved]  ✓", GREEN)],
        [("  Report completion to the user.", MUTED)],
    ])
    lines(bx, [
        [("kova › ", PURPLE), ("idle — nothing to do yet", MUTED)],
        None,
        [("[hcom-review rv-3f9a | round 1/3 | awaiting_review]", BLUE)],
        [("  Role: reviewer. Review the change; do not modify it.", MUTED)],
        [("$ ", MUTED), ("hcom review verdict rv-3f9a --round 1 --name kova \\", FG)],
        [("    --request-changes -- 'nil guard missing in parse()'", FG)],
        [("  waiting for delivery …", MUTED)],
        None,
        [("[hcom-review rv-3f9a | round 2/3 | awaiting_review]", BLUE)],
        [("  Submission: added nil guard + regression test", MUTED)],
        [("$ ", MUTED), ("hcom review verdict rv-3f9a --round 2 --name kova \\", FG)],
        [("    --lgtm -- 'LGTM'", FG)],
        [("  review loop complete", MUTED)],
    ])

    # arrows across the gap (A right edge = 432, B left edge = 548)
    ar, bl, mid = ax + pw, bx, (ax + pw + bx) / 2

    def row(i):
        return y0 + i * lh - 4

    s.line(ar + 4, row(3), bl - 4, row(2), BLUE)
    s.label(mid, row(2) - 8, "round 1", BLUE)
    s.line(bl - 4, row(5), ar + 4, row(6), AMBER)
    s.label(mid, row(5) - 8, "wakes luna", AMBER)
    s.line(ar + 4, row(9), bl - 4, row(8), BLUE)
    s.label(mid, row(8) - 8, "round 2", BLUE)
    s.line(bl - 4, row(11), ar + 4, row(12), GREEN)
    s.label(mid, row(11) - 8, "LGTM", GREEN)

    # state machine strip
    sy = 386
    s.text(28, sy, "STATE MACHINE", size=10.5, mono=False, color=MUTED, weight="600", ls=1.2)
    s.text(
        140, sy,
        "only structured  hcom review …  commands move it — plain chat never does",
        size=10.5, mono=False, color=MUTED,
    )
    bh = 32
    ry = sy + 22  # 408
    # awaiting_review
    s.box(100, ry, 160, bh, BLUE, [("awaiting_review", 11.5, BLUE, "600")])
    # awaiting_developer
    s.box(470, ry, 170, bh, AMBER, [("awaiting_developer", 11.5, AMBER, "600")])
    ty = ry + 118  # 526
    s.box(40, ty, 140, bh, GREEN, [("approved", 11.5, GREEN, "600")])
    s.box(230, ty, 140, bh, RED, [("max_rounds", 11.5, RED, "600")])
    s.box(770, ry + 58, 130, bh, MUTED, [("canceled", 11.5, MUTED, "600")], dash="4 3")

    # review -> developer (request changes)
    s.line(260, ry + 10, 470, ry + 10, AMBER)
    s.label(365, ry - 4, "verdict --request-changes", AMBER, mono=True, size=10)
    # developer -> review (fixed / rebut)
    s.line(470, ry + 24, 260, ry + 24, BLUE)
    s.label(365, ry + 52, "fixed | rebut   →  round + 1", BLUE, mono=True, size=10)
    # review -> approved
    s.line(150, ry + bh, 115, ty, GREEN)
    s.label(96, ry + 80, "verdict --lgtm", GREEN, mono=True, size=10, anchor="end")
    # review -> max_rounds (request changes at the last round)
    s.line(210, ry + bh, 295, ty, RED)
    s.label(262, ry + 80, "request-changes at round N/N", RED, mono=True, size=10, anchor="start")
    # max_rounds -> awaiting_developer (extend)
    s.path(f"M370,{ty + 16} C 480,{ty + 16} 520,{ry + 70} 555,{ry + bh + 2}", MUTED, dash="4 3")
    s.label(478, ty + 34, "extend --max-rounds", MUTED, mono=True, size=10)
    # cancel
    s.line(640, ry + 16, 770, ry + 74, MUTED, dash="4 3")
    s.label(712, ry + 32, "cancel — either side, any state", MUTED, size=10, anchor="start")

    s.text(
        28, 600,
        "Works between any two local top-level Claude Code / Codex agents launched by hcom, in separate windows or panes.   hcom review --help",
        size=11, mono=False, color=MUTED,
    )
    return s.render()


# ---------------------------------------------------------------------------
# Diagram 2: hcom arch — blank Architect + in-process supervisor
# ---------------------------------------------------------------------------
def architect_lane():
    s = Svg(
        980,
        664,
        "hcom arch: blank Architect plus in-process Developer/Reviewer supervisor",
        "The human types the first prompt into a blank interactive Architect, inspects and approves a typed plan, "
        "then an in-process supervisor runs a fresh no-TUI Developer and Reviewer(s) per task, resuming exact "
        "sessions for corrections until LGTM or the round budget, and auto-advances to the next task.",
    )
    s.text(28, 32, "hcom arch  —  talk to one blank Architect; a supervisor drives Developer → Reviewer for every task",
           size=16, mono=False, weight="600")
    s.text(
        28, 50,
        "You type the first prompt yourself. Nothing starts until you approve the typed plan. Workers are fresh no-TUI sessions that end with the Architect.",
        size=11.5, mono=False, color=MUTED,
    )

    px, py, pw, ph = 28, 66, 372, 494
    s.panel(px, py, pw, ph, "Terminal · hcom arch codex   (Architect)")
    y0, lh = py + 42, 17
    rows = [
        [("$ ", MUTED), ("cd ~/proj && hcom arch codex", FG)],
        [("  blank Codex Architect · no prompt injected", MUTED)],
        None,
        [("you › ", BLUE), ("Read TASKS.md, plan its two tasks,", FG)],
        [("      show me the plan, then drive it.", FG)],
        None,
        [("architect › ", PURPLE), ("typed plan v1 · hash 9c41…", FG)],
        [("  1  implement-fibonacci", FG)],
        [("     repo ~/proj · task.md · design.md", MUTED)],
        [("     review ≤ 5 rounds · clarifications ≤ 2", MUTED)],
        [("  2  test-fibonacci", FG)],
        [("     repo ~/proj · task.md · design.md", MUTED)],
        [("  authorized → session_approve_and_start", GREEN)],
        None,
        [("» session_wait", AMBER), ("  — sleeps until a real worker event", MUTED)],
        [("  ↳ task 1 · Developer final → review_requested", FG)],
        [("  ↳ task 1 · Reviewer1 → REQUEST_CHANGES", AMBER)],
        [("  ↳ task 1 · Developer resumed · commit amended", FG)],
        [("  ↳ task 1 · Reviewer1 → LGTM ✓", GREEN)],
        [("  ↳ task 2 · Developer final → review_requested", FG)],
        [("  ↳ task 2 · Reviewer1 → LGTM ✓", GREEN)],
        [("■ terminal · completed", GREEN)],
        [("  evidence: hcom-tasks/<run>/…/native-final", MUTED)],
        None,
        [("you › ", BLUE), ("(read the verdicts, discuss, or begin", FG)],
        [("      a fresh run in this same Architect)", FG)],
    ]
    for i, row in enumerate(rows):
        if row:
            s.text(px + 12, y0 + i * lh, row)

    # right column
    rx, rw = 428, 524
    s.text(rx, 84, "IN-PROCESS SUPERVISOR", size=10.5, mono=False, color=MUTED, weight="600", ls=1.2)
    s.text(rx + 186, 84, "fresh no-TUI workers per task · no daemon · dies with the Architect",
           size=10.5, mono=False, color=MUTED)

    # task 1 lane
    l1y, l1h = 96, 212
    s.rect(rx, l1y, rw, l1h, fill="none", stroke=BORDER, rx=8, dash="5 4")
    s.text(rx + 10, l1y + 17, "task 1 · implement-fibonacci", size=11, mono=False, weight="600")
    dev = (rx + 14, l1y + 56, 132, 58)
    s.box(*dev, BLUE, [("Developer", 12, FG, "600"), ("fresh Codex session · no TUI", 9.5, MUTED, "normal"),
                       ("signed-off candidate commit", 9.5, MUTED, "normal")])
    r1 = (rx + 206, l1y + 30, 156, 44)
    r2 = (rx + 206, l1y + 90, 156, 44)
    s.box(*r1, PURPLE, [("Reviewer1", 12, FG, "600"), ("Codex · fresh session", 9.5, MUTED, "normal")])
    s.box(*r2, PURPLE, [("Reviewer2 · --double-review", 10.5, FG, "600"), ("Claude · runs concurrently", 9.5, MUTED, "normal")],
          dash="4 3")
    vd = (rx + 392, l1y + 56, 118, 58)
    s.box(*vd, AMBER, [("verdict", 12, FG, "600"), ("same generation", 9.5, MUTED, "normal"),
                       ("every active Reviewer", 9.5, MUTED, "normal")])
    # arrows dev -> reviewers
    dx, dy = dev[0] + dev[2], dev[1] + dev[3] / 2
    s.line(dx + 3, dy - 6, r1[0] - 3, r1[1] + r1[3] / 2, BLUE)
    s.line(dx + 3, dy + 6, r2[0] - 3, r2[1] + r2[3] / 2, BLUE, dash="4 3")
    # reviewers -> verdict
    s.line(r1[0] + r1[2] + 3, r1[1] + r1[3] / 2, vd[0] - 3, vd[1] + vd[3] / 2 - 6, PURPLE)
    s.line(r2[0] + r2[2] + 3, r2[1] + r2[3] / 2, vd[0] - 3, vd[1] + vd[3] / 2 + 6, PURPLE, dash="4 3")
    # request changes loop: leaves the verdict's bottom-left, runs under the boxes, back into the Developer
    vx, vb = vd[0] + 16, vd[1] + vd[3]
    loop_y = l1y + l1h - 16
    s.path(f"M{vx},{vb + 2} L{vx},{loop_y} L{dev[0] + 44},{loop_y} L{dev[0] + 44},{dev[1] + dev[3] + 3}", AMBER)
    s.label(rx + 74, loop_y - 26, "REQUEST_CHANGES → exact-resume the same Developer,",
            AMBER, size=9.5, anchor="start")
    s.label(rx + 74, loop_y - 13, "amend the commit → exact-resume each Reviewer · round + 1",
            AMBER, size=9.5, anchor="start")
    # lgtm exit: straight down from the verdict's bottom-right into the next lane
    vcx = vd[0] + vd[2] - 22
    l2y, l2h = 328, 100
    s.line(vcx, vb + 2, vcx, l2y - 3, GREEN)
    s.label(vcx - 8, vb + 30, "LGTM", GREEN, size=10, anchor="end", weight="600")
    s.label(vcx - 8, vb + 42, "next task", GREEN, size=10, anchor="end", weight="600")

    # task 2 lane (compact)
    s.rect(rx, l2y, rw, l2h, fill="none", stroke=BORDER, rx=8, dash="5 4")
    s.text(rx + 10, l2y + 17, "task 2 · test-fibonacci", size=11, mono=False, weight="600")
    by = l2y + 32
    b1 = (rx + 16, by, 110, 34)
    b2 = (rx + 176, by, 110, 34)
    b3 = (rx + 336, by, 110, 34)
    s.box(*b1, BLUE, [("Developer", 11, FG, "600"), ("fresh session", 9, MUTED, "normal")])
    s.box(*b2, PURPLE, [("Reviewer(s)", 11, FG, "600"), ("fresh session(s)", 9, MUTED, "normal")])
    s.box(*b3, AMBER, [("verdict", 11, FG, "600"), ("same loop", 9, MUTED, "normal")])
    s.line(b1[0] + b1[2] + 3, by + 17, b2[0] - 3, by + 17, BLUE)
    s.line(b2[0] + b2[2] + 3, by + 17, b3[0] - 3, by + 17, PURPLE)
    s.line(b3[0] + b3[2] + 3, by + 17, rx + rw - 14, by + 17, GREEN)
    s.label(rx + 10, l2y + 86, "starts from the reviewed task-1 HEAD · fresh sessions · same correction loop",
            MUTED, size=9.5, anchor="start")
    s.text(rx + 10, l2y + l2h + 20, "…  task N", size=11, mono=False, color=MUTED)

    # outcomes
    oy = 466
    s.text(rx, oy, "PER TASK", size=10.5, mono=False, color=MUTED, weight="600", ls=1.2)
    s.text(rx + 70, oy, "→ what the supervisor does next", size=10.5, mono=False, color=MUTED)
    ob = oy + 12
    s.box(rx, ob, 150, 44, GREEN, [("lgtm", 11.5, GREEN, "600"), ("auto-advance", 9.5, MUTED, "normal")])
    s.box(rx + 164, ob, 170, 44, AMBER, [("review_exhausted", 11.5, AMBER, "600"),
                                         ("round budget hit · auto-advance", 9.5, MUTED, "normal")])
    s.box(rx + 348, ob, 176, 44, RED, [("needs_human", 11.5, RED, "600"),
                                       ("ambiguous identity · process / transport failure", 8.5, MUTED, "normal")])
    s.text(rx, ob + 66,
           "run: completed · needs_human · failed · canceled — immutable once terminal. A later request begins a fresh run",
           size=10.5, mono=False, color=MUTED)
    s.text(rx, ob + 80,
           "(new run ID, new worker sessions, re-approved plan) under the same still-open Architect.",
           size=10.5, mono=False, color=MUTED)

    # connectors between the Architect terminal and the supervisor
    ax_right = px + pw
    approve_y = y0 + 12 * lh - 4
    s.path(f"M{ax_right + 2},{approve_y} C {ax_right + 22},{approve_y} {rx - 26},{dy} {dev[0] - 3},{dy}", GREEN)
    wait_y = y0 + 15 * lh - 4
    s.path(f"M{rx - 2},{l1y + l1h - 40} C {rx - 22},{l1y + l1h - 40} {ax_right + 20},{wait_y} {ax_right + 3},{wait_y}",
           AMBER)

    # guardrails
    gy = 600
    s.line(28, gy - 18, 952, gy - 18, BORDER, sw=1, arrow=False)
    items = [
        "you type the first prompt",
        "typed plan shown before anything starts",
        "workers end with the Architect",
        "no push / install / release unless separately authorized",
    ]
    x = 28
    widths = [190, 262, 216, 320]
    for item, w in zip(items, widths):
        s.text(x, gy + 2, [("✓ ", GREEN), (item, FG)], size=11, mono=False)
        x += w
    s.text(28, gy + 24, "hcom arch --help   ·   docs/architect.md", size=10.5, mono=False, color=MUTED)
    return s.render()


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    for name, fn in (("review-loop.svg", review_loop), ("architect-lane.svg", architect_lane)):
        with open(os.path.join(OUT, name), "w", encoding="utf-8") as f:
            f.write(fn())
        print("wrote", os.path.join(OUT, name))

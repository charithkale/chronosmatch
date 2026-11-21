"""Terminal UIs: a curses live dashboard and a plain-text fallback."""
import time

SPARK = "▁▂▃▄▅▆▇█"


def sparkline(values, width):
    values = list(values)[-width:]
    if not values:
        return ""
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1
    return "".join(SPARK[int((v - lo) / span * (len(SPARK) - 1))] for v in values)


class PlainUI:
    """One status line per second. Used with --no-ui or when stdout is not a TTY."""

    def __init__(self):
        self._last = 0.0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def quit(self):
        return False

    def draw(self, st):
        now = time.time()
        e = st.get("engine")
        if not e or now - self._last < 1.0:
            return
        self._last = now
        print("[%s] orders=%d trades=%d backlog=%d p50=%.1fus p99=%.1fus max=%.1fus "
              "engine=%.0fns/order bid=%s ask=%s whales=%d" % (
                  e["backend"], e["orders"], e["trades"], e["backlog"], e["p50_us"],
                  e["p99_us"], e["max_us"], e["ns_per_order"], e["best_bid"], e["best_ask"],
                  e["whale_events"]), flush=True)


class CursesUI:
    def __init__(self):
        self.hist = []
        self.prev = None            # (time, orders) for orders/sec
        self.rate = 0.0
        self.whale_seen = 0
        self.whale_until = 0.0

    def __enter__(self):
        import curses, locale
        locale.setlocale(locale.LC_ALL, "")
        self.c = curses
        self.scr = curses.initscr()
        curses.noecho(); curses.cbreak(); curses.curs_set(0)
        self.scr.nodelay(True)
        curses.start_color(); curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_RED, -1)
        curses.init_pair(2, curses.COLOR_GREEN, -1)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        curses.init_pair(4, curses.COLOR_CYAN, -1)
        return self

    def __exit__(self, *exc):
        self.c.nocbreak(); self.c.echo(); self.c.curs_set(1); self.c.endwin()
        return False

    def quit(self):
        return self.scr.getch() in (ord("q"), ord("Q"), 27)

    def _put(self, y, x, text, attr=0):
        h, w = self.scr.getmaxyx()
        if 0 <= y < h and x < w:
            try:
                self.scr.addstr(y, x, text[: w - x - 1], attr)
            except self.c.error:
                pass

    def draw(self, st):
        c, e, f = self.c, st.get("engine"), st.get("feed", {})
        self.scr.erase()
        if not e:
            self._put(0, 0, "ChronosMatch - waiting for engine...", c.A_BOLD)
            self.scr.refresh()
            return
        now = time.time()
        if self.prev and now - self.prev[0] >= 0.5:
            self.rate = (e["orders"] - self.prev[1]) / (now - self.prev[0])
            self.prev = (now, e["orders"])
        elif not self.prev:
            self.prev = (now, e["orders"])
        self.hist.append(e["p99_us"])
        self.hist = self.hist[-120:]
        if e["whale_events"] > self.whale_seen:
            self.whale_seen = e["whale_events"]
            self.whale_until = now + 2.0

        self._put(0, 0, " ChronosMatch ", c.A_REVERSE | c.A_BOLD)
        self._put(0, 15, "backend: %s   |  press q to quit" % e["backend"])
        self._put(2, 0, "Throughput", c.A_BOLD)
        self._put(3, 2, "orders matched : %-12d  rate: %10.0f /s" % (e["orders"], self.rate))
        self._put(4, 2, "trades         : %-12d  ring backlog: %d" % (e["trades"], e["backlog"]))
        self._put(5, 2, "feed sent/drop : %s / %s" % (f.get("sent", "-"), f.get("dropped", "-")))
        self._put(7, 0, "Latency (order ingest -> match, microseconds)", c.A_BOLD)
        self._put(8, 2, "p50 %8.1f   p99 %8.1f   max %9.1f   engine %6.0f ns/order" % (
            e["p50_us"], e["p99_us"], e["max_us"], e["ns_per_order"]), c.color_pair(4))
        self._put(9, 2, "p99 " + sparkline(self.hist, 80), c.color_pair(3))
        self._put(11, 0, "Order book (price ticks / volume)", c.A_BOLD)
        bids, asks = e["bids"], e["asks"]
        y = 12
        for px, vol in reversed(asks):
            self._put(y, 2, "ASK %8d %8d %s" % (px, vol, "#" * min(40, vol // 50)), c.color_pair(1))
            y += 1
        spread = (e["best_ask"] - e["best_bid"]) if e["best_ask"] is not None and e["best_bid"] is not None else None
        self._put(y, 2, "---- spread: %s ----" % spread, c.A_DIM)
        y += 1
        for px, vol in bids:
            self._put(y, 2, "BID %8d %8d %s" % (px, vol, "#" * min(40, vol // 50)), c.color_pair(2))
            y += 1
        if now < self.whale_until:
            self._put(y + 1, 0, " WHALE ORDER: swept %d price levels, qty %d " % (
                e["whale_last_levels"], e["whale_last_qty"]), c.color_pair(1) | c.A_REVERSE | c.A_BOLD)
        self.scr.refresh()

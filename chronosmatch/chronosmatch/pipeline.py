"""Multi-process pipeline: feed -> [order ring] -> engine -> [trade ring] -> ledger.

Every arrow is a shared-memory ring buffer; the only pickled traffic is a 10 Hz
stats message to the dashboard, which is off the hot path.
"""
import asyncio
import gc
import multiprocessing as mp
import os
import queue
import shutil
import sys
import tempfile
import time
from time import perf_counter, perf_counter_ns

from .dashboard import CursesUI, PlainUI
from .ledger import ledger_process, summarize
from .ringbuffer import ORDER, TRADE, RingBuffer
from .simulator import MarketSimulator

MAX_PRICE = 200_000
BATCH = 2048


def _feed_process(order_path, rate, duration, seed, stop, stats_q):
    ring = RingBuffer.attach(order_path)
    sim = MarketSimulator(ring, rate=rate, seed=seed)
    asyncio.run(sim.run(duration, stop))
    stats_q.put(("feed", {"sent": sim.sent, "dropped": sim.dropped, "done": True}))
    ring.close()


def _percentile(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def _engine_process(order_path, trade_path, stats_q, stop, whale_levels):
    from .engine import BACKEND, OrderBook
    orders = RingBuffer.attach(order_path)
    trades = RingBuffer.attach(trade_path)
    book = OrderBook(max_price=MAX_PRICE, whale_levels=whale_levels)
    # Everything long-lived is allocated; from here on the collector never runs.
    gc.collect()
    gc.freeze()
    gc.disable()

    window, eng_ns, eng_orders, ledger_dropped = [], 0, 0, 0
    tot_ns = tot_orders = 0
    last_report = perf_counter()

    def report(final=False):
        nonlocal window, eng_ns, eng_orders
        w = sorted(window)
        bids, asks = book.depth(8)
        s = book.stats()
        s.update(
            backend=BACKEND, final=final, backlog=orders.backlog(),
            p50_us=_percentile(w, 0.50) / 1000 if w else 0.0,
            p99_us=_percentile(w, 0.99) / 1000 if w else 0.0,
            max_us=w[-1] / 1000 if w else 0.0,
            ns_per_order=tot_ns / tot_orders if tot_orders else 0.0,
            best_bid=book.best_bid(), best_ask=book.best_ask(), bids=bids, asks=asks,
            ledger_dropped=ledger_dropped)
        try:
            stats_q.put_nowait(("engine", s))
        except queue.Full:
            pass
        window, eng_ns, eng_orders = [], 0, 0

    while True:
        view, n = orders.peek(BATCH)
        if n == 0:
            if stop.is_set():
                break
            time.sleep(0)               # yield; a production engine would busy-spin a pinned core
        else:
            t0 = perf_counter_ns()
            book.process_batch(view, n)     # decodes + matches straight from shared memory
            dt = perf_counter_ns() - t0
            eng_ns += dt; tot_ns += dt
            eng_orders += n; tot_orders += n
            del view
            orders.advance(n)
            tr = book.drain_trades()
            if tr:
                pushed = trades.push_many(tr, TRADE)
                ledger_dropped += len(tr) - pushed
                window.extend(t[5] - t[4] for t in tr)
        if perf_counter() - last_report >= 0.1:
            report()
            last_report = perf_counter()
    report(final=True)
    orders.close(); trades.close()


def _pick_dir():
    for d in ("/dev/shm", tempfile.gettempdir()):
        try:
            st = os.statvfs(d)
            if st.f_bavail * st.f_frsize > 256 * 1024 * 1024:
                return tempfile.mkdtemp(prefix="chronos_", dir=d)
        except OSError:
            continue
    return tempfile.mkdtemp(prefix="chronos_")


def run_pipeline(rate=100_000, duration=20.0, ui="auto", db_path="chronos_ledger.db",
                 whale_levels=4, seed=7):
    """Run the whole system. ui: 'curses' | 'plain' | 'none' | 'auto'. Returns a summary dict."""
    if ui == "auto":
        ui = "curses" if sys.stdout.isatty() else "plain"
    ctx = mp.get_context("spawn")
    tmp = _pick_dir()
    order_path, trade_path = os.path.join(tmp, "orders.ring"), os.path.join(tmp, "trades.ring")
    orders = RingBuffer.create(order_path, 1 << 20, ORDER.size)
    trades = RingBuffer.create(trade_path, 1 << 18, TRADE.size)
    if os.path.exists(db_path):
        os.remove(db_path)
    stats_q = ctx.Queue(maxsize=1000)
    feed_stop, eng_stop, led_stop = ctx.Event(), ctx.Event(), ctx.Event()
    procs = {
        "feed": ctx.Process(target=_feed_process, args=(order_path, rate, duration, seed, feed_stop, stats_q)),
        "engine": ctx.Process(target=_engine_process, args=(order_path, trade_path, stats_q, eng_stop, whale_levels)),
        "ledger": ctx.Process(target=ledger_process, args=(trade_path, db_path, led_stop)),
    }
    state = {"engine": None, "feed": {}}

    def pump(block=0.05):
        try:
            kind, payload = stats_q.get(timeout=block)
        except queue.Empty:
            return
        state[kind] = payload
        while True:
            try:
                kind, payload = stats_q.get_nowait()
                state[kind] = payload
            except queue.Empty:
                break

    ui_obj = {"curses": CursesUI, "plain": PlainUI, "none": PlainUI}[ui]()
    quiet = ui == "none"
    try:
        for p in procs.values():
            p.start()
        with ui_obj:
            while procs["feed"].is_alive():
                pump()
                if not quiet:
                    ui_obj.draw(state)
                if ui_obj.quit():
                    feed_stop.set()
            procs["feed"].join()
            eng_stop.set()
            while procs["engine"].is_alive():
                pump()
                if not quiet:
                    ui_obj.draw(state)
            procs["engine"].join()
            for _ in range(5):          # collect the final stats messages
                pump(0.1)
            led_stop.set()
            procs["ledger"].join()
    finally:
        for p in procs.values():
            if p.is_alive():
                p.terminate()
        orders.close(); trades.close()
        shutil.rmtree(tmp, ignore_errors=True)

    summary = {"engine": state["engine"], "feed": state["feed"], "ledger": summarize(db_path),
               "db_path": db_path}
    return summary

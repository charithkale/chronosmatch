"""Benchmarks: IPC audit (zero-copy vs pickle) and raw matching-engine speed.

    python -m chronosmatch.bench ipc [--orders 1000000]
    python -m chronosmatch.bench engine [--orders 1000000]
"""
import argparse
import gc
import multiprocessing as mp
import os
import tempfile
from time import perf_counter, perf_counter_ns

from .ringbuffer import ORDER, U64, RingBuffer


# ---------------------------------------------------------------- IPC audit
def _producer(path, n, batch, ready, go):
    ring = RingBuffer.attach(path)
    blocks, ts = [], perf_counter_ns()
    for start in range(0, n, batch):
        m = min(batch, n - start)
        buf = bytearray(m * ORDER.size)
        for i in range(m):
            ORDER.pack_into(buf, i * ORDER.size, start + i + 1, i & 1, 10, 100_000, ts)
        blocks.append((bytes(buf), m))
    ready.set()
    go.wait()
    for data, m in blocks:
        while not ring.push_raw(data, m):
            pass                                    # ring full: spin until consumer catches up
    ring.close()


def _consumer(path, n, out_q):
    ring = RingBuffer.attach(path)
    got = bad = 0
    expect, t0 = 1, None
    while got < n:
        view, k = ring.peek(4096)
        if k == 0:
            continue
        if t0 is None:
            t0 = perf_counter()
        first = U64.unpack_from(view, 0)[0]
        last = U64.unpack_from(view, (k - 1) * ORDER.size)[0]
        if first != expect or last != expect + k - 1:
            bad += 1                                # lost / reordered / corrupted data
        expect += k
        got += k
        del view
        ring.advance(k)
    out_q.put((got, bad, perf_counter() - t0))
    ring.close()


def _pickle_producer(q, n):
    ts = perf_counter_ns()
    for i in range(n):
        q.put((i + 1, i & 1, 10, 100_000, ts))


def run_ipc(n=1_000_000, ring_capacity=1 << 16, pickle_n=200_000):
    ctx = mp.get_context("spawn")
    path = os.path.join(tempfile.mkdtemp(prefix="chronos_bench_"), "ipc.ring")
    ring = RingBuffer.create(path, ring_capacity, ORDER.size)
    ready, go, out_q = ctx.Event(), ctx.Event(), ctx.Queue()
    cons = ctx.Process(target=_consumer, args=(path, n, out_q))
    prod = ctx.Process(target=_producer, args=(path, n, 1000, ready, go))
    cons.start(); prod.start()
    ready.wait()
    go.set()
    got, bad, secs = out_q.get(timeout=300)
    prod.join(); cons.join()
    ring.close()

    # baseline: multiprocessing.Queue (pickle + pipe), one message per order
    q = ctx.Queue()
    pp = ctx.Process(target=_pickle_producer, args=(q, pickle_n))
    t0 = perf_counter()
    pp.start()
    for _ in range(pickle_n):
        q.get()
    pickle_secs = perf_counter() - t0
    pp.join()

    zc, pk = got / secs, pickle_n / pickle_secs
    return {"orders": got, "integrity_errors": bad, "zero_copy_orders_per_s": zc,
            "pickle_orders_per_s": pk, "speedup": zc / pk}


# ------------------------------------------------------------ engine speed
def run_engine(n=1_000_000, batch=4096):
    from .engine import BACKEND, OrderBook
    from .simulator import MarketSimulator
    sim = MarketSimulator(ring=None, seed=11)
    buf = bytearray(n * ORDER.size)
    for start in range(0, n, 10_000):
        for i, o in enumerate(sim.make_orders(min(10_000, n - start))):
            ORDER.pack_into(buf, (start + i) * ORDER.size, *o[:-1], perf_counter_ns())
    book = OrderBook(max_price=200_000)
    mv = memoryview(buf)
    gc.collect(); gc.disable()
    t0 = perf_counter()
    for off in range(0, n, batch):
        k = min(batch, n - off)
        book.process_batch(mv[off * ORDER.size:(off + k) * ORDER.size], k)
        book.drain_trades()
    secs = perf_counter() - t0
    gc.enable()
    return {"backend": BACKEND, "orders": n, "seconds": secs, "orders_per_s": n / secs,
            "ns_per_order": secs / n * 1e9, **book.stats()}


def main():
    ap = argparse.ArgumentParser(prog="chronosmatch.bench")
    ap.add_argument("what", choices=["ipc", "engine"])
    ap.add_argument("--orders", type=int, default=1_000_000)
    a = ap.parse_args()
    if a.what == "ipc":
        r = run_ipc(a.orders)
        print("IPC audit: %d orders, integrity errors: %d" % (r["orders"], r["integrity_errors"]))
        print("  zero-copy ring : %12.0f orders/s" % r["zero_copy_orders_per_s"])
        print("  pickle queue   : %12.0f orders/s" % r["pickle_orders_per_s"])
        print("  speedup        : %12.1fx" % r["speedup"])
    else:
        r = run_engine(a.orders)
        print("Engine (%s): %d orders in %.2fs -> %.0f orders/s, %.0f ns/order, %d trades" % (
            r["backend"], r["orders"], r["seconds"], r["orders_per_s"], r["ns_per_order"], r["trades"]))


if __name__ == "__main__":
    main()

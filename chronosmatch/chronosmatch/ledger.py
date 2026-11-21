"""Background ledger: flushes matched trades from the trade ring to SQLite."""
import sqlite3
import time

from .ringbuffer import RingBuffer, TRADE

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades(
    id INTEGER PRIMARY KEY, taker_id INTEGER, maker_id INTEGER,
    price INTEGER, qty INTEGER, ts_in INTEGER, ts_out INTEGER, latency_ns INTEGER);
"""


def connect(db_path):
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.executescript(SCHEMA)
    return con


def ledger_process(trade_ring_path, db_path, stop, result_q=None):
    ring = RingBuffer.attach(trade_ring_path)
    con = connect(db_path)
    total = 0
    while True:
        view, n = ring.peek(8192)
        if n == 0:
            if stop.is_set():
                break
            time.sleep(0.005)
            continue
        rows = [(a, b, p, q, ti, to, to - ti) for a, b, p, q, ti, to in TRADE.iter_unpack(view)]
        del view
        con.executemany(
            "INSERT INTO trades(taker_id,maker_id,price,qty,ts_in,ts_out,latency_ns) "
            "VALUES(?,?,?,?,?,?,?)", rows)
        con.commit()
        ring.advance(n)
        total += n
    con.commit()
    con.close()
    ring.close()
    if result_q is not None:
        result_q.put(("ledger", {"rows": total}))


def summarize(db_path):
    """Row count and latency percentiles (microseconds) straight from the ledger."""
    con = sqlite3.connect(db_path)
    (n,) = con.execute("SELECT COUNT(*) FROM trades").fetchone()
    out = {"rows": n}
    if n:
        def pct(p):
            off = min(n - 1, int(n * p))
            return con.execute("SELECT latency_ns FROM trades ORDER BY latency_ns LIMIT 1 OFFSET ?",
                               (off,)).fetchone()[0] / 1000.0
        out.update(p50_us=pct(0.50), p99_us=pct(0.99), max_us=pct(0.999999))
    con.close()
    return out

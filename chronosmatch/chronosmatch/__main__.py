"""CLI:  python -m chronosmatch run [--rate N] [--duration S] [--ui auto|curses|plain|none]
        python -m chronosmatch bench ipc|engine"""
import argparse
import sys


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "bench":
        from .bench import main as bench_main
        sys.argv = ["chronosmatch.bench"] + sys.argv[2:]
        return bench_main()
    ap = argparse.ArgumentParser(prog="chronosmatch")
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--rate", type=int, default=100_000, help="orders per second")
    ap.add_argument("--duration", type=float, default=20.0, help="seconds")
    ap.add_argument("--ui", default="auto", choices=["auto", "curses", "plain", "none"])
    ap.add_argument("--db", default="chronos_ledger.db")
    ap.add_argument("--whale-levels", type=int, default=4)
    a = ap.parse_args()
    from .engine import BACKEND
    from .pipeline import run_pipeline
    print("backend:", BACKEND)
    s = run_pipeline(a.rate, a.duration, a.ui, a.db, a.whale_levels)
    e, f, l = s["engine"], s["feed"], s["ledger"]
    print("\n=== ChronosMatch summary ===")
    print("feed   : sent %d, dropped %d" % (f.get("sent", 0), f.get("dropped", 0)))
    print("engine : %d orders, %d trades, %d whale sweeps, %.0f ns/order (%s)" % (
        e["orders"], e["trades"], e["whale_events"], e["ns_per_order"], e["backend"]))
    print("ledger : %d rows in %s" % (l["rows"], s["db_path"]))
    if l.get("rows"):
        print("latency: p50 %.1f us | p99 %.1f us | max %.1f us (ingest -> match)" % (
            l["p50_us"], l["p99_us"], l["max_us"]))


if __name__ == "__main__":
    main()

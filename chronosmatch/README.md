# ChronosMatch - zero-copy limit-order-book matching engine

Python ingestion writes orders into a memory-mapped ring buffer; a separate engine
process reads the **same RAM** and matches with price-time priority; matched trades
flow through a second ring to a SQLite ledger; a curses dashboard shows it live.

```
asyncio feed --> [orders.ring mmap] --> engine (Cython | Python) --> [trades.ring mmap] --> SQLite ledger
                                           |  10 Hz stats (only pickled traffic)
                                           v
                                     curses dashboard
```

## Run

```bash
pip install cython setuptools          # optional but recommended
python setup.py build_ext --inplace    # builds chronosmatch/_matcher*.so
python -m chronosmatch run --rate 100000 --duration 30      # live curses UI (q quits)
python -m chronosmatch run --ui plain --duration 10         # one status line per second
python -m chronosmatch bench ipc                            # 1M orders, zero-copy vs pickle
python -m chronosmatch bench engine                         # matching speed
python -m unittest discover -s tests -t . -v                # 14 tests
```
Without the compiled extension the engine falls back automatically to
`matcher_py.py` (same API, same results). `CHRONOS_BACKEND=python` forces the fallback.

## Modules (maps to the brief)

| Brief module | File |
|---|---|
| Cython Matching Engine (C structs, no Python objects in the loop) | `chronosmatch/_matcher.pyx` |
| Zero-Copy IPC Bus (mmap + struct) | `chronosmatch/ringbuffer.py` |
| Market Simulator (asyncio) | `chronosmatch/simulator.py` |
| Latency Monitor (curses) | `chronosmatch/dashboard.py` |
| Resiliency: async flush to ledger | `chronosmatch/ledger.py` |
| Wiring / GC control | `chronosmatch/pipeline.py` |

Design notes
* Records are fixed 32-byte structs (`<QB3xIqQ`: id, side, qty, price-ticks, ingest ts).
  The Cython `process_batch` casts the mmap memory to a C struct pointer and matches inside a
  `nogil` block, so no Python object is created per order.
* The order book is array-based: one FIFO list per integer price tick, an order-node pool with a
  free list, and a preallocated trade buffer. No malloc and no hashing on the hot path.
* The engine process calls `gc.freeze(); gc.disable()` after setup.
* Latency = engine match timestamp minus the producer's ingest timestamp (both `CLOCK_MONOTONIC`,
  `perf_counter_ns` in Python), so it includes time spent waiting in the ring.
* Backpressure: if the order ring is full the feed drops and counts the orders (as a real feed
  handler would); the ledger ring does the same and reports `ledger_dropped`.

## Measured here (1 vCPU sandbox, **pure-Python fallback engine**)

| Check | Result |
|---|---|
| Zero-copy ring, 1M orders between two processes | 8.3M orders/s, 0 integrity errors |
| `multiprocessing.Queue` (pickle), per-order | 0.15M orders/s (ring ~56x faster) |
| Pure-Python matching, 500k orders | ~440 ns/order |
| End-to-end, 20k-50k orders/s | p50 ~60-170 us; p99 is dominated by scheduler jitter because feed, engine, ledger and UI all share one core |

## Not verified / known limits
* **The Cython extension (`_matcher.pyx`) could not be compiled in my sandbox (no Cython, no network),
  so it is untested.** The test-suite automatically runs every matcher test against it once it is built,
  including a randomized comparison with a naive oracle, so run the tests after `build_ext`.
* The brief's "under 50 us" target is therefore unproven. Expect low microseconds per order from Cython
  on a normal multi-core machine; pin the engine to a core (`taskset -c 2`) for stable tail latency.
* Single-producer/single-consumer only. Correct on x86-64 (TSO); ARM would need memory fences.
* Limit orders only: no cancel/modify/IOC. A "whale" is any order that sweeps >= `--whale-levels` levels.
* The feed is a mock generator, not a real websocket client.

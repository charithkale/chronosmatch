# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False
"""Cython limit order book: price-time priority, zero Python objects in the hot loop.

Data layout (all C memory, allocated once in __cinit__):
  * price ladders: per-price FIFO queues (head/tail indices) for bids and asks,
    indexed directly by integer tick price (O(1) level lookup, no hashing)
  * an order-node pool with an intrusive free list (no malloc per order)
  * a preallocated trade buffer, drained in batches by drain_trades()

process_batch() decodes 32-byte order records straight out of the shared mmap
memory (same layout as ringbuffer.ORDER) inside a `nogil` block, so matching
never touches the Python allocator or the garbage collector.

Requires Cython >= 3.0 (uses `noexcept`).
"""
from libc.stdint cimport uint8_t, int32_t, uint32_t, int64_t, uint64_t
from libc.stdlib cimport calloc, malloc, free
from posix.time cimport clock_gettime, timespec, CLOCK_MONOTONIC


cdef struct Node:
    uint64_t oid
    uint64_t ts_in
    uint32_t qty
    int32_t next

cdef struct Trade:
    uint64_t taker
    uint64_t maker
    int64_t price
    uint64_t ts_in
    uint64_t ts_out
    uint32_t qty

cdef packed struct OrderRec:        # identical to struct "<QB3xIqQ" (32 bytes)
    uint64_t oid
    uint8_t side
    uint8_t pad0
    uint8_t pad1
    uint8_t pad2
    uint32_t qty
    int64_t price
    uint64_t ts


cdef inline uint64_t now_ns() noexcept nogil:
    cdef timespec t
    clock_gettime(CLOCK_MONOTONIC, &t)      # same clock as time.perf_counter_ns() on Linux
    return <uint64_t>t.tv_sec * 1000000000 + <uint64_t>t.tv_nsec


cdef class OrderBook:
    backend = "cython"

    cdef int64_t max_price
    cdef Node* pool
    cdef int32_t free_head
    cdef int32_t* bid_head
    cdef int32_t* bid_tail
    cdef int32_t* ask_head
    cdef int32_t* ask_tail
    cdef uint64_t* bid_vol
    cdef uint64_t* ask_vol
    cdef int64_t best_bid_px          # -1 when empty
    cdef int64_t best_ask_px          # max_price when empty
    cdef Trade* trades
    cdef Py_ssize_t trade_cap
    cdef Py_ssize_t trade_n
    cdef int whale_levels
    cdef public uint64_t orders_processed
    cdef public uint64_t trades_total
    cdef public uint64_t trades_dropped
    cdef public uint64_t rejected
    cdef public uint64_t whale_events
    cdef public uint32_t whale_last_levels
    cdef public uint64_t whale_last_qty

    def __cinit__(self, int64_t max_price=200000, int32_t pool_size=1048576,
                  Py_ssize_t trade_cap=65536, int whale_levels=3):
        cdef int64_t i
        self.max_price = max_price
        self.whale_levels = whale_levels
        self.trade_cap = trade_cap
        self.pool = <Node*>malloc(pool_size * sizeof(Node))
        self.bid_head = <int32_t*>malloc(max_price * sizeof(int32_t))
        self.bid_tail = <int32_t*>malloc(max_price * sizeof(int32_t))
        self.ask_head = <int32_t*>malloc(max_price * sizeof(int32_t))
        self.ask_tail = <int32_t*>malloc(max_price * sizeof(int32_t))
        self.bid_vol = <uint64_t*>calloc(max_price, sizeof(uint64_t))
        self.ask_vol = <uint64_t*>calloc(max_price, sizeof(uint64_t))
        self.trades = <Trade*>malloc(trade_cap * sizeof(Trade))
        if (self.pool == NULL or self.bid_head == NULL or self.bid_tail == NULL or
                self.ask_head == NULL or self.ask_tail == NULL or self.bid_vol == NULL or
                self.ask_vol == NULL or self.trades == NULL):
            raise MemoryError()
        for i in range(max_price):
            self.bid_head[i] = -1
            self.bid_tail[i] = -1
            self.ask_head[i] = -1
            self.ask_tail[i] = -1
        for i in range(pool_size - 1):
            self.pool[i].next = <int32_t>(i + 1)
        self.pool[pool_size - 1].next = -1
        self.free_head = 0
        self.best_bid_px = -1
        self.best_ask_px = max_price
        self.trade_n = 0

    def __dealloc__(self):
        free(self.pool); free(self.bid_head); free(self.bid_tail)
        free(self.ask_head); free(self.ask_tail)
        free(self.bid_vol); free(self.ask_vol); free(self.trades)

    # ---- internals (pure C, no GIL) ---------------------------------------
    cdef inline int32_t _alloc(self) noexcept nogil:
        cdef int32_t idx = self.free_head
        if idx != -1:
            self.free_head = self.pool[idx].next
        return idx

    cdef inline void _release(self, int32_t idx) noexcept nogil:
        self.pool[idx].next = self.free_head
        self.free_head = idx

    cdef inline void _record(self, uint64_t taker, uint64_t maker, int64_t px,
                             uint32_t qty, uint64_t ts_in, uint64_t ts_out) noexcept nogil:
        cdef Trade* t
        self.trades_total += 1
        if self.trade_n >= self.trade_cap:
            self.trades_dropped += 1
            return
        t = &self.trades[self.trade_n]
        t.taker = taker; t.maker = maker; t.price = px
        t.qty = qty; t.ts_in = ts_in; t.ts_out = ts_out
        self.trade_n += 1

    cdef inline void _advance_ask(self, int64_t px) noexcept nogil:
        px += 1
        while px < self.max_price and self.ask_head[px] == -1:
            px += 1
        self.best_ask_px = px            # == max_price when no asks remain

    cdef inline void _advance_bid(self, int64_t px) noexcept nogil:
        px -= 1
        while px >= 0 and self.bid_head[px] == -1:
            px -= 1
        self.best_bid_px = px            # == -1 when no bids remain

    cdef void _submit(self, uint64_t oid, uint8_t side, int64_t price,
                      uint32_t qty, uint64_t ts_in) noexcept nogil:
        cdef int64_t px
        cdef int32_t idx, tail
        cdef uint32_t fill
        cdef uint32_t orig = qty
        cdef int levels = 0
        cdef uint64_t ts_out = 0
        self.orders_processed += 1
        if qty == 0 or price < 0 or price >= self.max_price:
            self.rejected += 1
            return

        if side == 0:                                         # BUY: sweep asks
            while qty > 0 and self.best_ask_px < self.max_price and self.best_ask_px <= price:
                px = self.best_ask_px
                levels += 1
                if ts_out == 0:
                    ts_out = now_ns()
                while qty > 0 and self.ask_head[px] != -1:
                    idx = self.ask_head[px]
                    fill = qty if qty < self.pool[idx].qty else self.pool[idx].qty
                    self._record(oid, self.pool[idx].oid, px, fill, ts_in, ts_out)
                    qty -= fill
                    self.pool[idx].qty -= fill
                    self.ask_vol[px] -= fill
                    if self.pool[idx].qty == 0:
                        self.ask_head[px] = self.pool[idx].next
                        self._release(idx)
                if self.ask_head[px] == -1:
                    self.ask_tail[px] = -1
                    self._advance_ask(px)
            if qty > 0:                                       # rest remainder on bid side
                idx = self._alloc()
                if idx == -1:
                    self.rejected += 1
                    return
                self.pool[idx].oid = oid; self.pool[idx].qty = qty
                self.pool[idx].ts_in = ts_in; self.pool[idx].next = -1
                tail = self.bid_tail[price]
                if tail == -1:
                    self.bid_head[price] = idx
                else:
                    self.pool[tail].next = idx
                self.bid_tail[price] = idx
                self.bid_vol[price] += qty
                if price > self.best_bid_px:
                    self.best_bid_px = price
        else:                                                 # SELL: sweep bids
            while qty > 0 and self.best_bid_px >= 0 and self.best_bid_px >= price:
                px = self.best_bid_px
                levels += 1
                if ts_out == 0:
                    ts_out = now_ns()
                while qty > 0 and self.bid_head[px] != -1:
                    idx = self.bid_head[px]
                    fill = qty if qty < self.pool[idx].qty else self.pool[idx].qty
                    self._record(oid, self.pool[idx].oid, px, fill, ts_in, ts_out)
                    qty -= fill
                    self.pool[idx].qty -= fill
                    self.bid_vol[px] -= fill
                    if self.pool[idx].qty == 0:
                        self.bid_head[px] = self.pool[idx].next
                        self._release(idx)
                if self.bid_head[px] == -1:
                    self.bid_tail[px] = -1
                    self._advance_bid(px)
            if qty > 0:                                       # rest remainder on ask side
                idx = self._alloc()
                if idx == -1:
                    self.rejected += 1
                    return
                self.pool[idx].oid = oid; self.pool[idx].qty = qty
                self.pool[idx].ts_in = ts_in; self.pool[idx].next = -1
                tail = self.ask_tail[price]
                if tail == -1:
                    self.ask_head[price] = idx
                else:
                    self.pool[tail].next = idx
                self.ask_tail[price] = idx
                self.ask_vol[price] += qty
                if price < self.best_ask_px:
                    self.best_ask_px = price

        if levels >= self.whale_levels:
            self.whale_events += 1
            self.whale_last_levels = levels
            self.whale_last_qty = orig - qty

    # ---- public API (same as matcher_py.OrderBook) --------------------------
    def submit(self, uint64_t oid, int side, int64_t price, uint32_t qty, uint64_t ts_in):
        self._submit(oid, <uint8_t>side, price, qty, ts_in)

    def process_batch(self, const unsigned char[::1] buf, Py_ssize_t count):
        """Match `count` packed 32-byte orders read directly from `buf` (e.g. the mmap)."""
        cdef const OrderRec* rec
        cdef Py_ssize_t i
        if count <= 0:
            return
        if buf.shape[0] < count * sizeof(OrderRec):
            raise ValueError("buffer too small")
        rec = <const OrderRec*>&buf[0]
        with nogil:
            for i in range(count):
                self._submit(rec[i].oid, rec[i].side, rec[i].price, rec[i].qty, rec[i].ts)

    def drain_trades(self):
        cdef Py_ssize_t i
        cdef list out = []
        for i in range(self.trade_n):
            out.append((self.trades[i].taker, self.trades[i].maker, self.trades[i].price,
                        self.trades[i].qty, self.trades[i].ts_in, self.trades[i].ts_out))
        self.trade_n = 0
        return out

    def best_bid(self):
        return None if self.best_bid_px < 0 else int(self.best_bid_px)

    def best_ask(self):
        return None if self.best_ask_px >= self.max_price else int(self.best_ask_px)

    def depth(self, int n=8):
        cdef list bids = []
        cdef list asks = []
        cdef int64_t px
        px = self.best_bid_px
        while px >= 0 and len(bids) < n:
            if self.bid_head[px] != -1:
                bids.append((int(px), int(self.bid_vol[px])))
            px -= 1
        px = self.best_ask_px
        while px < self.max_price and len(asks) < n:
            if self.ask_head[px] != -1:
                asks.append((int(px), int(self.ask_vol[px])))
            px += 1
        return bids, asks

    def stats(self):
        return {"orders": self.orders_processed, "trades": self.trades_total,
                "rejected": self.rejected, "trades_dropped": self.trades_dropped,
                "whale_events": self.whale_events,
                "whale_last_levels": self.whale_last_levels,
                "whale_last_qty": self.whale_last_qty}

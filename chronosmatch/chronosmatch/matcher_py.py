"""Pure-Python limit order book (price-time priority).

Reference implementation and automatic fallback when the Cython extension
is not compiled. It exposes exactly the same API as `_matcher.pyx`.

Prices are integer ticks. side: 0 = buy, 1 = sell. Trades execute at the
resting (maker) order's price.
"""
import heapq
from collections import deque
from time import perf_counter_ns

from .ringbuffer import ORDER


class OrderBook:
    backend = "python"

    def __init__(self, max_price=200_000, pool_size=0, trade_cap=0, whale_levels=3):
        self.max_price = max_price
        self.whale_levels = whale_levels
        self._bids, self._asks = {}, {}          # price -> deque([id, qty, ts])
        self._bid_heap, self._ask_heap = [], []  # -price / price min-heaps
        self._trades = []
        self.orders_processed = self.trades_total = self.rejected = 0
        self.trades_dropped = 0
        self.whale_events = self.whale_last_levels = self.whale_last_qty = 0

    def submit(self, oid, side, price, qty, ts_in):
        self.orders_processed += 1
        if qty <= 0 or price < 0 or price >= self.max_price:
            self.rejected += 1
            return
        orig, levels, ts_out = qty, 0, 0
        trades = self._trades
        if side == 0:                                   # BUY sweeps asks
            book, heap = self._asks, self._ask_heap
            while qty and heap and heap[0] <= price:
                px = heap[0]
                level = book[px]
                levels += 1
                if not ts_out:
                    ts_out = perf_counter_ns()
                while qty and level:
                    maker = level[0]
                    fill = qty if qty < maker[1] else maker[1]
                    trades.append((oid, maker[0], px, fill, ts_in, ts_out))
                    self.trades_total += 1
                    qty -= fill
                    maker[1] -= fill
                    if not maker[1]:
                        level.popleft()
                if not level:
                    del book[px]
                    heapq.heappop(heap)
            if qty:
                lv = self._bids.get(price)
                if lv is None:
                    lv = self._bids[price] = deque()
                    heapq.heappush(self._bid_heap, -price)
                lv.append([oid, qty, ts_in])
        else:                                           # SELL sweeps bids
            book, heap = self._bids, self._bid_heap
            while qty and heap and -heap[0] >= price:
                px = -heap[0]
                level = book[px]
                levels += 1
                if not ts_out:
                    ts_out = perf_counter_ns()
                while qty and level:
                    maker = level[0]
                    fill = qty if qty < maker[1] else maker[1]
                    trades.append((oid, maker[0], px, fill, ts_in, ts_out))
                    self.trades_total += 1
                    qty -= fill
                    maker[1] -= fill
                    if not maker[1]:
                        level.popleft()
                if not level:
                    del book[px]
                    heapq.heappop(heap)
            if qty:
                lv = self._asks.get(price)
                if lv is None:
                    lv = self._asks[price] = deque()
                    heapq.heappush(self._ask_heap, price)
                lv.append([oid, qty, ts_in])
        if levels >= self.whale_levels:
            self.whale_events += 1
            self.whale_last_levels = levels
            self.whale_last_qty = orig - qty

    def process_batch(self, buf, count):
        """Decode `count` 32-byte order records from a buffer and match them."""
        mv = memoryview(buf).cast("B")
        if len(mv) < count * ORDER.size:
            raise ValueError("buffer too small")
        submit = self.submit
        for oid, side, qty, price, ts in ORDER.iter_unpack(mv[:count * ORDER.size]):
            submit(oid, side, price, qty, ts)

    def drain_trades(self):
        t, self._trades = self._trades, []
        return t

    def best_bid(self):
        return -self._bid_heap[0] if self._bid_heap else None

    def best_ask(self):
        return self._ask_heap[0] if self._ask_heap else None

    def depth(self, n=8):
        bids = [(-p, sum(o[1] for o in self._bids[-p])) for p in heapq.nsmallest(n, self._bid_heap)]
        asks = [(p, sum(o[1] for o in self._asks[p])) for p in heapq.nsmallest(n, self._ask_heap)]
        return bids, asks

    def stats(self):
        return {
            "orders": self.orders_processed, "trades": self.trades_total,
            "rejected": self.rejected, "trades_dropped": self.trades_dropped,
            "whale_events": self.whale_events,
            "whale_last_levels": self.whale_last_levels,
            "whale_last_qty": self.whale_last_qty,
        }

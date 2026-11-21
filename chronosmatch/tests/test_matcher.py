import random
import unittest

from chronosmatch import matcher_py
from chronosmatch.ringbuffer import ORDER

BACKENDS = [matcher_py.OrderBook]
try:
    from chronosmatch._matcher import OrderBook as CyBook
    BACKENDS.append(CyBook)
except ImportError:
    pass


class Reference:
    """Deliberately naive O(n) book used as an oracle."""
    def __init__(self):
        self.bids, self.asks, self.seq, self.trades = [], [], 0, []

    def submit(self, oid, side, price, qty):
        self.seq += 1
        book = self.asks if side == 0 else self.bids
        while qty:
            cands = [o for o in book if (o[0] <= price if side == 0 else o[0] >= price)]
            if not cands:
                break
            best = min(cands, key=lambda o: (o[0] if side == 0 else -o[0], o[1]))
            fill = min(qty, best[3])
            self.trades.append((oid, best[2], best[0], fill))
            qty -= fill
            best[3] -= fill
            if not best[3]:
                book.remove(best)
        if qty:
            (self.bids if side == 0 else self.asks).append([price, self.seq, oid, qty])

    def depth(self):
        def agg(book):
            d = {}
            for o in book:
                d[o[0]] = d.get(o[0], 0) + o[3]
            return d
        return agg(self.bids), agg(self.asks)


class MatcherTests(unittest.TestCase):
    def each(self):
        for cls in BACKENDS:
            with self.subTest(backend=cls.__module__):
                yield cls(max_price=1000, pool_size=4096, trade_cap=4096)

    def test_resting_orders_do_not_cross(self):
        for b in self.each():
            b.submit(1, 0, 50, 100, 0); b.submit(2, 1, 51, 10, 0)
            self.assertEqual(b.drain_trades(), [])
            self.assertEqual((b.best_bid(), b.best_ask()), (50, 51))

    def test_exact_match_and_empty_book(self):
        for b in self.each():
            b.submit(1, 1, 50, 10, 5); b.submit(2, 0, 50, 10, 7)
            t = b.drain_trades()
            self.assertEqual([x[:4] for x in t], [(2, 1, 50, 10)])
            self.assertEqual(t[0][4], 7)
            self.assertIsNone(b.best_bid()); self.assertIsNone(b.best_ask())

    def test_price_time_priority(self):
        for b in self.each():
            b.submit(1, 1, 50, 10, 0); b.submit(2, 1, 50, 10, 0); b.submit(3, 1, 49, 4, 0)
            b.submit(9, 0, 50, 22, 0)
            self.assertEqual([x[:4] for x in b.drain_trades()],
                             [(9, 3, 49, 4), (9, 1, 50, 10), (9, 2, 50, 8)])

    def test_trades_execute_at_maker_price(self):
        for b in self.each():
            b.submit(1, 1, 50, 10, 0); b.submit(2, 0, 55, 10, 0)
            self.assertEqual(b.drain_trades()[0][2], 50)

    def test_partial_fill_rests_remainder(self):
        for b in self.each():
            b.submit(1, 1, 50, 5, 0); b.submit(2, 0, 50, 20, 0)
            self.assertEqual(b.drain_trades()[0][3], 5)
            self.assertEqual(b.depth(3)[0], [(50, 15)])

    def test_sweep_and_whale(self):
        for b in self.each():
            b.whale_levels = 3
            for i, px in enumerate((50, 51, 52)):
                b.submit(i + 1, 1, px, 5, 0)
            b.submit(9, 0, 52, 12, 0)
            self.assertEqual([x[3] for x in b.drain_trades()], [5, 5, 2])
            self.assertEqual(b.depth(3)[1], [(52, 3)])
            self.assertEqual(b.stats()["whale_events"], 1)
            self.assertEqual(b.stats()["whale_last_levels"], 3)

    def test_invalid_orders_rejected(self):
        for b in self.each():
            b.submit(1, 0, 50, 0, 0); b.submit(2, 0, -1, 5, 0); b.submit(3, 0, 5000, 5, 0)
            self.assertEqual(b.stats()["rejected"], 3)

    def test_matches_oracle_on_random_flow(self):
        rng = random.Random(42)
        orders = [(i + 1, rng.getrandbits(1), rng.randint(95, 105), rng.randint(1, 50))
                  for i in range(4000)]
        ref = Reference()
        for oid, side, px, qty in orders:
            ref.submit(oid, side, px, qty)
        for b in self.each():
            for oid, side, px, qty in orders:
                b.submit(oid, side, px, qty, 0)
            got = [t[:4] for t in b.drain_trades()]
            self.assertEqual(got, ref.trades)
            bids, asks = b.depth(100)
            rb, ra = ref.depth()
            self.assertEqual(dict(bids), rb)
            self.assertEqual(dict(asks), ra)

    def test_process_batch_equals_submit(self):
        rng = random.Random(1)
        orders = [(i + 1, rng.getrandbits(1), rng.randint(95, 105), rng.randint(1, 50))
                  for i in range(500)]
        buf = bytearray(len(orders) * 32)
        for i, (oid, side, px, qty) in enumerate(orders):
            ORDER.pack_into(buf, i * 32, oid, side, qty, px, 1)
        for cls in BACKENDS:
            a, b = cls(max_price=1000, trade_cap=4096), cls(max_price=1000, trade_cap=4096)
            for oid, side, px, qty in orders:
                a.submit(oid, side, px, qty, 1)
            b.process_batch(buf, len(orders))
            self.assertEqual([t[:4] for t in a.drain_trades()], [t[:4] for t in b.drain_trades()])


if __name__ == "__main__":
    unittest.main()

"""Market simulator: asyncio firehose of mock limit orders into the IPC bus."""
import asyncio
import random
from time import perf_counter, perf_counter_ns

from .ringbuffer import ORDER


class MarketSimulator:
    """Generates a random-walk order flow, with occasional 'whale' orders that
    sweep many price levels. Prices are integer ticks around `mid`."""

    def __init__(self, ring, rate=100_000, mid=100_000, seed=7, whale_prob=0.0002):
        self.ring, self.rate, self.mid = ring, rate, mid
        self.whale_prob = whale_prob
        self.rng = random.Random(seed)
        self.next_id = 1
        self.sent = 0
        self.dropped = 0

    def make_orders(self, n):
        rng, mid, out = self.rng, self.mid, []
        for _ in range(n):
            side = rng.getrandbits(1)
            if rng.random() < self.whale_prob:
                qty = rng.randint(400, 1500)
                reach = rng.randint(8, 12)
                price = mid + reach if side == 0 else mid - reach
            else:
                qty = rng.randint(1, 100)
                off = rng.randint(-3, 3)            # negative = marketable; keeps the book shallow
                price = mid - off if side == 0 else mid + off
            out.append((self.next_id, side, qty, price, 0))
            self.next_id += 1
        if rng.random() < 0.01:
            self.mid += rng.choice((-1, 1))
        return out

    def push(self, orders):
        pushed = self.ring.push_many(orders, ORDER, stamp=perf_counter_ns())
        self.sent += pushed
        self.dropped += len(orders) - pushed       # feed handlers drop when the consumer lags
        return pushed

    async def run(self, duration, stop=None, tick=0.001):
        start = perf_counter()
        sent_target = 0
        while True:
            elapsed = perf_counter() - start
            if elapsed >= duration or (stop is not None and stop.is_set()):
                break
            n = int(elapsed * self.rate) - sent_target
            if n > 0:
                self.push(self.make_orders(n))
                sent_target += n
            await asyncio.sleep(tick)

import os
import tempfile
import unittest

from chronosmatch.bench import run_ipc
from chronosmatch.ringbuffer import ORDER, RingBuffer


class RingTests(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "t.ring")
        self.ring = RingBuffer.create(self.path, 8, ORDER.size)

    def tearDown(self):
        self.ring.close()

    def test_roundtrip_via_second_mapping(self):
        other = RingBuffer.attach(self.path)
        self.assertEqual(self.ring.push_many([(1, 0, 5, 100, 0), (2, 1, 6, 101, 0)], ORDER, stamp=77), 2)
        view, n = other.peek()
        self.assertEqual(n, 2)
        self.assertEqual(list(ORDER.iter_unpack(view))[1], (2, 1, 6, 101, 77))
        del view
        other.advance(n)
        self.assertEqual(self.ring.backlog(), 0)
        other.close()

    def test_wraparound_and_order(self):
        seen, nxt = [], 1
        for _ in range(20):
            batch = [(nxt + i, 0, 1, 1, 0) for i in range(5)]
            pushed = self.ring.push_many(batch, ORDER, stamp=0)
            nxt += pushed
            while True:
                view, n = self.ring.peek(3)
                if not n:
                    break
                seen += [r[0] for r in ORDER.iter_unpack(view)]
                del view
                self.ring.advance(n)
        self.assertEqual(seen, list(range(1, nxt)))

    def test_full_ring_applies_backpressure(self):
        self.assertEqual(self.ring.push_many([(i, 0, 1, 1, 0) for i in range(12)], ORDER, stamp=0), 8)
        self.assertFalse(self.ring.push_raw(bytes(32), 1))

    def test_cross_process_zero_copy(self):
        r = run_ipc(n=60_000, ring_capacity=1 << 12, pickle_n=5_000)
        self.assertEqual(r["orders"], 60_000)
        self.assertEqual(r["integrity_errors"], 0)


if __name__ == "__main__":
    unittest.main()

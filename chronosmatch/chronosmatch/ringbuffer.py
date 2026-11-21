"""Zero-copy single-producer / single-consumer ring buffer on a shared mmap.

Two processes map the same file (ideally on /dev/shm), so a record written by
the producer is read by the consumer from the *same physical RAM* with no
pickling, sockets or copies.

Layout (all little-endian):
    0   : magic u32 | capacity u32 | record_size u32 | reserved u32
    64  : write_seq u64   (only the producer writes it)
    128 : read_seq  u64   (only the consumer writes it)
    192 : capacity * record_size bytes of records

write_seq/read_seq sit on separate cache lines to avoid false sharing.
Sequence numbers only ever grow; slot = seq & (capacity - 1).

Memory ordering: the producer writes record bytes first and publishes
write_seq last. This is safe on x86-64 (TSO); on weakly ordered CPUs (ARM)
a real implementation needs release/acquire fences, which pure Python can't
express. The Cython engine and this class are intended for x86-64 Linux.
"""
import mmap
import os
import struct

MAGIC = 0x43524E47  # "CRNG"
WRITE_OFF, READ_OFF, DATA_OFF = 64, 128, 192
_HDR = struct.Struct("<IIII")
U64 = struct.Struct("<Q")

# Order record, 32 bytes: order_id, side(0=buy,1=sell), qty, price(ticks), ts_ns
ORDER = struct.Struct("<QB3xIqQ")
# Trade record, 48 bytes: taker_id, maker_id, price, qty, ts_in, ts_out
TRADE = struct.Struct("<QQqI4xQQ")
assert ORDER.size == 32 and TRADE.size == 48


class RingBuffer:
    def __init__(self, fd, mm, path, capacity, rec_size, owner):
        self._fd, self._mm, self.path = fd, mm, path
        self.capacity, self.rec_size, self.mask = capacity, rec_size, capacity - 1
        self._owner = owner
        self._mv = memoryview(mm)

    # -- construction -----------------------------------------------------
    @classmethod
    def create(cls, path, capacity, rec_size):
        if capacity & (capacity - 1):
            raise ValueError("capacity must be a power of two")
        size = DATA_OFF + capacity * rec_size
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        os.ftruncate(fd, size)
        mm = mmap.mmap(fd, size)
        _HDR.pack_into(mm, 0, MAGIC, capacity, rec_size, 0)
        U64.pack_into(mm, WRITE_OFF, 0)
        U64.pack_into(mm, READ_OFF, 0)
        return cls(fd, mm, path, capacity, rec_size, True)

    @classmethod
    def attach(cls, path):
        fd = os.open(path, os.O_RDWR)
        mm = mmap.mmap(fd, os.fstat(fd).st_size)
        magic, capacity, rec_size, _ = _HDR.unpack_from(mm, 0)
        if magic != MAGIC:
            mm.close(); os.close(fd)
            raise ValueError("not a ChronosMatch ring buffer: %s" % path)
        return cls(fd, mm, path, capacity, rec_size, False)

    # -- cursors ----------------------------------------------------------
    @property
    def write_seq(self):
        return U64.unpack_from(self._mm, WRITE_OFF)[0]

    @property
    def read_seq(self):
        return U64.unpack_from(self._mm, READ_OFF)[0]

    def backlog(self):
        return self.write_seq - self.read_seq

    def free(self):
        return self.capacity - self.backlog()

    # -- producer side ----------------------------------------------------
    def push_raw(self, data, n):
        """Append n pre-packed records. All-or-nothing; returns success."""
        w = self.write_seq
        if self.capacity - (w - self.read_seq) < n:
            return False
        rs, cap = self.rec_size, self.capacity
        start = w & self.mask
        first = min(n, cap - start)
        src = memoryview(data)
        off = DATA_OFF + start * rs
        self._mv[off:off + first * rs] = src[:first * rs]
        if first < n:
            self._mv[DATA_OFF:DATA_OFF + (n - first) * rs] = src[first * rs:n * rs]
        U64.pack_into(self._mm, WRITE_OFF, w + n)   # publish last
        return True

    def push_many(self, records, st, stamp=None):
        """Pack tuples with struct `st` and push as many as fit. Returns count.

        If `stamp` is given it replaces the last field of every record
        (used to timestamp orders at the moment of ingestion).
        """
        n = min(len(records), self.free())
        if n <= 0:
            return 0
        rs = st.size
        buf = bytearray(n * rs)
        pack = st.pack_into
        if stamp is None:
            for i in range(n):
                pack(buf, i * rs, *records[i])
        else:
            for i in range(n):
                pack(buf, i * rs, *records[i][:-1], stamp)
        self.push_raw(buf, n)
        return n

    # -- consumer side ----------------------------------------------------
    def peek(self, max_records=None):
        """Zero-copy view of up to max_records contiguous readable records.

        Returns (memoryview, n). Call advance(n) once you are done with the
        view; the producer cannot overwrite those slots until then.
        """
        r = self.read_seq
        avail = self.write_seq - r
        if avail <= 0:
            return self._mv[0:0], 0
        start = r & self.mask
        n = min(avail, self.capacity - start)
        if max_records:
            n = min(n, max_records)
        off = DATA_OFF + start * self.rec_size
        return self._mv[off:off + n * self.rec_size], n

    def advance(self, n):
        U64.pack_into(self._mm, READ_OFF, self.read_seq + n)

    # -- lifecycle --------------------------------------------------------
    def close(self):
        try:
            self._mv.release()
            self._mm.close()
        except BufferError:
            pass            # caller still holds a peek() view; GC will free it
        try:
            os.close(self._fd)
        except OSError:
            pass
        if self._owner:
            try:
                os.unlink(self.path)
            except OSError:
                pass

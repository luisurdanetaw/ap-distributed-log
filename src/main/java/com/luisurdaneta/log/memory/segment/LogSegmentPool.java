package com.luisurdaneta.log.memory.segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;


public final class LogSegmentPool implements AutoCloseable {

    private final int max;
    private final LogSegment[] segments;

    // 0 = FREE, 1 = IN_USE
    private final AtomicIntegerArray state;

    // for "round-robin-ish" scanning to reduce contention on index 0
    private final AtomicInteger cursor = new AtomicInteger(0);

    // optional: track in-use count (like your ConnectionLimiter.active)
    private final AtomicInteger inUse = new AtomicInteger(0);

    /**
     * @param dir directory where segments will live
     * @param max max segments in the pool
     * @param initialCapacity passed into LogSegment.loadOrInit(path, initialCapacity)
     */
    public LogSegmentPool(Path dir, int max, long initialCapacity) throws IOException {
        if (max <= 0) throw new IllegalArgumentException("max must be > 0");
        this.max = max;
        this.segments = new LogSegment[max];
        this.state = new AtomicIntegerArray(max);

        Files.createDirectories(dir);

        // Eagerly create all segments up-front
        for (int i = 0; i < max; i++) {
            Path path = dir.resolve(fileNameFor(i));
            segments[i] = LogSegment.loadOrInit(path, initialCapacity);
            state.set(i, 0); // FREE
        }
    }

    /**
     * Try to acquire a segment.
     * @return a Lease (must be closed) or null if none are available
     */
    public Lease tryAcquire() {
        final int start = floorMod(cursor.getAndIncrement(), max);

        // Scan at most 'max' slots.
        for (int probe = 0; probe < max; probe++) {
            int idx = start + probe;
            if (idx >= max) idx -= max;

            // CAS: FREE -> IN_USE
            if (state.compareAndSet(idx, 0, 1)) {
                inUse.incrementAndGet();
                return new Lease(this, idx, segments[idx]);
            }
        }

        return null; // pool exhausted
    }

    /**
     * Releases a previously acquired segment index back to the pool.
     * No locks, CAS to ensure correctness.
     */
    private void release(int idx) {
        // IN_USE -> FREE
        if (!state.compareAndSet(idx, 1, 0)) {
            throw new IllegalStateException("Double release or corrupt state for idx=" + idx);
        }
        inUse.decrementAndGet();
    }

    public int max() {
        return max;
    }

    public int inUse() {
        return inUse.get();
    }

    public int available() {
        return max - inUse.get();
    }

    @Override
    public void close() throws Exception {
        // This is not "lock-free critical"; it's shutdown.
        Exception first = null;
        for (int i = 0; i < max; i++) {
            try {
                if (segments[i] != null) segments[i].close();
            } catch (Exception e) {
                if (first == null) first = e;
            }
        }
        if (first != null) throw first;
    }

    /**
     * A small RAII-style handle that returns the segment to the pool on close().
     */
    public static final class Lease implements AutoCloseable {
        private final LogSegmentPool pool;
        private final int idx;
        private LogSegment seg;

        private Lease(LogSegmentPool pool, int idx, LogSegment seg) {
            this.pool = pool;
            this.idx = idx;
            this.seg = seg;
        }

        public LogSegment segment() {
            LogSegment s = seg;
            if (s == null) throw new IllegalStateException("Lease already closed");
            return s;
        }

        @Override
        public void close() {
            LogSegment s = seg;
            if (s == null) return; // idempotent close
            seg = null;
            pool.release(idx);
        }
    }

    private static String fileNameFor(int i) {
        // stable naming: segment-00000.qlog, segment-00001.qlog, ...
        return String.format("segment-%05d.qlog", i);
    }

    private static int floorMod(int x, int m) {
        int r = x % m;
        return (r < 0) ? (r + m) : r;
    }
}

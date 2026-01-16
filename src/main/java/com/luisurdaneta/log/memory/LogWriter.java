package com.luisurdaneta.log.memory;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.SegmentConstants.*;

/**
 * LogWriter - append-side writer for a mapped {@link LogSegment}.
 *
 * Design:
 *  - Single-writer thread (no locks, no contention)
 *  - Append-only, 64B aligned entries
 *  - Two-phase publish:
 *      append(...) writes bytes but does NOT advance committed tail
 *      commit() updates superblock committed_tail / entry_count / last_seq (publication barrier)
 *
 * This matches LogReader semantics: readers scan up to committedTailSnapshot().
 */
public final class LogWriter implements AutoCloseable {

    // For power-of-two alignment ops
    private static final int ALIGN_MASK = ENTRY_ALIGNMENT - 1;

    static {
        // Fail fast if someone changes alignment to non-power-of-two.
        if ((ENTRY_ALIGNMENT & (ENTRY_ALIGNMENT - 1)) != 0) {
            throw new ExceptionInInitializerError("ENTRY_ALIGNMENT must be power of two: " + ENTRY_ALIGNMENT);
        }
        if (ENTRY_HEADER_SIZE <= 0 || ENTRY_HEADER_SIZE > 512) {
            throw new ExceptionInInitializerError("ENTRY_HEADER_SIZE looks wrong: " + ENTRY_HEADER_SIZE);
        }
    }

    // Scratch (avoid per-append allocations)
    private static final int SCRATCH_BYTES = 64 * 1024;

    private static final class Scratch {
        final byte[] bytes = new byte[SCRATCH_BYTES];
        final MemorySegment seg = MemorySegment.ofArray(bytes);
    }

    private static final ThreadLocal<Scratch> SCRATCH =
            ThreadLocal.withInitial(Scratch::new);

    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    // ------------------------------------------------------------

    private final LogSegment seg;
    private final MemorySegment ms;
    private final long fileCapacity;

    // Single-writer enforcement
    private final Thread ownerThread;

    // Uncommitted (private writer state)
    private long tail;        // next append position (uncommitted)
    private long entryCount;  // uncommitted count
    private long nextSeq;     // next sequence to assign (lastSeq + 1)

    // Batching counters (optional)
    private long pendingEntries;
    private long pendingBytes;

    // Defaults
    private final int defaultFlags;
    private final int defaultType;
    private final int defaultCodec;

    public LogWriter(LogSegment seg) {
        this(seg, 0, 1, 0); // flags=0, type=1, codec=0
    }

    public LogWriter(LogSegment seg, int defaultFlags, int defaultType, int defaultCodec) {
        if (seg == null) throw new NullPointerException("seg");
        this.seg = seg;
        this.ms = seg.getSegment();
        this.fileCapacity = seg.getFileCapacity();

        this.defaultFlags = defaultFlags;
        this.defaultType = defaultType;
        this.defaultCodec = defaultCodec;

        this.ownerThread = Thread.currentThread();

        // Start from committed superblock snapshot
        this.tail = seg.getCommittedTail();
        this.entryCount = seg.getEntryCount();
        long lastSeq = seg.getLastSeq();
        this.nextSeq = lastSeq + 1;

        // Safety: tail must be aligned and >= DATA_OFFSET
        if (tail < DATA_OFFSET) {
            throw new IllegalStateException("Committed tail < DATA_OFFSET: " + tail);
        }
        if ((tail & ALIGN_MASK) != 0) {
            throw new IllegalStateException("Committed tail not aligned: " + tail);
        }
        if (tail > fileCapacity) {
            throw new IllegalStateException("Committed tail > fileCapacity: " + tail + " > " + fileCapacity);
        }
    }

    // ------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------

    /** Current uncommitted tail (next append offset). */
    public long tail() {
        return tail;
    }

    /** Current uncommitted entry count. */
    public long entryCount() {
        return entryCount;
    }

    /** Next seq that will be assigned on append. */
    public long nextSeq() {
        return nextSeq;
    }

    /**
     * Append payload bytes as a log entry.
     *
     * Returns the assigned sequence number.
     * DOES NOT publish to readers until commit() is called.
     */
    public long append(byte[] payload) {
        return append(payload, 0, payload.length, defaultFlags, defaultType, defaultCodec, nowUnixNanos());
    }

    public long append(byte[] payload, int flags, int type, int codec) {
        return append(payload, 0, payload.length, flags, type, codec, nowUnixNanos());
    }

    public long append(byte[] payload, int off, int len, int flags, int type, int codec, long tsUnixNanos) {
        checkThread();
        if (payload == null) throw new NullPointerException("payload");
        if (off < 0 || len < 0 || off + len > payload.length) {
            throw new IndexOutOfBoundsException("off=" + off + " len=" + len + " payloadLen=" + payload.length);
        }

        // Compute layout
        int padding = paddingFor(len);
        int totalLen = checkedTotalLen(len, padding);

        long entryStart = tail;
        long entryEnd = entryStart + (long) totalLen;

        if (entryEnd > fileCapacity) {
            throw new IllegalStateException(
                    "Segment full: need " + totalLen + " bytes at tail=" + tail + " but capacity=" + fileCapacity);
        }

        long seq = nextSeq++;

        // Write payload first (writer-private until commit)
        long payloadOff = entryStart + ENTRY_HEADER_SIZE;
        MemorySegment.copy(MemorySegment.ofArray(payload), off, ms, payloadOff, len);

        // Zero padding (not strictly required, but keeps file clean / deterministic)
        if (padding != 0) {
            long padStart = payloadOff + len;
            for (int i = 0; i < padding; i++) {
                ms.set(ValueLayout.JAVA_BYTE, padStart + i, (byte) 0);
            }
        }

        // Compute payload CRC over source bytes (no second pass over mapped memory)
        int payloadCrc = crc32c(payload, off, len);

        // Write header with CRC fields as ZERO (required for header CRC to match "zeroed CRC" convention)
        writeHeaderZeroCrc(
                entryStart,
                flags,
                type,
                codec,
                seq,
                tsUnixNanos,
                /*compressedLen*/ len,
                /*rawLen*/ len,
                totalLen,
                padding
        );

        // Compute header CRC over header bytes with CRC fields still zero
        int headerCrc = computeHeaderCrcZeroed(entryStart);

        // Now store CRC fields (reader will zero them when validating header CRC)
        ms.set(INT_LE, entryStart + ENT_PAYLOAD_CRC, payloadCrc);
        ms.set(INT_LE, entryStart + ENT_HEADER_CRC, headerCrc);

        // Advance uncommitted state
        tail = entryEnd;
        entryCount++;
        pendingEntries++;
        pendingBytes += totalLen;

        return seq;
    }

    /**
     * Publish all appended entries since last commit to readers by advancing superblock committed tail.
     *
     * This is your "release" barrier: only after this does LogReader see new entries.
     */
    public void commit() {
        commit(false);
    }

    /**
     * Commit, optionally forcing to disk.
     * force==true is expensive (msync).
     */
    public void commit(boolean force) {
        checkThread();

        if (pendingEntries == 0) return; // nothing to do

        // Publish: checkpoint writes committed_tail/count/seq + superblock CRC, then updates volatiles.
        seg.checkpointSuperblock(tail, entryCount, nextSeq - 1);

        // Optional durability
        if (force) {
            seg.sync();
        }

        pendingEntries = 0;
        pendingBytes = 0;
    }

    /**
     * Convenience: append + commit.
     * Slower but simple for early testing.
     */
    public long appendAndCommit(byte[] payload) {
        long seq = append(payload);
        commit(false);
        return seq;
    }

    /** Commit + force (durable group commit). */
    public void commitAndSync() {
        commit(true);
    }

    @Override
    public void close() {
        // For a writer, closing without publishing is almost always "oops".
        // Commit without force; caller can sync if they want.
        try {
            commit(false);
        } catch (Throwable ignored) {
            // don't throw on close
        }
    }

    // ------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------

    private void checkThread() {
        if (Thread.currentThread() != ownerThread) {
            throw new IllegalStateException("LogWriter used from multiple threads. Owner=" +
                    ownerThread.getName() + " current=" + Thread.currentThread().getName());
        }
    }

    private static long nowUnixNanos() {
        return System.currentTimeMillis() * 1_000_000L;
    }

    private static int paddingFor(int payloadLen) {
        int base = ENTRY_HEADER_SIZE + payloadLen;
        int mod = base & ALIGN_MASK;
        return (mod == 0) ? 0 : (ENTRY_ALIGNMENT - mod);
    }

    private static int checkedTotalLen(int payloadLen, int padding) {
        // totalLen = header + payload + padding (all int)
        long total = (long) ENTRY_HEADER_SIZE + (long) payloadLen + (long) padding;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Entry too large: totalLen overflows int: " + total);
        }
        return (int) total;
    }

    private void writeHeaderZeroCrc(long entryStart,
                                    int flags,
                                    int type,
                                    int codec,
                                    long seq,
                                    long tsUnixNanos,
                                    int compressedLen,
                                    int rawLen,
                                    int totalLen,
                                    int paddingLen) {

        ms.set(INT_LE, entryStart + ENT_MAGIC, MAGIC_ENTR);
        ms.set(SHORT_LE, entryStart + ENT_HEADER_BYTES, (short) ENTRY_HEADER_SIZE);
        ms.set(SHORT_LE, entryStart + ENT_VERSION, (short) VERSION);

        ms.set(INT_LE, entryStart + ENT_FLAGS, flags);
        ms.set(SHORT_LE, entryStart + ENT_TYPE, (short) type);
        ms.set(SHORT_LE, entryStart + ENT_CODEC, (short) codec);

        ms.set(LONG_LE, entryStart + ENT_SEQ, seq);
        ms.set(LONG_LE, entryStart + ENT_TS_UNIX_NANOS, tsUnixNanos);

        ms.set(INT_LE, entryStart + ENT_COMPRESSED_LEN, compressedLen);
        ms.set(INT_LE, entryStart + ENT_UNCOMPRESSED_LEN, rawLen);

        ms.set(INT_LE, entryStart + ENT_TOTAL_LEN, totalLen);
        ms.set(SHORT_LE, entryStart + ENT_PADDING_LEN, (short) paddingLen);

        // CRC fields must be ZERO while computing header CRC
        ms.set(INT_LE, entryStart + ENT_PAYLOAD_CRC, 0);
        ms.set(INT_LE, entryStart + ENT_HEADER_CRC, 0);
    }

    private static int crc32c(byte[] data, int off, int len) {
        CRC32C crc = CRC.get();
        crc.reset();
        crc.update(data, off, len);
        return (int) crc.getValue();
    }

    /**
     * Compute header CRC32C over 64 bytes with BOTH CRC fields treated as zero.
     * Writer guarantees CRC fields are currently zero, so this is just CRC(headerBytes).
     *
     * Uses scratch copy: no allocation, no writes to mapped memory besides initial header write.
     */
    private int computeHeaderCrcZeroed(long entryStart) {
        CRC32C crc = CRC.get();
        crc.reset();

        Scratch scratch = SCRATCH.get();

        // Copy header bytes into scratch and CRC them
        MemorySegment.copy(ms, entryStart, scratch.seg, 0, (long) ENTRY_HEADER_SIZE);
        crc.update(scratch.bytes, 0, ENTRY_HEADER_SIZE);

        return (int) crc.getValue();
    }
}

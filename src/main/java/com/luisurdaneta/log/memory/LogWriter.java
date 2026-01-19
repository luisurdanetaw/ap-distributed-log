package com.luisurdaneta.log.memory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.SegmentConstants.*;

/**
 * =============================================================================
 * LogWriter — QLOG Segment Format v2 (Simplified Append-Only)
 * =============================================================================
 *
 * Writer model
 * ------------
 * • Single-writer per segment instance (enforced by owner thread check)
 * • Appends entries into the mmap region starting at current committed_tail
 * • Entries are NOT visible to readers until commit() advances superblock committed_tail
 * • "All-or-nothing": data beyond committed_tail is ignored on recovery
 *
 * Entry layout (v2)
 * -----------------
 *   entry_start (16B aligned):
 *     [32B header]
 *     [payload_len bytes payload]
 *     [0..15B padding to 16B]
 *
 * Header fields (32B):
 *   0x00 u32 magic = "ENTR"
 *   0x04 u16 header_bytes = 32
 *   0x06 u16 flags
 *   0x08 u32 type
 *   0x0C u32 codec
 *   0x10 u64 ts_unix_nanos    (LWW timestamp)
 *   0x18 u32 payload_len      (bytes on disk)
 *   0x1C u32 payload_crc32c   (optional; 0 if unused)
 *
 * Notes
 * -----
 * • No seq, no header CRC, no total_len stored (derived).
 * • CRC32C is computed from source payload bytes (no second pass over mapped memory).
 * • Padding is derived and optionally zeroed (keeps file deterministic).
 */

public final class LogWriter implements AutoCloseable {

    // Power-of-two alignment ops
    private static final int ALIGN_MASK = ENTRY_ALIGNMENT - 1;

    static {
        if ((ENTRY_ALIGNMENT & (ENTRY_ALIGNMENT - 1)) != 0) {
            throw new ExceptionInInitializerError("ENTRY_ALIGNMENT must be power of two: " + ENTRY_ALIGNMENT);
        }
        if (ENTRY_HEADER_SIZE <= 0 || ENTRY_HEADER_SIZE > 256) {
            throw new ExceptionInInitializerError("ENTRY_HEADER_SIZE looks wrong: " + ENTRY_HEADER_SIZE);
        }
        if (ENTRY_HEADER_SIZE != 32) {
            // Keep this strict until you intentionally rev the format again.
            throw new ExceptionInInitializerError("ENTRY_HEADER_SIZE must be 32 for v2 layout: " + ENTRY_HEADER_SIZE);
        }
    }

    // Reusable CRC32C instance (ThreadLocal)
    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    // -------------------------------------------------------------------------

    private final LogSegment seg;
    private final MemorySegment ms;
    private final long fileCapacity;

    // Single-writer enforcement
    private final Thread ownerThread;

    // Uncommitted writer state
    private long tail; // next append position (uncommitted)
    private long pendingEntries;
    private long pendingBytes;

    // Defaults
    private final int defaultFlags; // ENT_FLAGS (u16, but stored in int)
    private final int defaultType;  // ENT_TYPE (u32)
    private final int defaultCodec; // ENT_CODEC (u32)
    private final boolean defaultCrcEnabled;

    public LogWriter(LogSegment seg) {
        this(seg, /*flags*/0, /*type*/1, /*codec*/0, /*crcEnabled*/true);
    }

    public LogWriter(LogSegment seg, int defaultFlags, int defaultType, int defaultCodec, boolean crcEnabled) {
        if (seg == null) throw new NullPointerException("seg");
        this.seg = seg;
        this.ms = seg.getSegment();
        this.fileCapacity = seg.getFileCapacity();

        this.defaultFlags = defaultFlags & 0xFFFF;
        this.defaultType = defaultType;
        this.defaultCodec = defaultCodec;
        this.defaultCrcEnabled = crcEnabled;

        this.ownerThread = Thread.currentThread();

        // Start from committed snapshot
        this.tail = seg.getCommittedTail();

        // Safety checks
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

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Current uncommitted tail (next append offset). */
    public long tail() { return tail; }

    /** Pending entries since last commit. */
    public long pendingEntries() { return pendingEntries; }

    /** Pending bytes since last commit. */
    public long pendingBytes() { return pendingBytes; }

    /** Append with defaults; timestamp set to now (unix nanos). Returns entry start offset. */
    public long append(byte[] payload) {
        return append(payload, 0, payload.length, defaultFlags, defaultType, defaultCodec, nowUnixNanos(), defaultCrcEnabled);
    }

    /** Append with explicit header fields; timestamp set to now (unix nanos). Returns entry start offset. */
    public long append(byte[] payload, int flags, int type, int codec) {
        return append(payload, 0, payload.length, flags, type, codec, nowUnixNanos(), defaultCrcEnabled);
    }

    /**
     * Append payload as an entry. Returns the entry start offset (useful as a stable handle).
     *
     * The entry is not visible to readers until commit() is called.
     */
    public long append(byte[] payload,
                       int off,
                       int len,
                       int flags,
                       int type,
                       int codec,
                       long tsUnixNanos,
                       boolean crcEnabled) {
        checkThread();
        if (payload == null) throw new NullPointerException("payload");
        if (off < 0 || len < 0 || off + len > payload.length) {
            throw new IndexOutOfBoundsException("off=" + off + " len=" + len + " payloadLen=" + payload.length);
        }
        if (len == 0) {
            throw new IllegalArgumentException("Zero-length payload not allowed (simplifies readers & avoids ambiguity).");
        }
        if (tsUnixNanos <= 0) {
            throw new IllegalArgumentException("tsUnixNanos must be > 0 for LWW: " + tsUnixNanos);
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

        // ----------------------------
        // Write payload (writer-private)
        // ----------------------------
        long payloadOff = entryStart + ENTRY_HEADER_SIZE;
        MemorySegment.copy(MemorySegment.ofArray(payload), off, ms, payloadOff, len);

        // Optional: zero padding for determinism / cleaner scans
        if (padding != 0) {
            zeroRange(payloadOff + len, padding);
        }

        // ----------------------------
        // Compute CRC32C over SOURCE bytes (no second pass over mmap)
        // ----------------------------
        int payloadCrc = 0;
        int hdrFlags = (flags & 0xFFFF);

        if (crcEnabled) {
            payloadCrc = crc32c(payload, off, len);
            hdrFlags |= ENT_FLAG_HAS_CRC;
        } else {
            hdrFlags &= ~ENT_FLAG_HAS_CRC;
        }

        // Caller can also set ENT_FLAG_COMPRESSED if codec != 0
        // (we don't enforce; reader will interpret based on flags/codec)
        // If you want strictness:
        // if (codec != 0) hdrFlags |= ENT_FLAG_COMPRESSED;

        // ----------------------------
        // Write header
        // ----------------------------
        writeHeader(
                entryStart,
                hdrFlags,
                type,
                codec,
                tsUnixNanos,
                len,
                payloadCrc
        );

        // Advance uncommitted state
        tail = entryEnd;
        pendingEntries++;
        pendingBytes += totalLen;

        return entryStart;
    }

    /** Publish appended entries by advancing superblock committed_tail. */
    public void commit() { commit(false); }

    /**
     * Commit, optionally forcing to disk (msync).
     * force==true is expensive; use for group commit / durability points.
     */
    public void commit(boolean force) {
        checkThread();
        if (pendingEntries == 0) return;

        seg.checkpointCommittedTail(tail);

        if (force) seg.sync();

        pendingEntries = 0;
        pendingBytes = 0;
    }

    /** Convenience: append + commit (no force). */
    public long appendAndCommit(byte[] payload) {
        long off = append(payload);
        commit(false);
        return off;
    }

    /** Commit + force. */
    public void commitAndSync() {
        commit(true);
    }

    @Override
    public void close() {
        try {
            commit(false);
        } catch (Throwable ignored) {
            // don't throw on close
        }
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

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
        long total = (long) ENTRY_HEADER_SIZE + (long) payloadLen + (long) padding;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Entry too large: totalLen overflows int: " + total);
        }
        return (int) total;
    }

    private void writeHeader(long entryStart,
                             int flagsU16,
                             int typeU32,
                             int codecU32,
                             long tsUnixNanos,
                             int payloadLenU32,
                             int payloadCrc32c) {

        ms.set(INT_LE,   entryStart + ENT_MAGIC, MAGIC_ENTR);
        ms.set(SHORT_LE, entryStart + ENT_HEADER_BYTES, (short) ENTRY_HEADER_SIZE);
        ms.set(SHORT_LE, entryStart + ENT_FLAGS, (short) (flagsU16 & 0xFFFF));

        ms.set(INT_LE, entryStart + ENT_TYPE, typeU32);
        ms.set(INT_LE, entryStart + ENT_CODEC, codecU32);

        ms.set(LONG_LE, entryStart + ENT_TS_UNIX_NANOS, tsUnixNanos);

        ms.set(INT_LE, entryStart + ENT_PAYLOAD_LEN, payloadLenU32);

        // If CRC disabled, this will be 0 and ENT_FLAG_HAS_CRC will be unset.
        ms.set(INT_LE, entryStart + ENT_PAYLOAD_CRC32, payloadCrc32c);
    }

    private void zeroRange(long start, int len) {
        // Byte-wise zero is fine; small (0..15).
        // Keep it simple; if you later align bigger, you can optimize.
        for (int i = 0; i < len; i++) {
            ms.set(ValueLayout.JAVA_BYTE, start + i, (byte) 0);
        }
    }

    private static int crc32c(byte[] data, int off, int len) {
        CRC32C crc = CRC.get();
        crc.reset();
        crc.update(data, off, len);
        return (int) crc.getValue();
    }
}

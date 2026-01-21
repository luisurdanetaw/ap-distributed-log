package com.luisurdaneta.log.memory.io;

import com.luisurdaneta.log.memory.segment.LogSegment;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.segment.SegmentConstants.*;


import java.nio.ByteBuffer;

/**
 * =============================================================================
 * LogWriter
 * =============================================================================

 * Purpose:
 *   Efficiently stream large payloads (e.g. 1GB) into a mmap-backed log segment
 *   using a reusable 4KB ByteBuffer, with incremental CRC32C.

 * Correctness:
 *   - Payload bytes are written first (writer-private region)
 *   - Header is written last (makes entry parseable)
 *   - Entry becomes visible ONLY after commit() advances committed_tail
 */

public final class LogWriter implements AutoCloseable {

    private static final int ALIGN_MASK = ENTRY_ALIGNMENT - 1;

    static {
        if ((ENTRY_ALIGNMENT & (ENTRY_ALIGNMENT - 1)) != 0) {
            throw new ExceptionInInitializerError("ENTRY_ALIGNMENT must be power of two: " + ENTRY_ALIGNMENT);
        }
        if (ENTRY_HEADER_SIZE != 32) {
            throw new ExceptionInInitializerError("ENTRY_HEADER_SIZE must be 32 for v2 layout: " + ENTRY_HEADER_SIZE);
        }
    }

    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    // backing segment
    private final LogSegment seg;
    private final MemorySegment ms;
    private final long fileCapacity;

    // single-writer enforcement
    private final Thread ownerThread;

    // uncommitted writer state
    private long tail;
    private long pendingEntries;
    private long pendingBytes;

    // defaults
    private final int defaultFlags;
    private final int defaultType;
    private final int defaultCodec;
    private final boolean defaultCrcEnabled;

    // -------------------------------------------------------------------------
    // streaming state (exactly one in-flight streaming entry)
    // -------------------------------------------------------------------------

    private boolean streaming;

    private long entryStart;
    private long payloadOff;
    private long writePos;
    private long remaining;

    private int payloadLenU32;
    private int padding;
    private int totalLen;

    private int flagsU16;
    private int typeU32;
    private int codecU32;
    private long tsUnixNanos;

    private boolean crcEnabled;
    private CRC32C crc;

    // -------------------------------------------------------------------------

    public LogWriter(LogSegment seg) {
        this(seg, /*flags*/0, /*type*/1, /*codec*/0, /*crcEnabled*/true);
    }

    public LogWriter(LogSegment seg, int defaultFlags, int defaultType, int defaultCodec, boolean defaultCrcEnabled) {
        if (seg == null) throw new NullPointerException("seg");

        this.seg = seg;
        this.ms = seg.getSegment();
        this.fileCapacity = seg.getFileCapacity();

        this.defaultFlags = defaultFlags & 0xFFFF;
        this.defaultType = defaultType;
        this.defaultCodec = defaultCodec;
        this.defaultCrcEnabled = defaultCrcEnabled;

        this.ownerThread = Thread.currentThread();

        this.tail = seg.getCommittedTail();

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
    // streaming API
    // -------------------------------------------------------------------------

    /** Start streaming with defaults (flags/type/codec/crc enabled). */
    public long beginStreaming(long payloadLenBytes) {
        return beginStreaming(payloadLenBytes, defaultFlags, defaultType, defaultCodec, nowUnixNanos(), defaultCrcEnabled);
    }

    /** Begin streaming an entry with known payload length. */
    public long beginStreaming(long payloadLenBytes,
                               int flags,
                               int type,
                               int codec,
                               long tsUnixNanos,
                               boolean crcEnabled) {

        checkThread();

        if (streaming) throw new IllegalStateException("Streaming entry already in progress");
        if (payloadLenBytes <= 0) throw new IllegalArgumentException("payloadLenBytes must be > 0");
        if (payloadLenBytes > Integer.MAX_VALUE) {
            // 1GB is fine; >2GB would require widening a couple internal ints.
            throw new IllegalArgumentException("payloadLenBytes too large for current writer: " + payloadLenBytes);
        }
        if (tsUnixNanos <= 0) throw new IllegalArgumentException("tsUnixNanos must be > 0");

        int payloadLen = (int) payloadLenBytes;

        int padding = paddingFor(payloadLen);
        int totalLen = totalLen(payloadLen, padding);

        long entryStart = tail;
        long entryEnd = entryStart + (long) totalLen;

        if (entryEnd > fileCapacity) {
            throw new IllegalStateException(
                    "Segment full: need " + totalLen + " bytes at tail=" + tail + " but capacity=" + fileCapacity);
        }

        // setup in-flight streaming state
        this.streaming = true;

        this.entryStart = entryStart;
        this.payloadOff = entryStart + ENTRY_HEADER_SIZE;
        this.writePos = this.payloadOff;
        this.remaining = payloadLenBytes;

        this.payloadLenU32 = payloadLen;
        this.padding = padding;
        this.totalLen = totalLen;

        this.flagsU16 = (flags & 0xFFFF);
        this.typeU32 = type;
        this.codecU32 = codec;
        this.tsUnixNanos = tsUnixNanos;

        this.crcEnabled = crcEnabled;
        if (crcEnabled) {
            CRC32C c = CRC.get();
            c.reset();
            this.crc = c;
        } else {
            this.crc = null;
        }

        // NOTE: tail is NOT advanced until finishStreaming().
        // If the socket dies mid-stream, you abortStreaming() and overwrite this region later.
        return entryStart;
    }

    /**
     * Stream one chunk into the mmap entry and update CRC32C incrementally.
     *
     * Zero-alloc path:
     *   - pass the SAME MemorySegment view of your scratch ByteBuffer each time.
     *
     * Requirements:
     *   - beginStreaming() already called
     *   - chunkBuf is in read-mode (position..limit are valid bytes)
     *   - chunkLen <= chunkBuf.remaining()
     */
    public void writeChunk(ByteBuffer chunkBuf, MemorySegment chunkSeg, int chunkLen) {
        checkThread();

        if (!streaming) throw new IllegalStateException("writeChunk() without beginStreaming()");
        if (chunkLen <= 0) return;

        if (chunkLen > chunkBuf.remaining()) {
            throw new IllegalArgumentException("chunkLen > chunkBuf.remaining(): " + chunkLen + " > " + chunkBuf.remaining());
        }
        if ((long) chunkLen > remaining) {
            throw new IllegalArgumentException("chunkLen > remaining: " + chunkLen + " > " + remaining);
        }

        // CRC32C over the exact bytes (no extra copy)
        if (crcEnabled) {
            // CRC32C.update(ByteBuffer) advances position => use mark/reset to keep caller buffer stable
            chunkBuf.mark();
            int oldLimit = chunkBuf.limit();
            chunkBuf.limit(chunkBuf.position() + chunkLen);
            crc.update(chunkBuf);
            chunkBuf.limit(oldLimit);
            chunkBuf.reset();
        }

        // Copy bytes from scratch buffer memory into mmap at writePos
        long srcOff = chunkBuf.position();
        MemorySegment.copy(chunkSeg, srcOff, ms, writePos, chunkLen);

        writePos += chunkLen;
        remaining -= chunkLen;

        // advance caller buffer position to match consumed bytes (optional but convenient)
        chunkBuf.position(chunkBuf.position() + chunkLen);
    }

    /**
     * Finish the entry:
     *  - require remaining==0
     *  - zero padding
     *  - write entry header
     *  - advance tail + pending stats
     *
     * Entry is still NOT visible until commit().
     */
    public long finishStreaming() {
        checkThread();

        if (!streaming) throw new IllegalStateException("finishStreaming() without beginStreaming()");
        if (remaining != 0) {
            throw new IllegalStateException("finishStreaming() called with remaining=" + remaining);
        }

        // zero padding bytes for determinism
        if (padding != 0) {
            zeroRange(payloadOff + payloadLenU32, padding);
        }

        int payloadCrc32c = 0;
        int hdrFlags = flagsU16;

        if (crcEnabled) {
            payloadCrc32c = (int) crc.getValue();
            hdrFlags |= ENT_FLAG_HAS_CRC;
        } else {
            hdrFlags &= ~ENT_FLAG_HAS_CRC;
        }

        writeHeader(entryStart, hdrFlags, typeU32, codecU32, tsUnixNanos, payloadLenU32, payloadCrc32c);

        long entryEnd = entryStart + (long) totalLen;

        tail = entryEnd;
        pendingEntries++;
        pendingBytes += totalLen;

        // clear streaming state
        streaming = false;
        crc = null;

        return entryStart;
    }

    /**
     * Abort the current streaming entry (connection dropped / protocol error).
     * Tail is NOT advanced. Since committed_tail was never updated, readers will never see partial bytes.
     */
    public void abortStreaming() {
        checkThread();
        streaming = false;
        crc = null;
    }

    /** Publish appended entries by advancing superblock committed_tail. */
    public void commit() { commit(false); }

    public void commit(boolean force) {
        checkThread();
        if (pendingEntries == 0) return;

        seg.checkpointCommittedTail(tail);
        if (force) seg.sync();

        pendingEntries = 0;
        pendingBytes = 0;
    }

    // -------------------------------------------------------------------------
    // misc getters
    // -------------------------------------------------------------------------

    public long tail() { return tail; }
    public long pendingEntries() { return pendingEntries; }
    public long pendingBytes() { return pendingBytes; }
    public boolean isStreaming() { return streaming; }

    @Override
    public void close() {
        try {
            // If someone forgets to finish, abort (safe: uncommitted region ignored).
            if (streaming) abortStreaming();
            commit(false);
        } catch (Throwable ignored) {
        }
    }

    // -------------------------------------------------------------------------
    // internals
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

    private static int totalLen(int payloadLen, int padding) {
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
        ms.set(INT_LE, entryStart + ENT_PAYLOAD_CRC32, payloadCrc32c);
    }

    private void zeroRange(long start, int len) {
        for (int i = 0; i < len; i++) {
            ms.set(ValueLayout.JAVA_BYTE, start + i, (byte) 0);
        }
    }
}
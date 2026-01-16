package com.luisurdaneta.log.memory;

import java.lang.foreign.MemorySegment;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.SegmentConstants.*;

public final class LogReader {

    // Thread-local scratch (byte[] + MemorySegment view) to avoid per-entry allocation
    private static final int SCRATCH_BYTES = 64 * 1024; // payload CRC chunk

    private static final class Scratch {
        final byte[] bytes = new byte[SCRATCH_BYTES];
        final MemorySegment seg = MemorySegment.ofArray(bytes);
    }

    private static final ThreadLocal<Scratch> SCRATCH =
            ThreadLocal.withInitial(Scratch::new);

    // Thread-local CRC32C to avoid contention + allocation
    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    /** Callback invoked per validated entry. Return false to stop scanning. */
    @FunctionalInterface
    public interface EntryHandler {
        boolean onEntry(long entryStart,
                        int totalLen,
                        int flags,
                        int type,
                        int codec,
                        long seq,
                        long tsNanos,
                        long payloadOff,
                        int payloadLen,
                        int rawLen);
    }

    private final LogSegment seg;
    private final MemorySegment ms;
    private final long fileCapacity;

    public LogReader(LogSegment seg) {
        this.seg = seg;
        this.ms = seg.getSegment();
        this.fileCapacity = seg.getFileCapacity();
    }

    /** Snapshot the committed tail once; use this for consistent scans. */
    public long committedTailSnapshot() {
        return seg.getCommittedTail();
    }

    /**
     * Scan entries from {@code startOffset} up to a committed tail snapshot.
     *
     * @return the next offset after the last valid processed entry
     */
    public long scan(long startOffset, long tailSnapshot, long maxEntries, EntryHandler handler) {
        if (handler == null) throw new NullPointerException("handler");

        long off = normalizeStart(startOffset);
        long tail = clampTail(tailSnapshot);

        long delivered = 0;
        while (off < tail && delivered < maxEntries) {
            int r = validateAndDecode(off, tail, handler);
            if (r == 0) break; // invalid/partial/corrupt

            int len = Math.abs(r);
            off += len;
            delivered++;

            if (r < 0) break; // handler requested stop (but we already advanced)
        }

        return off;
    }

    /** Convenience: scan from DATA_OFFSET to current committed tail snapshot. */
    public long scanAll(EntryHandler handler) {
        long tail = committedTailSnapshot();
        return scan(DATA_OFFSET, tail, Long.MAX_VALUE, handler);
    }

    /** Returns offset just past last valid entry (useful for repair). */
    public long lastValidOffset(long tailSnapshot) {
        long tail = clampTail(tailSnapshot);
        long off = DATA_OFFSET;

        while (off < tail) {
            int totalLen = validateHeaderAndCrc(off, tail);
            if (totalLen == 0) break;
            off += totalLen;
        }
        return off;
    }

    // ------------------------
    // Internals
    // ------------------------

    private long normalizeStart(long startOffset) {
        if (startOffset <= DATA_OFFSET) return DATA_OFFSET;

        long aligned = startOffset & -ENTRY_ALIGNMENT;
        if (aligned < DATA_OFFSET) aligned = DATA_OFFSET;
        return aligned;
    }

    private long clampTail(long tailSnapshot) {
        long tail = tailSnapshot;
        if (tail < DATA_OFFSET) tail = DATA_OFFSET;
        if (tail > fileCapacity) tail = fileCapacity;
        return tail;
    }

    /**
     * Returns:
     *  - 0            => invalid/partial/corrupt
     *  - +totalLen    => valid, continue scanning
     *  - -totalLen    => valid, but handler requested stop
     */
    private int validateAndDecode(long entryStart, long tail, EntryHandler handler) {
        // Need at least header
        if (entryStart < DATA_OFFSET || entryStart > tail - ENTRY_HEADER_SIZE) return 0;
        if (entryStart > fileCapacity - ENTRY_HEADER_SIZE) return 0;

        int magic = ms.get(INT_LE, entryStart + ENT_MAGIC);
        if (magic != MAGIC_ENTR) return 0;

        int headerBytes = ms.get(SHORT_LE, entryStart + ENT_HEADER_BYTES) & 0xFFFF;
        if (headerBytes != ENTRY_HEADER_SIZE) return 0;

        int version = ms.get(SHORT_LE, entryStart + ENT_VERSION) & 0xFFFF;
        if (version != VERSION) return 0;

        int flags = ms.get(INT_LE, entryStart + ENT_FLAGS);
        int type  = ms.get(SHORT_LE, entryStart + ENT_TYPE) & 0xFFFF;
        int codec = ms.get(SHORT_LE, entryStart + ENT_CODEC) & 0xFFFF;

        long seq     = ms.get(LONG_LE, entryStart + ENT_SEQ);
        long tsNanos = ms.get(LONG_LE, entryStart + ENT_TS_UNIX_NANOS);

        int compressedLen = ms.get(INT_LE, entryStart + ENT_COMPRESSED_LEN);
        int rawLen        = ms.get(INT_LE, entryStart + ENT_UNCOMPRESSED_LEN);

        int totalLen   = ms.get(INT_LE, entryStart + ENT_TOTAL_LEN);
        int paddingLen = ms.get(SHORT_LE, entryStart + ENT_PADDING_LEN) & 0xFFFF;

        // Sanity
        if (compressedLen < 0) return 0;
        if (rawLen < 0) return 0;
        if (paddingLen >= ENTRY_ALIGNMENT) return 0;
        if (totalLen < ENTRY_HEADER_SIZE) return 0;

        int expected = ENTRY_HEADER_SIZE + compressedLen + paddingLen;
        if (totalLen != expected) return 0;

        long entryEnd = entryStart + (long) totalLen;
        if (entryEnd > tail) return 0;         // partial relative to committed tail snapshot
        if (entryEnd > fileCapacity) return 0; // out of bounds

        if (!validateCrcs(entryStart, compressedLen)) return 0;

        long payloadOff = entryStart + ENTRY_HEADER_SIZE;

        boolean cont = handler.onEntry(
                entryStart,
                totalLen,
                flags,
                type,
                codec,
                seq,
                tsNanos,
                payloadOff,
                compressedLen,
                rawLen
        );

        return cont ? totalLen : -totalLen;
    }

    /** Validation-only (no handler). Returns totalLen or 0. */
    private int validateHeaderAndCrc(long entryStart, long tail) {
        if (entryStart < DATA_OFFSET || entryStart > tail - ENTRY_HEADER_SIZE) return 0;
        if (entryStart > fileCapacity - ENTRY_HEADER_SIZE) return 0;

        int magic = ms.get(INT_LE, entryStart + ENT_MAGIC);
        if (magic != MAGIC_ENTR) return 0;

        int headerBytes = ms.get(SHORT_LE, entryStart + ENT_HEADER_BYTES) & 0xFFFF;
        if (headerBytes != ENTRY_HEADER_SIZE) return 0;

        int version = ms.get(SHORT_LE, entryStart + ENT_VERSION) & 0xFFFF;
        if (version != VERSION) return 0;

        int compressedLen = ms.get(INT_LE, entryStart + ENT_COMPRESSED_LEN);
        int totalLen      = ms.get(INT_LE, entryStart + ENT_TOTAL_LEN);
        int paddingLen    = ms.get(SHORT_LE, entryStart + ENT_PADDING_LEN) & 0xFFFF;

        if (compressedLen < 0 || totalLen < ENTRY_HEADER_SIZE || paddingLen >= ENTRY_ALIGNMENT) return 0;

        int expected = ENTRY_HEADER_SIZE + compressedLen + paddingLen;
        if (totalLen != expected) return 0;

        long entryEnd = entryStart + (long) totalLen;
        if (entryEnd > tail || entryEnd > fileCapacity) return 0;

        if (!validateCrcs(entryStart, compressedLen)) return 0;

        return totalLen;
    }

    private boolean validateCrcs(long entryStart, int compressedLen) {
        int storedPayloadCrc = ms.get(INT_LE, entryStart + ENT_PAYLOAD_CRC);
        int storedHeaderCrc  = ms.get(INT_LE, entryStart + ENT_HEADER_CRC);

        int computedHeaderCrc = computeHeaderCrcZeroed(entryStart);
        if (computedHeaderCrc != storedHeaderCrc) return false;

        long payloadOff = entryStart + ENTRY_HEADER_SIZE;
        int computedPayloadCrc = computeCrcRange(payloadOff, compressedLen);
        return computedPayloadCrc == storedPayloadCrc;
    }

    /**
     * Header CRC32C over 64 bytes with BOTH CRC fields treated as zero.
     * Never writes to mapped memory.
     */
    private int computeHeaderCrcZeroed(long entryStart) {
        CRC32C crc = CRC.get();
        crc.reset();

        Scratch scratch = SCRATCH.get();
        byte[] buf = scratch.bytes;
        MemorySegment bufSeg = scratch.seg;

        // Segment -> segment copy (portable across FFM builds)
        MemorySegment.copy(ms, entryStart, bufSeg, 0, (long) ENTRY_HEADER_SIZE);

        // Zero both crc fields in scratch
        int p = (int) ENT_PAYLOAD_CRC;
        int h = (int) ENT_HEADER_CRC;

        buf[p] = buf[p + 1] = buf[p + 2] = buf[p + 3] = 0;
        buf[h] = buf[h + 1] = buf[h + 2] = buf[h + 3] = 0;

        crc.update(buf, 0, ENTRY_HEADER_SIZE);
        return (int) crc.getValue();
    }

    /** CRC32C over arbitrary mapped range, chunked via scratch buffer. */
    private int computeCrcRange(long offset, int length) {
        CRC32C crc = CRC.get();
        crc.reset();

        if (length <= 0) return 0;

        Scratch scratch = SCRATCH.get();
        byte[] buf = scratch.bytes;
        MemorySegment bufSeg = scratch.seg;

        long remaining = length;
        long pos = 0;

        while (remaining != 0) {
            int n = (int) Math.min((long) buf.length, remaining);

            // Segment -> segment copy (portable)
            MemorySegment.copy(ms, offset + pos, bufSeg, 0, (long) n);

            crc.update(buf, 0, n);
            pos += n;
            remaining -= n;
        }

        return (int) crc.getValue();
    }
}
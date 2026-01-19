package com.luisurdaneta.log.memory;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.SegmentConstants.*;

/**
 * =============================================================================
 * LogReader — QLOG Segment v2 Reader (Simplified Layout)
 * =============================================================================
 *
 * This reader scans committed entries in a memory-mapped LogSegment (v2).
 *
 * Key properties:
 *  • Single source of truth for visibility: superblock COMMITTED_TAIL
 *  • Entries are fixed-header + payload + derived padding (alignment)
 *  • No streaming write support: if entry bytes are within committed_tail,
 *    they are considered fully written (writer only advances tail after writing).
 *
 * Entry header (32 bytes):
 *   0x00 u32 magic         = "ENTR"
 *   0x04 u16 header_bytes  = 32
 *   0x06 u16 flags
 *   0x08 u32 type
 *   0x0C u32 codec
 *   0x10 u64 ts_unix_nanos
 *   0x18 u32 payload_len
 *   0x1C u32 payload_crc32 (optional; only valid if flags bit ENT_FLAG_HAS_CRC)
 *
 * Entry total length is derived:
 *   base = ENTRY_HEADER_SIZE + payload_len
 *   pad  = (ENTRY_ALIGNMENT - (base % ENTRY_ALIGNMENT)) % ENTRY_ALIGNMENT
 *   total_len = base + pad
 *
 * This class provides:
 *  • scan(start, tailSnapshot, maxEntries, handler)
 *  • scanAll(handler)
 *  • lastValidOffset(tailSnapshot) for repair/truncation discovery
 */
public final class LogReader {

    // Chunk size used only if you decide to compute CRC by copying mapped bytes.
    // For now we compute CRC via MemorySegment slices -> ByteBuffer directly,
    // which is simpler and fast enough for large sequential reads.
    private static final ThreadLocal<CRC32C> CRC =
            ThreadLocal.withInitial(CRC32C::new);

    /**
     * Callback invoked per validated entry. Return false to stop scanning.
     *
     * Note:
     *  • entryStart is the file offset of the entry header.
     *  • totalLen includes header + payload + alignment padding (derived).
     *  • payloadOff = entryStart + ENTRY_HEADER_SIZE
     */
    @FunctionalInterface
    public interface EntryHandler {
        boolean onEntry(long entryStart,
                        int totalLen,
                        int flags,
                        int type,
                        int codec,
                        long tsNanos,
                        long payloadOff,
                        int payloadLen,
                        int payloadCrc32);
    }

    private final LogSegment seg;
    private final MemorySegment ms;
    private final long fileCapacity;

    public LogReader(LogSegment seg) {
        if (seg == null) throw new NullPointerException("seg");
        this.seg = seg;
        this.ms = seg.getSegment();
        this.fileCapacity = seg.getFileCapacity();
    }

    /** Snapshot committed tail once; use for consistent scans. */
    public long committedTailSnapshot() {
        return seg.getCommittedTail();
    }

    /**
     * Scan entries from {@code startOffset} up to {@code tailSnapshot}.
     *
     * @return the next offset after the last valid processed entry
     */
    public long scan(long startOffset, long tailSnapshot, long maxEntries, EntryHandler handler) {
        if (handler == null) throw new NullPointerException("handler");
        if (maxEntries < 0) throw new IllegalArgumentException("maxEntries < 0: " + maxEntries);

        long off = normalizeStart(startOffset);
        long tail = clampTail(tailSnapshot);

        long delivered = 0;
        while (off < tail && delivered < maxEntries) {
            int r = validateAndDecode(off, tail, handler);
            if (r == 0) break; // invalid/partial/corrupt

            int len = Math.abs(r);
            off += len;
            delivered++;

            if (r < 0) break; // handler requested stop
        }

        return off;
    }

    /** Convenience: scan from DATA_OFFSET to current committed tail snapshot. */
    public long scanAll(EntryHandler handler) {
        long tail = committedTailSnapshot();
        return scan(DATA_OFFSET, tail, Long.MAX_VALUE, handler);
    }

    /** Returns offset just past last valid entry (useful for repair/trim). */
    public long lastValidOffset(long tailSnapshot) {
        long tail = clampTail(tailSnapshot);
        long off = DATA_OFFSET;

        while (off < tail) {
            int totalLen = validateHeaderAndOptionalCrc(off, tail);
            if (totalLen == 0) break;
            off += totalLen;
        }
        return off;
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private long normalizeStart(long startOffset) {
        if (startOffset <= DATA_OFFSET) return DATA_OFFSET;

        // align down to ENTRY_ALIGNMENT
        long aligned = startOffset & -((long) ENTRY_ALIGNMENT);
        if (aligned < DATA_OFFSET) aligned = DATA_OFFSET;
        return aligned;
    }

    private long clampTail(long tailSnapshot) {
        long tail = tailSnapshot;
        if (tail < DATA_OFFSET) tail = DATA_OFFSET;
        if (tail > fileCapacity) tail = fileCapacity;

        // tail should be aligned, but clamp anyway (don't throw in reader)
        tail &= -((long) ENTRY_ALIGNMENT);
        if (tail < DATA_OFFSET) tail = DATA_OFFSET;
        return tail;
    }

    /**
     * Returns:
     *  - 0            => invalid/partial/corrupt
     *  - +totalLen    => valid, continue scanning
     *  - -totalLen    => valid, but handler requested stop
     */
    private int validateAndDecode(long entryStart, long tail, EntryHandler handler) {
        // Need at least header available within committed bytes
        if (entryStart < DATA_OFFSET) return 0;
        if (entryStart > tail - ENTRY_HEADER_SIZE) return 0;
        if (entryStart > fileCapacity - ENTRY_HEADER_SIZE) return 0;

        // ---- Header fields ----
        int magic = ms.get(INT_LE, entryStart + ENT_MAGIC);
        if (magic != MAGIC_ENTR) return 0;

        int headerBytes = ms.get(SHORT_LE, entryStart + ENT_HEADER_BYTES) & 0xFFFF;
        if (headerBytes != ENTRY_HEADER_SIZE) return 0;

        int flags = ms.get(SHORT_LE, entryStart + ENT_FLAGS) & 0xFFFF;
        int type  = ms.get(INT_LE, entryStart + ENT_TYPE);
        int codec = ms.get(INT_LE, entryStart + ENT_CODEC);

        long tsNanos = ms.get(LONG_LE, entryStart + ENT_TS_UNIX_NANOS);

        int payloadLen = ms.get(INT_LE, entryStart + ENT_PAYLOAD_LEN);
        if (payloadLen < 0) return 0;

        int storedPayloadCrc32 = ms.get(INT_LE, entryStart + ENT_PAYLOAD_CRC32);

        // ---- Derive total length ----
        int totalLen = computeTotalLen(payloadLen);
        long entryEnd = entryStart + (long) totalLen;

        if (entryEnd > tail) return 0;         // partial relative to committed tail snapshot
        if (entryEnd > fileCapacity) return 0; // out of bounds

        long payloadOff = entryStart + (long) ENTRY_HEADER_SIZE;

        // ---- Optional payload CRC validation ----
        if ((flags & ENT_FLAG_HAS_CRC) != 0) {
            int computed = computePayloadCrc32(payloadOff, payloadLen);
            if (computed != storedPayloadCrc32) return 0;
        }

        boolean cont = handler.onEntry(
                entryStart,
                totalLen,
                flags,
                type,
                codec,
                tsNanos,
                payloadOff,
                payloadLen,
                storedPayloadCrc32
        );

        return cont ? totalLen : -totalLen;
    }

    /** Validation-only. Returns derived totalLen if valid, otherwise 0. */
    private int validateHeaderAndOptionalCrc(long entryStart, long tail) {
        if (entryStart < DATA_OFFSET) return 0;
        if (entryStart > tail - ENTRY_HEADER_SIZE) return 0;
        if (entryStart > fileCapacity - ENTRY_HEADER_SIZE) return 0;

        int magic = ms.get(INT_LE, entryStart + ENT_MAGIC);
        if (magic != MAGIC_ENTR) return 0;

        int headerBytes = ms.get(SHORT_LE, entryStart + ENT_HEADER_BYTES) & 0xFFFF;
        if (headerBytes != ENTRY_HEADER_SIZE) return 0;

        int flags = ms.get(SHORT_LE, entryStart + ENT_FLAGS) & 0xFFFF;

        int payloadLen = ms.get(INT_LE, entryStart + ENT_PAYLOAD_LEN);
        if (payloadLen < 0) return 0;

        int totalLen = computeTotalLen(payloadLen);
        long entryEnd = entryStart + (long) totalLen;

        if (entryEnd > tail) return 0;
        if (entryEnd > fileCapacity) return 0;

        if ((flags & ENT_FLAG_HAS_CRC) != 0) {
            long payloadOff = entryStart + (long) ENTRY_HEADER_SIZE;
            int storedPayloadCrc32 = ms.get(INT_LE, entryStart + ENT_PAYLOAD_CRC32);
            int computed = computePayloadCrc32(payloadOff, payloadLen);
            if (computed != storedPayloadCrc32) return 0;
        }

        return totalLen;
    }

    private int computeTotalLen(int payloadLen) {
        int base = ENTRY_HEADER_SIZE + payloadLen;
        int pad = paddingFor(base, ENTRY_ALIGNMENT);
        return base + pad;
    }

    private static int paddingFor(int len, int alignment) {
        int mod = len & (alignment - 1);
        return (mod == 0) ? 0 : (alignment - mod);
    }

    /**
     * CRC32C over the payload bytes.
     *
     * Implementation note:
     *  • Uses a MemorySegment slice -> ByteBuffer and updates CRC in one shot.
     *  • For very large payloads this is still fine (CRC32C.update(ByteBuffer) is native-ish).
     */
    private int computePayloadCrc32(long payloadOff, int payloadLen) {
        if (payloadLen <= 0) return 0;

        CRC32C crc = CRC.get();
        crc.reset();

        ByteBuffer buf = ms.asSlice(payloadOff, payloadLen).asByteBuffer();
        crc.update(buf);

        return (int) crc.getValue();
    }
}
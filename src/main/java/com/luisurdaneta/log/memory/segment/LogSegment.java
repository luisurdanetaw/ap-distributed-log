package com.luisurdaneta.log.memory.segment;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.segment.SegmentConstants.*;

/**
 * ================================================================================
 *  LogSegment — QLOG Segment Format v2 (Simplified Append-Only, mmap-owned)
 * ================================================================================

 *  This class owns ONE memory-mapped segment file and provides the *minimum*
 *  machinery to:
 *    - create or open the file
 *    - validate/initialize the superblock
 *    - expose the MemorySegment for fast append-style writers
 *    - publish durability/visibility via a single committed tail pointer

 *  The design is intentionally "dumb fast":
 *    - No indexing
 *    - No per-entry global ordering guarantees
 *    - Visibility is controlled ONLY by committed_tail

 * ================================================================================
 *  FILE FORMAT SUMMARY (v2)
 * ================================================================================

 *  Endianness
 *  ----------
 *  • All multi-byte fields are LITTLE-ENDIAN.

 *  Layout
 *  ------
 *  • File offset 0..4095 is reserved for the superblock page (SUPERBLOCK_SIZE = 4096).
 *  • Only the first SUPERBLOCK_USED bytes (64B) are defined.
 *  • Data region begins at DATA_OFFSET (4096) and stores a sequence of entries.
 *  • Entries are aligned to ENTRY_ALIGNMENT (16 bytes).

 * ================================================================================
 *  DURABILITY / VISIBILITY MODEL (Single Authoritative Pointer)
 * ================================================================================

 *  committed_tail is the ONLY authoritative indicator of what bytes are valid.

 *    - Bytes in [DATA_OFFSET, committed_tail) are durable/visible.
 *    - Bytes >= committed_tail are uncommitted garbage (ignore on recovery).

 *  This enables ALL-OR-NOTHING visibility:
 *    - Writers may stream bytes into the uncommitted region (in chunks).
 *    - The entry becomes visible ONLY after committed_tail advances past it.
 *    - If a writer crashes mid-stream, committed_tail is unchanged and recovery
 *      ignores the partial bytes completely.

 * ================================================================================
 *  SUPERBLOCK (first 64 bytes of page 0)
 * ================================================================================

 *  Offset  Size  Field
 *  ------  ----  ------------------------------------------------
 *  0x00    u32   MAGIC              = "QLOG"
 *  0x04    u16   VERSION            = 2
 *  0x06    u16   HEADER_BYTES       = 64
 *  0x08    u64   FILE_UUID_HI
 *  0x10    u64   FILE_UUID_LO
 *  0x18    u64   CREATED_UNIX_NANOS
 *  0x20    u64   COMMITTED_TAIL     (absolute file offset)
 *  0x28    u64   FILE_CAPACITY      (bytes)
 *  0x30    u32   FLAGS              (reserved for future use)
 *  0x34    u32   SUPERBLOCK_CRC32   (optional; 0 = disabled)

 *  Notes
 *  -----
 *  • CRC32C covers bytes [0 .. SB_CRC32) (excludes the CRC field itself).
 *  • If SUPERBLOCK_CRC32 is 0, CRC validation is treated as disabled.
 *  • The remainder of the 4KB page is reserved (future growth, stats, etc.).

 * ================================================================================
 *  ENTRY FORMAT (append-only)
 * ================================================================================

 *  Each entry is written into the uncommitted region, then committed_tail advances.

 *  Entry layout at an aligned entry_start:
 *    [32B fixed header]
 *    [payload_len bytes payload]
 *    [0..15B padding to 16B alignment]

 *  Padding is derived:
 *    pad = (ENTRY_ALIGNMENT - ((ENTRY_HEADER_SIZE + payload_len) % ENTRY_ALIGNMENT)) % ENTRY_ALIGNMENT

 *  There is NO stored total_len field in v2 — readers compute total_len from payload_len.

 * ================================================================================
 *  STREAMING WRITE SUPPORT (IMPORTANT)
 * ================================================================================

 *  Supports streaming writes of large payloads:

 *    1) Reserve space conceptually at tail (caller-owned bookkeeping)
 *    2) Write the 32B entry header at entry_start
 *    3) Stream the payload bytes into the mapped region in chunks
 *         - e.g. 4KB / 64KB chunks
 *         - CRC32C may be computed incrementally while streaming
 *    4) Write padding (if needed)
 *    5) Publish by advancing committed_tail to end_of_entry_aligned

 *  Key property:
 *    - Streaming is allowed
 *    - Partial visibility is NOT allowed
 *    - Only committed_tail makes data real

 * ================================================================================
 *  ENTRY HEADER (32 bytes)
 * ================================================================================

 *  Offset  Size  Field
 *  ------  ----  ------------------------------------------------
 *  0x00    u32   MAGIC = "ENTR"
 *  0x04    u16   HEADER_BYTES = 32
 *  0x06    u16   FLAGS
 *                 bit 0 → payload compressed
 *                 bit 1 → payload_crc32 valid
 *                 bit 2 → tombstone (delete marker)
 *  0x08    u32   TYPE             (application-defined)
 *  0x0C    u32   CODEC            (0 = none, 1 = zstd, ...)
 *  0x10    u64   TS_UNIX_NANOS     (used for LWW conflict resolution)
 *  0x18    u32   PAYLOAD_LEN      (bytes on disk)
 *  0x1C    u32   PAYLOAD_CRC32    (optional; 0 if unused)

 * ================================================================================
 *  INVARIANTS & EXPECTATIONS
 * ================================================================================

 *  • This class does NOT implement the append/write logic itself.
 *    It only provides:
 *      - the mapped MemorySegment
 *      - a cached committedTail
 *      - checkpointCommittedTail() to publish new durable bytes

 *  • Concurrency:
 *      - Intended for a single-writer model per LogSegment instance.
 *      - Writers must ensure they do not overlap in the uncommitted region.

 *  • Recovery:
 *      - Readers should scan from DATA_OFFSET up to committedTail.
 *      - Any trailing bytes beyond committedTail are ignored.

 *  • Resizing:
 *      - Not supported in this implementation (capacity is fixed once mapped).
 *      - validateSuperblock() enforces superblock capacity == mapped capacity.
 */

public final class LogSegment implements AutoCloseable {

    // Immutable instance state
    private final Path filePath;
    private final FileChannel channel;
    private final Arena arena;
    private final MemorySegment segment;
    private final long fileCapacity;
    private final boolean isNewFile;

    // Cached superblock field(s) (read-mostly; updated on checkpoint)
    private volatile long committedTail;

    // Reusable CRC32C instances (ThreadLocal for low overhead)
    private static final ThreadLocal<CRC32C> crcThreadLocal =
            ThreadLocal.withInitial(CRC32C::new);

    private LogSegment(Path path,
                       FileChannel chan,
                       Arena arena,
                       MemorySegment seg,
                       long capacity,
                       boolean isNew) {
        this.filePath = path;
        this.channel = chan;
        this.arena = arena;
        this.segment = seg;
        this.fileCapacity = capacity;
        this.isNewFile = isNew;

        refreshCachedSuperblock();
    }

    public static LogSegment loadOrInit(Path path, long initialCapacity) throws IOException {
        FileChannel chan;
        long capacity;
        boolean isNew;

        // Avoid TOCTOU race condition by relying on atomic CREATE_NEW open instead of checking existence first
        try {
            if (initialCapacity < MIN_CAPACITY) {
                throw new IllegalArgumentException(INITIAL_CAP_TOO_SMALL + initialCapacity + " < " + MIN_CAPACITY);
            }
            if (initialCapacity > Long.MAX_VALUE - PAGE_MASK) {
                throw new IllegalArgumentException(OVERFLOW_RISK + initialCapacity);
            }

            long aligned = (initialCapacity + PAGE_MASK) & ~PAGE_MASK;

            chan = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );

            isNew = true;
            capacity = aligned;

        } catch (FileAlreadyExistsException e) {
            chan = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

            long size = chan.size();
            if (size < MIN_CAPACITY) {
                throw new IOException(FILE_TOO_SMALL + size + " < " + MIN_CAPACITY);
            }

            isNew = false;
            capacity = size;
        }

        // Ensure file is at least 'capacity' (new file starts at size 0)
        if (chan.size() < capacity) {
            chan.truncate(capacity);
        }

        final Arena arena = Arena.ofShared();
        final MemorySegment seg = chan.map(FileChannel.MapMode.READ_WRITE, 0, capacity, arena);

        final LogSegment logSeg = new LogSegment(path, chan, arena, seg, capacity, isNew);

        if (isNew) {
            logSeg.initializeSuperblock();
        } else {
            logSeg.validateSuperblock();
        }

        return logSeg;
    }

    private void initializeSuperblock() {
        final long nowNanos = System.currentTimeMillis() * 1_000_000L;
        final UUID uuid = UUID.randomUUID();

        segment.set(INT_LE,   SB_MAGIC, MAGIC_QLOG);
        segment.set(SHORT_LE, SB_VERSION, (short) VERSION);
        segment.set(SHORT_LE, SB_HEADER_BYTES, (short) SUPERBLOCK_USED);

        segment.set(LONG_LE, SB_FILE_UUID_HI, uuid.getMostSignificantBits());
        segment.set(LONG_LE, SB_FILE_UUID_LO, uuid.getLeastSignificantBits());
        segment.set(LONG_LE, SB_CREATED_UNIX_NANOS, nowNanos);

        // No entries yet
        segment.set(LONG_LE, SB_COMMITTED_TAIL, (long) DATA_OFFSET);
        segment.set(LONG_LE, SB_FILE_CAPACITY, fileCapacity);

        segment.set(INT_LE, SB_FLAGS, 0);

        int crc = computeSuperblockCRC32();
        segment.set(INT_LE, SB_CRC32, crc);

        // Ensure superblock visibility/durability
        segment.force();

        refreshCachedSuperblock();
    }

    private void validateSuperblock() throws IOException {
        int magic = segment.get(INT_LE, SB_MAGIC);
        if (magic != MAGIC_QLOG) {
            throw new IOException("Invalid QLOG magic: 0x" + Integer.toHexString(magic));
        }

        short version = segment.get(SHORT_LE, SB_VERSION);
        if (version != (short) VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        short headerBytes = segment.get(SHORT_LE, SB_HEADER_BYTES);
        if (headerBytes != (short) SUPERBLOCK_USED) {
            throw new IOException("Invalid superblock header_bytes: " + headerBytes);
        }

        long cap = segment.get(LONG_LE, SB_FILE_CAPACITY);
        if (cap != fileCapacity) {
            throw new IOException("File capacity mismatch: superblock=" + cap + " mapped=" + fileCapacity);
        }

        long tail = segment.get(LONG_LE, SB_COMMITTED_TAIL);
        if (tail < DATA_OFFSET || tail > fileCapacity) {
            throw new IOException("Invalid committed_tail: " + tail + " (data_offset=" + DATA_OFFSET + ", capacity=" + fileCapacity + ")");
        }
        if ((tail % ENTRY_ALIGNMENT) != 0) {
            throw new IOException("Committed tail not aligned: tail=" + tail + " alignment=" + ENTRY_ALIGNMENT);
        }

        int storedCrc = segment.get(INT_LE, SB_CRC32);
        if (storedCrc != 0) {
            int computed = computeSuperblockCRC32();
            if (storedCrc != computed) {
                throw new IOException(String.format(
                        "Superblock CRC mismatch: stored=0x%08X computed=0x%08X",
                        storedCrc, computed));
            }
        }

        refreshCachedSuperblock();
    }

    private int computeSuperblockCRC32() {
        CRC32C crc = crcThreadLocal.get();
        crc.reset();

        MemorySegment slice = segment.asSlice(0, SB_CRC32);
        ByteBuffer buf = slice.asByteBuffer();
        crc.update(buf);

        return (int) crc.getValue();
    }

    private void refreshCachedSuperblock() {
        this.committedTail = segment.get(LONG_LE, SB_COMMITTED_TAIL);
    }

    // ===== Public API =====

    public Path getFilePath() { return filePath; }
    public long getFileCapacity() { return fileCapacity; }
    public boolean isNewFile() { return isNewFile; }

    public long getCommittedTail() { return committedTail; }

    public MemorySegment getSegment() { return segment; }

    /**
     * Publish a new committed tail (the ONLY mutable superblock field).
     *
     * Requirements:
     * • newTail must be <= fileCapacity
     * • newTail must be >= DATA_OFFSET
     * • newTail must be aligned to ENTRY_ALIGNMENT
     *
     * If superblock CRC is enabled (SB_CRC32 != 0 at init), it is recomputed here.
     */
    public void checkpointCommittedTail(long newTail) {
        if (newTail < DATA_OFFSET || newTail > fileCapacity) {
            throw new IllegalArgumentException("newTail out of range: " + newTail);
        }
        if ((newTail % ENTRY_ALIGNMENT) != 0) {
            throw new IllegalArgumentException("newTail not aligned: " + newTail + " (alignment=" + ENTRY_ALIGNMENT + ")");
        }

        // Write tail first
        segment.set(LONG_LE, SB_COMMITTED_TAIL, newTail);

        // If CRC enabled, recompute and write; if disabled (stored 0), keep it 0
        int storedCrc = segment.get(INT_LE, SB_CRC32);
        if (storedCrc != 0) {
            int crc = computeSuperblockCRC32();
            segment.set(INT_LE, SB_CRC32, crc);
        }

        // Publish cached value after writing mapped memory
        this.committedTail = newTail;
    }

    /**
     * Force mapped pages to disk.
     */
    public void sync() {
        segment.force();
    }

    @Override
    public void close() throws IOException {
        segment.force();
        arena.close();
        channel.close();
    }
}
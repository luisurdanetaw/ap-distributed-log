package com.luisurdaneta.log.memory;

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

import static com.luisurdaneta.log.memory.SegmentConstants.*;

/**
 * =============================================================================
 * LogSegment — QLOG Segment Format v2 (Simplified Append-Only)
 * =============================================================================
 *
 * This class owns a single memory-mapped segment file.
 *
 * FORMAT SUMMARY (v2)
 * ------------------
 * • Page 0 (0..4095) is reserved for the "superblock region".
 * • Only the first 64 bytes of page 0 are defined; the rest is reserved.
 * • Data region starts at DATA_OFFSET (4096) and contains a sequence of entries.
 *
 * SUPERBLOCK (first 64 bytes)
 * ---------------------------
 *   0x00 u32 magic              = "QLOG"
 *   0x04 u16 version            = 2
 *   0x06 u16 header_bytes       = 64
 *   0x08 u64 file_uuid_hi
 *   0x10 u64 file_uuid_lo
 *   0x18 u64 created_unix_nanos
 *   0x20 u64 committed_tail     (absolute file offset)
 *   0x28 u64 file_capacity
 *   0x30 u32 flags
 *   0x34 u32 superblock_crc32   (optional; may be 0)
 *
 * COMMIT MODEL (All-or-Nothing)
 * -----------------------------
 * committed_tail is the ONLY authoritative indicator of what bytes are valid.
 *
 * Writer appends:
 *   1) Writes entry header + payload + padding at the current tail (uncommitted region)
 *   2) (optional) fsync/force depending on durability policy
 *   3) Advances committed_tail in superblock to publish those bytes as durable/visible
 *
 * Recovery:
 *   • Trust bytes in [DATA_OFFSET, committed_tail)
 *   • Ignore everything at or beyond committed_tail
 *
 * DESIGN CHOICES vs v1
 * --------------------
 * • Removed per-entry seq, entry_count, generation, header CRC, total_len/padding_len fields.
 *   These are either redundant, derivable, or unnecessary for an unordered append-only log.
 * • Timestamp is kept per entry for LWW use-cases.
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

    /**
     * Load existing segment or initialize a new one (v2 layout).
     *
     * @param path            file path
     * @param initialCapacity requested file size for new files (bytes)
     */
    public static LogSegment loadOrInit(Path path, long initialCapacity) throws IOException {
        FileChannel chan;
        long capacity;
        boolean isNew;

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

    /**
     * Initialize the v2 superblock.
     *
     * NOTE: The reserved bytes are already zero from file creation; we only write defined fields.
     */
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

        // Optional CRC32C: if you want to disable CRC entirely, leave SB_CRC32 = 0
        int crc = computeSuperblockCRC32();
        segment.set(INT_LE, SB_CRC32, crc);

        // Ensure visibility/durability
        segment.force();

        refreshCachedSuperblock();
    }

    /**
     * Validate v2 superblock on load.
     * If stored CRC32 is 0, CRC validation is treated as disabled (backward/ops-friendly).
     */
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
            // If you want to allow remapping/resizing later, relax this check.
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

    /**
     * CRC32C over superblock bytes [0..SB_CRC32) i.e., excludes the CRC field itself.
     * This is stable and avoids needing to "zero-out" a field in-place.
     */
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

    /**
     * Direct access to mapped bytes. Readers/writers must obey layout invariants.
     */
    public MemorySegment getSegment() { return segment; }

    /**
     * Publish a new committed tail (the ONLY mutable superblock field in v2).
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
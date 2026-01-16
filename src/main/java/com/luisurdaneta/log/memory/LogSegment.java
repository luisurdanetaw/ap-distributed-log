package com.luisurdaneta.log.memory;

import static com.luisurdaneta.log.memory.SegmentConstants.*;
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

/**
 * @author Luis Urdaneta
 * @since 1-13-2025
 *
 * Memory-mapped, append-only memory segment backing the log.
 *
 * <h2>Overview</h2>
 * {@code LogSegment} owns a single on-disk persistence file and exposes a zero-copy view of that file
 * via the Foreign Function & Memory (FFM) API ({@link java.lang.foreign.MemorySegment}). The file is
 * structured as a fixed-size 4 KiB superblock followed by an append-only data region containing
 * 64-byte aligned entries. The segment is designed for high-throughput, low-latency write paths with
 * deterministic resource management and minimal GC interaction.
 *
 * <h2>File layout</h2>
 * <ul>
 *   <li><b>Superblock</b> (offset {@code 0}, size {@code 4096} bytes)
 *     <ul>
 *       <li>Identifies the file: magic {@code "QLOG"} and format {@code version}.</li>
 *       <li>Stores checkpoint metadata: {@code committed_tail}, {@code entry_count}, {@code last_seq}.</li>
 *       <li>Stores invariants: {@code data_offset=4096} and {@code file_capacity}.</li>
 *       <li>Includes {@code superblock_crc32c} (CRC32C over bytes {@code [0x0000 .. 0x0FF7]}).</li>
 *     </ul>
 *   </li>
 *   <li><b>Data region</b> (offset {@code 4096} onward)
 *     <ul>
 *       <li>Append-only sequence of entries.</li>
 *       <li>Each entry starts at a 64-byte boundary.</li>
 *       <li>Entry format: 64-byte header + payload + 0..63 bytes padding.</li>
 *       <li>Integrity: per-entry {@code header_crc32c} and {@code payload_crc32c} (CRC32C).</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h2>Initialization and open semantics</h2>
 * {@link #loadOrInit(java.nio.file.Path, long, long)} provides an atomic open/create flow:
 * <ul>
 *   <li>Attempts {@code CREATE_NEW} to create a brand-new file, avoiding TOCTOU races.</li>
 *   <li>If the file already exists, opens it read-write and validates the superblock.</li>
 *   <li>Ensures the file is at least {@code fileCapacity} bytes before mapping (via {@code truncate}).</li>
 * </ul>
 *
 * <h2>Durability and checkpointing</h2>
 * The superblock is the durable checkpoint for readers and recovery.
 * On updates, the writer must follow the checkpoint rule:
 * <ol>
 *   <li>Write checkpoint fields ({@code committed_tail}, {@code entry_count}, {@code last_seq}, etc.).</li>
 *   <li>Compute {@code superblock_crc32c} over the superblock region excluding the CRC itself.</li>
 *   <li>Write {@code superblock_crc32c} last.</li>
 *   <li>Optionally call {@link #sync()} depending on durability requirements.</li>
 * </ol>
 * This class maintains cached volatile copies of the checkpoint fields for fast reads.
 * After initialization and after any checkpoint update, the cache is refreshed to remain consistent
 * with the mapped superblock.
 *
 * <h2>Concurrency model</h2>
 * <ul>
 *   <li>This class is designed for a single-writer model on the data region.</li>
 *   <li>Checkpoint fields are exposed via {@code volatile} reads for safe publication to readers.</li>
 *   <li>No locks are used; callers must coordinate write ownership externally.</li>
 * </ul>
 *
 * <h2>Performance notes</h2>
 * <ul>
 *   <li>All on-disk numeric fields are encoded little-endian using {@link java.lang.foreign.ValueLayout}.</li>
 *   <li>CRC32C is computed using a per-thread {@link java.util.zip.CRC32C} via {@link ThreadLocal}
 *       to avoid contention and hot-path allocations.</li>
 *   <li>Mapping lifetime is deterministic: {@link #close()} closes the {@link Arena} (unmapping the region)
 *       and closes the underlying {@link FileChannel}.</li>
 * </ul>
 *
 * <h2>Safety</h2>
 * This class intentionally exposes the underlying {@link MemorySegment} for zero-copy readers/writers.
 * Callers must respect alignment and format rules defined by {@link SegmentConstants} and the QLOG layout.
 * Invalid writes can corrupt the segment; on open, corruption is detected via magic/version checks and CRC32C.
 *
 */

public final class LogSegment implements AutoCloseable {

    // Instance state - immutable after construction
    private final Path filePath;
    private final FileChannel channel;
    private final Arena arena;
    private final MemorySegment segment;
    private final long fileCapacity;
    private final long nodeId;
    private final boolean isNewFile;

    // Cached superblock values (read from mapped memory, no locks needed)
    private volatile long committedTail;
    private volatile long entryCount;
    private volatile long lastSeq;

    // Reusable CRC32C instance (ThreadLocal for  zero-GC in multithreaded scenarios)
    private static final ThreadLocal<CRC32C> crcThreadLocal =
            ThreadLocal.withInitial(CRC32C::new);


    //Private constructor - use loadOrInit()
    private LogSegment(Path path, FileChannel chan, Arena arena, MemorySegment seg,
                       long capacity, long nodeId, boolean isNew) {
        this.filePath = path;
        this.channel = chan;
        this.arena = arena;
        this.segment = seg;
        this.fileCapacity = capacity;
        this.nodeId = nodeId;
        this.isNewFile = isNew;

        refreshCachedSuperblock();
    }

    /**
     * Load existing segment or initialize new one.
     *
     * @param path File path for segment
     * @param initialCapacity Initial file size in bytes (for new files)
     * @param nodeId Node identifier (0 if unused)
     * @return LogSegment instance
     */
    public static LogSegment loadOrInit(Path path, long initialCapacity, long nodeId)
            throws IOException {

        FileChannel chan;
        long capacity;
        boolean isNew;

        // Try create-new first: atomic "does not exist" check - this avoids time of check to time of use race bs
        try {
            if (initialCapacity < MIN_CAPACITY) {
                throw new IllegalArgumentException(
                        "initialCapacity too small: " + initialCapacity + " < " + MIN_CAPACITY);
            }
            if (initialCapacity > Long.MAX_VALUE - PAGE_MASK) {
                throw new IllegalArgumentException(
                        "initialCapacity overflow risk: " + initialCapacity);
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
                throw new IOException("File too small to be valid segment: " + size);
            }

            isNew = false;
            capacity = size;
        }

        // Ensure file is big enough for mapping (new file starts at size 0)
        // For existing file: size==capacity already; truncate is a no-op.
        if (chan.size() < capacity) {
            chan.truncate(capacity);
        }

        final Arena arena = Arena.ofShared();
        final MemorySegment seg = chan.map(FileChannel.MapMode.READ_WRITE, 0, capacity, arena);

        final LogSegment logSeg = new LogSegment(path, chan, arena, seg, capacity, nodeId, isNew);

        if (isNew) {
            logSeg.initializeSuperblock();
        } else {
            logSeg.validateSuperblock();
        }

        return logSeg;
    }

    /**
     * Initialize new superblock with defaults.
     */
    private void initializeSuperblock() {
        long nowNanos = System.currentTimeMillis() * 1_000_000L;
        UUID uuid = UUID.randomUUID();

        segment.set(INT_LE, OFF_MAGIC, MAGIC_QLOG);
        segment.set(INT_LE, OFF_VERSION, VERSION);
        segment.set(INT_LE, OFF_FLAGS, 0);
        segment.set(INT_LE, OFF_HEADER_BYTES, SUPERBLOCK_SIZE);

        segment.set(LONG_LE, OFF_GENERATION, 0L);
        segment.set(LONG_LE, OFF_FILE_UUID, uuid.getMostSignificantBits());
        segment.set(LONG_LE, OFF_FILE_UUID + 8, uuid.getLeastSignificantBits());
        segment.set(LONG_LE, OFF_NODE_ID, nodeId);
        segment.set(LONG_LE, OFF_CREATED_UNIX_NANOS, nowNanos);

        segment.set(LONG_LE, OFF_DATA_OFFSET, DATA_OFFSET);
        segment.set(LONG_LE, OFF_FILE_CAPACITY, fileCapacity);
        segment.set(LONG_LE, OFF_COMMITTED_TAIL, DATA_OFFSET);  // No entries yet
        segment.set(LONG_LE, OFF_ENTRY_COUNT, 0L);
        segment.set(LONG_LE, OFF_LAST_SEQ, 0L);

        segment.set(INT_LE, OFF_DEFAULT_CODEC, 0);  // no compression for now

        // Zero reserved fields (already zero from file creation, but explicit)
        for (int i = 0x0064; i < OFF_SUPERBLOCK_CRC; i += 8) {
            segment.set(LONG_LE, i, 0L);
        }

        // Compute and write CRC
        int crc = computeSuperblockCRC();
        segment.set(INT_LE, OFF_SUPERBLOCK_CRC, crc);
        segment.set(INT_LE, 0x0FFC, 0);  // pad

        segment.force();

        refreshCachedSuperblock();// Ensure superblock is durable
    }

    /**
     * Validate existing superblock on load.
     */
    private void validateSuperblock() throws IOException {
        int magic = segment.get(INT_LE, OFF_MAGIC);
        if (magic != MAGIC_QLOG) {
            throw new IOException("Invalid QLOG magic: 0x" + Integer.toHexString(magic));
        }

        int version = segment.get(INT_LE, OFF_VERSION);
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        int headerBytes = segment.get(INT_LE, OFF_HEADER_BYTES);
        if (headerBytes != SUPERBLOCK_SIZE) {
            throw new IOException("Invalid header_bytes: " + headerBytes);
        }

        // Validate CRC
        int storedCrc = segment.get(INT_LE, OFF_SUPERBLOCK_CRC);
        int computedCrc = computeSuperblockCRC();

        if (storedCrc != computedCrc) {
            throw new IOException(String.format(
                    "Superblock CRC mismatch: stored=0x%08X computed=0x%08X",
                    storedCrc, computedCrc));
        }
    }

    /**
     * Compute CRC32C over superblock [0x0000..0x0FF7].
     * Zero-allocation using ThreadLocal CRC instance.
     */
    private int computeSuperblockCRC() {
        CRC32C crc = crcThreadLocal.get();
        crc.reset();

        // Create slice for CRC calculation
        MemorySegment slice = segment.asSlice(0, OFF_SUPERBLOCK_CRC);

        // Convert to ByteBuffer for CRC32C.update()
        ByteBuffer buf = slice.asByteBuffer();
        crc.update(buf);

        return (int) crc.getValue();
    }

    /**
     * Compute CRC32C over arbitrary byte range.
     */
    private int computeCRC32C(long offset, long length) {
        CRC32C crc = crcThreadLocal.get();
        crc.reset();

        MemorySegment slice = segment.asSlice(offset, length);
        ByteBuffer buf = slice.asByteBuffer();
        crc.update(buf);

        return (int) crc.getValue();
    }

    private void refreshCachedSuperblock() {
        this.committedTail = segment.get(LONG_LE, OFF_COMMITTED_TAIL);
        this.entryCount = segment.get(LONG_LE, OFF_ENTRY_COUNT);
        this.lastSeq = segment.get(LONG_LE, OFF_LAST_SEQ);
    }

    // ===== Public API =====

    public Path getFilePath() { return filePath; }
    public long getFileCapacity() { return fileCapacity; }
    public long getNodeId() { return nodeId; }
    public boolean isNewFile() { return isNewFile; }

    public long getCommittedTail() { return committedTail; }
    public long getEntryCount() { return entryCount; }
    public long getLastSeq() { return lastSeq; }

    /**
     * Get direct access to mapped segment for readers/writers.
     * CAUTION: Caller must respect alignment and CRC protocols.
     */
    public MemorySegment getSegment() { return segment; }

    /**
     * Update superblock checkpoint (called by writer after batch commit).
     * This is the ONLY place we update superblock after initialization.
     */
    public void checkpointSuperblock(long newTail, long newCount, long newSeq) {
        segment.set(LONG_LE, OFF_COMMITTED_TAIL, newTail);
        segment.set(LONG_LE, OFF_ENTRY_COUNT, newCount);
        segment.set(LONG_LE, OFF_LAST_SEQ, newSeq);

        // Increment generation (optional but useful for debugging)
        long gen = segment.get(LONG_LE, OFF_GENERATION);
        segment.set(LONG_LE, OFF_GENERATION, gen + 1);

        // Recompute CRC
        int crc = computeSuperblockCRC();
        segment.set(INT_LE, OFF_SUPERBLOCK_CRC, crc);

        // Update volatiles AFTER writing (release semantics via volatile store)
        this.committedTail = newTail;
        this.entryCount = newCount;
        this.lastSeq = newSeq;

        // Optional: force to disk (can be done less frequently)
        // segment.force();
    }

    /**
     * Validate entry header at given position.
     * Returns total_len if valid, -1 if invalid.
     */
    public int validateEntryHeader(long entryStart) {
        if (entryStart < DATA_OFFSET || entryStart >= fileCapacity - ENTRY_HEADER_SIZE) {
            return -1;
        }

        int magic = segment.get(INT_LE, entryStart + ENT_MAGIC);
        if (magic != MAGIC_ENTR) return -1;

        short headerBytes = segment.get(SHORT_LE, entryStart + ENT_HEADER_BYTES);
        if (headerBytes != ENTRY_HEADER_SIZE) return -1;

        short version = segment.get(SHORT_LE, entryStart + ENT_VERSION);
        if (version != VERSION) return -1;

        int compressedLen = segment.get(INT_LE, entryStart + ENT_COMPRESSED_LEN);
        int totalLen = segment.get(INT_LE, entryStart + ENT_TOTAL_LEN);
        short paddingLen = segment.get(SHORT_LE, entryStart + ENT_PADDING_LEN);

        // Sanity check
        if (compressedLen < 0 || totalLen < ENTRY_HEADER_SIZE ||
                paddingLen < 0 || paddingLen >= ENTRY_ALIGNMENT) {
            return -1;
        }

        int expected = ENTRY_HEADER_SIZE + compressedLen + paddingLen;
        if (totalLen != expected) return -1;

        // Validate header CRC (with both CRC fields zeroed)
        int storedHeaderCrc = segment.get(INT_LE, entryStart + ENT_HEADER_CRC);
        int storedPayloadCrc = segment.get(INT_LE, entryStart + ENT_PAYLOAD_CRC);

        // Temporarily zero CRCs for validation
        segment.set(INT_LE, entryStart + ENT_HEADER_CRC, 0);
        segment.set(INT_LE, entryStart + ENT_PAYLOAD_CRC, 0);

        int computedHeaderCrc = computeCRC32C(entryStart, ENTRY_HEADER_SIZE);

        // Restore CRCs
        segment.set(INT_LE, entryStart + ENT_HEADER_CRC, storedHeaderCrc);
        segment.set(INT_LE, entryStart + ENT_PAYLOAD_CRC, storedPayloadCrc);

        if (computedHeaderCrc != storedHeaderCrc) return -1;

        // Validate payload CRC
        long payloadStart = entryStart + ENTRY_HEADER_SIZE;
        int computedPayloadCrc = computeCRC32C(payloadStart, compressedLen);

        if (computedPayloadCrc != storedPayloadCrc) return -1;

        return totalLen;
    }

    /**
     * Close segment (unmap buffer, close channel).
     * Implements AutoCloseable for try-with-resources.
     */
    @Override
    public void close() throws IOException {
        // Force any pending writes
        segment.force();

        arena.close();

        channel.close();
    }

    /**
     * Force segment to disk.
     */
    public void sync() {
        segment.force();
    }
}
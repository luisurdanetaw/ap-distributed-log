package com.luisurdaneta.log.memory.segment;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;


public final class SegmentConstants {
    private SegmentConstants() {}

    // -------------------------------------------------------------------------
    // Endianness helpers (foreign memory API)
    // -------------------------------------------------------------------------
    public static final ValueLayout.OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);
    public static final ValueLayout.OfLong LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);
    public static final ValueLayout.OfShort SHORT_LE = ValueLayout.JAVA_SHORT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // -------------------------------------------------------------------------
    // Magic values / versioning
    // -------------------------------------------------------------------------
    public static final int MAGIC_QLOG = 0x514C4F47; // "QLOG"
    public static final int MAGIC_ENTR = 0x52544E45; // "ENTR"
    public static final int VERSION = 2;

    // -------------------------------------------------------------------------
    // Layout / alignment
    // -------------------------------------------------------------------------
    public static final int SUPERBLOCK_SIZE = 4096;   // one page reserved
    public static final int SUPERBLOCK_USED = 64;     // bytes actually defined
    public static final int DATA_OFFSET = SUPERBLOCK_SIZE;
    public static final int ENTRY_ALIGNMENT = 16;
    public static final int ENTRY_HEADER_SIZE = 32;

    // -------------------------------------------------------------------------
    // Superblock offsets (absolute, bytes)
    // -------------------------------------------------------------------------
    public static final int SB_MAGIC              = 0x00; // u32
    public static final int SB_VERSION            = 0x04; // u16
    public static final int SB_HEADER_BYTES       = 0x06; // u16
    public static final int SB_FILE_UUID_HI       = 0x08; // u64
    public static final int SB_FILE_UUID_LO       = 0x10; // u64
    public static final int SB_CREATED_UNIX_NANOS = 0x18; // u64
    public static final int SB_COMMITTED_TAIL     = 0x20; // u64
    public static final int SB_FILE_CAPACITY      = 0x28; // u64
    public static final int SB_FLAGS              = 0x30; // u32
    public static final int SB_CRC32              = 0x34; // u32

    // -------------------------------------------------------------------------
    // Entry header offsets (relative to entry start)
    // -------------------------------------------------------------------------
    public static final int ENT_MAGIC         = 0x00; // u32
    public static final int ENT_HEADER_BYTES  = 0x04; // u16
    public static final int ENT_FLAGS         = 0x06; // u16
    public static final int ENT_TYPE          = 0x08; // u32
    public static final int ENT_CODEC         = 0x0C; // u32
    public static final int ENT_TS_UNIX_NANOS  = 0x10; // u64
    public static final int ENT_PAYLOAD_LEN   = 0x18; // u32
    public static final int ENT_PAYLOAD_CRC32 = 0x1C; // u32

    // -------------------------------------------------------------------------
    // Entry flag bits
    // -------------------------------------------------------------------------
    public static final int ENT_FLAG_COMPRESSED = 1 << 0;
    public static final int ENT_FLAG_HAS_CRC    = 1 << 1;
    public static final int ENT_FLAG_TOMBSTONE  = 1 << 2;

    // -------------------------------------------------------------------------
    // Misc
    // -------------------------------------------------------------------------
    public static final long PAGE_SIZE = 4096L;
    public static final long PAGE_MASK = PAGE_SIZE - 1;
    public static final long MIN_CAPACITY = SUPERBLOCK_SIZE + ENTRY_ALIGNMENT;

    // -------------------------------------------------------------------------
    // Error messages
    // -------------------------------------------------------------------------
    public static final String INITIAL_CAP_TOO_SMALL = "initialCapacity is too small: ";
    public static final String OVERFLOW_RISK = "Overflow risk for initialCapacity: ";
    public static final String FILE_TOO_SMALL = "File too small to be valid segment: ";
}
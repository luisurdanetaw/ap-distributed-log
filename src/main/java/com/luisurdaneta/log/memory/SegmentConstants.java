package com.luisurdaneta.log.memory;

final class SegmentConstants {
    private SegmentConstants() {}

    // ------------------------------------------------------------
    // File / format identifiers
    // ------------------------------------------------------------
    static final int MAGIC_QLOG = 0x514C4F47; // "QLOG"
    static final int MAGIC_ENTR = 0x52544E45; // "ENTR"
    static final int VERSION = 1;

    // ------------------------------------------------------------
    // Layout / alignment
    // ------------------------------------------------------------
    static final int SUPERBLOCK_SIZE   = 4096;
    static final int DATA_OFFSET       = 4096;
    static final int ENTRY_ALIGNMENT   = 64;
    static final int ENTRY_HEADER_SIZE = 64;

    // ------------------------------------------------------------
    // Superblock offsets (bytes)
    // ------------------------------------------------------------
    static final int OFF_MAGIC               = 0x0000;
    static final int OFF_VERSION             = 0x0004;
    static final int OFF_FLAGS               = 0x0008;
    static final int OFF_HEADER_BYTES        = 0x000C;
    static final int OFF_GENERATION          = 0x0010;
    static final int OFF_FILE_UUID           = 0x0018;
    static final int OFF_NODE_ID             = 0x0028;
    static final int OFF_CREATED_UNIX_NANOS  = 0x0030;
    static final int OFF_DATA_OFFSET         = 0x0038;
    static final int OFF_FILE_CAPACITY       = 0x0040;
    static final int OFF_COMMITTED_TAIL      = 0x0048;
    static final int OFF_ENTRY_COUNT         = 0x0050;
    static final int OFF_LAST_SEQ            = 0x0058;
    static final int OFF_DEFAULT_CODEC       = 0x0060;
    static final int OFF_SUPERBLOCK_CRC      = 0x0FF8;

    // ------------------------------------------------------------
    // Entry header offsets (bytes)
    // ------------------------------------------------------------
    static final int ENT_MAGIC              = 0x00;
    static final int ENT_HEADER_BYTES       = 0x04;
    static final int ENT_VERSION            = 0x06;
    static final int ENT_FLAGS              = 0x08;
    static final int ENT_TYPE               = 0x0C;
    static final int ENT_CODEC              = 0x0E;
    static final int ENT_SEQ                = 0x14;
    static final int ENT_TS_UNIX_NANOS      = 0x1C;
    static final int ENT_COMPRESSED_LEN     = 0x24;
    static final int ENT_UNCOMPRESSED_LEN   = 0x28;
    static final int ENT_PAYLOAD_CRC        = 0x2C;
    static final int ENT_HEADER_CRC         = 0x30;
    static final int ENT_TOTAL_LEN          = 0x34;
    static final int ENT_PADDING_LEN        = 0x38;

    static final long PAGE_SIZE = 1L << 12;   // 4096
    static final long PAGE_MASK = PAGE_SIZE - 1;
    static final long MIN_CAPACITY = SUPERBLOCK_SIZE + ENTRY_ALIGNMENT;

    // ------------------------------------------------------------
    // Error messages
    // ------------------------------------------------------------
    static final String OVERFLOW_RISK = "initialCapacity overflow risk: ";
    static final String INITIAL_CAP_TOO_SMALL = "initialCapacity too small: ";
    static final String FILE_TO_SMALL = "initialCapacity too small: ";


}
package com.luisurdaneta.log.memory;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/*
================================================================================
 QLOG SEGMENT FORMAT v2 — SIMPLIFIED APPEND-ONLY LOG
================================================================================

GOALS
-----
• Append-only, mmap-friendly on-disk layout
• All-or-nothing writes (no streaming / partial visibility)
• Optimized for large AI training blobs
• Minimal metadata, maximal sequential throughput
• Supports Last-Writer-Wins (LWW) via per-entry timestamps
• No global ordering guarantees (unordered log)

GENERAL RULES
-------------
• Little-endian for all multi-byte fields
• File begins with a reserved 4KB page (superblock region)
• Only a single commit pointer defines what is durable/visible
• Everything beyond committed_tail is undefined garbage
• Entry headers are fixed-size (32 bytes)
• Entry payloads are opaque blobs (optionally compressed)
• Entry alignment is 16 bytes (sufficient, cache-friendly)

--------------------------------------------------------------------------------
 FILE LAYOUT
--------------------------------------------------------------------------------

  +------------------------------+  offset 0
  | Superblock (reserved 4096B)  |
  |  - first 64B defined         |
  |  - remainder reserved        |
  +------------------------------+  offset 4096 (DATA_OFFSET)
  | Entry 0                      |
  |  [32B header]                |
  |  [payload bytes]             |
  |  [padding → 16B alignment]   |
  +------------------------------+
  | Entry 1                      |
  +------------------------------+
  | ...                          |
  +------------------------------+  offset = file_capacity

--------------------------------------------------------------------------------
 SUPERBLOCK (first 64 bytes of page 0)
--------------------------------------------------------------------------------
 Offset  Size  Field
 ------  ----  -----------------------------------------------
 0x00    u32   MAGIC = "QLOG"
 0x04    u16   VERSION = 2
 0x06    u16   HEADER_BYTES = 64
 0x08    u64   FILE_UUID_HI
 0x10    u64   FILE_UUID_LO
 0x18    u64   CREATED_UNIX_NANOS
 0x20    u64   COMMITTED_TAIL   (absolute file offset)
 0x28    u64   FILE_CAPACITY
 0x30    u32   FLAGS
 0x34    u32   SUPERBLOCK_CRC32 (optional; may be zero)
 0x38..0x3F    RESERVED

Only COMMITTED_TAIL defines durability & visibility.

--------------------------------------------------------------------------------
 ENTRY FORMAT (append-only)
--------------------------------------------------------------------------------
 Each entry is written fully, then COMMITTED_TAIL is advanced.

 Entry layout:
   [32B fixed header]
   [payload_len bytes payload]
   [0-15B padding to 16B alignment]

 Padding length is derived; never stored.

--------------------------------------------------------------------------------
 ENTRY HEADER (32 bytes)
--------------------------------------------------------------------------------
 Offset  Size  Field
 ------  ----  -----------------------------------------------
 0x00    u32   MAGIC = "ENTR"
 0x04    u16   HEADER_BYTES = 32
 0x06    u16   FLAGS
                 bit 0 → payload compressed
                 bit 1 → payload_crc32 valid
                 bit 2 → tombstone (delete marker)
 0x08    u32   TYPE        (application-defined)
 0x0C    u32   CODEC       (0 = none, 1 = zstd, ...)
 0x10    u64   TS_UNIX_NANOS   (used for LWW)
 0x18    u32   PAYLOAD_LEN    (bytes on disk)
 0x1C    u32   PAYLOAD_CRC32  (optional; 0 if unused)

 total_len = HEADER_BYTES + PAYLOAD_LEN + padding

================================================================================
*/

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
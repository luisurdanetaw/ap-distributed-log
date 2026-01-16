package com.luisurdaneta.log.memory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.luisurdaneta.log.memory.SegmentConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class LogSegmentIntegrationTest {

    @TempDir
    Path dir;

    @Test
    void loadOrInit_createsAndReopens_withValidSuperblock_andPersistsCheckpoint() throws Exception {
        Path file = dir.resolve("segment.qlog");

        long nodeId = 42L;
        long initialCapacity = MIN_CAPACITY + (64L * 1024); // comfortably above min

        // 1) Create new segment
        try (LogSegment seg = LogSegment.loadOrInit(file, initialCapacity, nodeId)) {
            assertTrue(seg.isNewFile(), "Expected new file on first open");
            assertTrue(Files.exists(file), "Segment file should exist");

            // File size should match mapped capacity and be page-aligned
            assertEquals(seg.getFileCapacity(), Files.size(file));
            assertEquals(0L, seg.getFileCapacity() & PAGE_MASK, "Capacity must be 4096-aligned");

            // Superblock sanity via public getters + direct reads
            assertEquals(nodeId, seg.getNodeId());
            assertEquals(DATA_OFFSET, seg.getCommittedTail());
            assertEquals(0L, seg.getEntryCount());
            assertEquals(0L, seg.getLastSeq());

            MemorySegment ms = seg.getSegment();
            assertEquals(MAGIC_QLOG, ms.get(INT_LE, OFF_MAGIC));
            assertEquals(VERSION, ms.get(INT_LE, OFF_VERSION));
            assertEquals(SUPERBLOCK_SIZE, ms.get(INT_LE, OFF_HEADER_BYTES));
            assertEquals(DATA_OFFSET, ms.get(LONG_LE, OFF_DATA_OFFSET));
            assertEquals(seg.getFileCapacity(), ms.get(LONG_LE, OFF_FILE_CAPACITY));

            // 2) Mutate checkpoint and force durability
            long newTail = DATA_OFFSET + 64;
            long newCount = 7;
            long newSeq = 7;
            seg.checkpointSuperblock(newTail, newCount, newSeq);
            seg.sync();

            // Verify getters reflect the update (volatiles)
            assertEquals(newTail, seg.getCommittedTail());
            assertEquals(newCount, seg.getEntryCount());
            assertEquals(newSeq, seg.getLastSeq());
        }

        // 3) Reopen existing segment and validate persisted state
        try (LogSegment seg2 = LogSegment.loadOrInit(file, initialCapacity, nodeId)) {
            assertFalse(seg2.isNewFile(), "Expected existing file on second open");

            // Must have persisted checkpoint values
            assertEquals(DATA_OFFSET + 64, seg2.getCommittedTail());
            assertEquals(7L, seg2.getEntryCount());
            assertEquals(7L, seg2.getLastSeq());

            // Superblock still valid (validateSuperblock is called inside loadOrInit)
            MemorySegment ms2 = seg2.getSegment();
            assertEquals(MAGIC_QLOG, ms2.get(INT_LE, OFF_MAGIC));
            assertEquals(VERSION, ms2.get(INT_LE, OFF_VERSION));
        }
    }

    @Test
    void loadOrInit_rejectsTooSmallCapacityForNewFile() throws IOException {
        Path file = dir.resolve("segment.qlog");

        long tooSmall = MIN_CAPACITY - 1;

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> LogSegment.loadOrInit(file, tooSmall, 1L)
        );
        assertTrue(ex.getMessage().contains("too small"));
        assertFalse(Files.exists(file), "Should not create file on invalid capacity");
    }
}
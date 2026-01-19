package com.luisurdaneta.log;


import com.luisurdaneta.log.memory.LogReader;
import com.luisurdaneta.log.memory.LogSegment;
import com.luisurdaneta.log.memory.LogWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.luisurdaneta.log.memory.SegmentConstants.DATA_OFFSET;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for v2 layout:
 *   LogSegment + LogWriter + LogReader
 *
 * Writes a few entries ("Hello", "World", etc) and verifies the reader
 * returns the exact payload bytes in order.
 */
public class LogIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void appendAndReadBack_HelloWorld() throws IOException {
        Path file = tempDir.resolve("segment.qlog");

        // Keep it comfortably large for the test
        long initialCapacity = 1L << 20; // 1 MiB

        // --- Create new segment, write entries, commit ---
        long tailAfterWrite;
        try (LogSegment seg = LogSegment.loadOrInit(file, initialCapacity);
             LogWriter writer = new LogWriter(seg)) {

            assertTrue(seg.isNewFile(), "Expected new file");

            writer.append("Hello".getBytes());
            writer.append("World".getBytes());
            writer.append("!".getBytes());

            writer.commit(true); // commit + force for test determinism

            tailAfterWrite = seg.getCommittedTail();
            assertTrue(tailAfterWrite > DATA_OFFSET, "Committed tail should advance after writes");
        }

        // --- Re-open and read entries ---
        try (LogSegment seg2 = LogSegment.loadOrInit(file, initialCapacity)) {
            assertFalse(seg2.isNewFile(), "Expected existing file on reopen");

            LogReader reader = new LogReader(seg2);
            long tailSnap = reader.committedTailSnapshot();
            assertEquals(tailAfterWrite, tailSnap, "Tail snapshot should match tail after write");

            List<byte[]> payloads = new ArrayList<>();
            MemorySegment ms = seg2.getSegment();

            long endOff = reader.scan(DATA_OFFSET, tailSnap, Long.MAX_VALUE,
                    (entryStart, totalLen, flags, type, codec, tsNanos, payloadOff, payloadLen, payloadCrc32) -> {
                        byte[] b = new byte[payloadLen];
                        MemorySegment.copy(ms, payloadOff, MemorySegment.ofArray(b), 0, payloadLen);
                        payloads.add(b);

                        // sanity
                        assertTrue(totalLen >= 32 + payloadLen, "totalLen must include header+payload (+padding)");
                        assertTrue(tsNanos > 0, "tsNanos should be set for LWW");

                        return true;
                    });

            // Reader should end exactly at tail (entries are contiguous, derived padding)
            assertEquals(tailSnap, endOff, "Scan should end at committed tail");

            assertEquals(3, payloads.size());
            assertEquals("Hello", new String(payloads.get(0)));
            assertEquals("World", new String(payloads.get(1)));
            assertEquals("!", new String(payloads.get(2)));
        }
    }

    @Test
    void uncommittedEntriesAreInvisible() throws IOException {
        Path file = tempDir.resolve("segment_uncommitted.qlog");
        long initialCapacity = 1L << 20;

        try (LogSegment seg = LogSegment.loadOrInit(file, initialCapacity)) {
            long initialTail = seg.getCommittedTail();

            try (LogWriter writer = new LogWriter(seg)) {
                writer.append("NotCommitted".getBytes());
                LogReader reader = new LogReader(seg);
                List<byte[]> payloads = new ArrayList<>();
                MemorySegment ms = seg.getSegment();

                long tailSnap = reader.committedTailSnapshot();
                long endOff = reader.scan(DATA_OFFSET, tailSnap, Long.MAX_VALUE,
                        (entryStart, totalLen, flags, type, codec, tsNanos, payloadOff, payloadLen, payloadCrc32) -> {
                            byte[] b = new byte[payloadLen];
                            MemorySegment.copy(ms, payloadOff, MemorySegment.ofArray(b), 0, payloadLen);
                            payloads.add(b);
                            return true;
                        });

                assertEquals(tailSnap, endOff);
                assertEquals(0, payloads.size(), "No entries should be visible without commit");
            }
        }
    }
}
package com.luisurdaneta.log.memory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import static com.luisurdaneta.log.memory.SegmentConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class LogReaderIntegrationTest {

    @TempDir
    Path dir;

    @Test
    void scan_readsBackValidEntries_andStopsOnHandlerFalse() throws Exception {
        Path tmp = dir.resolve("seg.qlog");
        long capacity = 8L * 1024 * 1024; // 8MB

        try (LogSegment seg = LogSegment.loadOrInit(tmp, capacity, 1L)) {

            MemorySegment ms = seg.getSegment();

            // --- Manually append 3 entries into the file region ---
            long off = DATA_OFFSET;

            off = writeEntry(ms, off, 1, "hello".getBytes(), 0, 1, 0);
            off = writeEntry(ms, off, 2, "world".getBytes(), 0, 1, 0);
            off = writeEntry(ms, off, 3, "bro".getBytes(),   0, 1, 0);

            // IMPORTANT: we have not updated superblock committed tail in this test
            // so don't call scanAll(). Pass the "tailSnapshot" explicitly.
            long tailSnapshot = off;

            LogReader reader = new LogReader(seg);

            List<Long> seqs = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();

            long next = reader.scan(DATA_OFFSET, tailSnapshot, Long.MAX_VALUE,
                    (entryStart, totalLen, flags, type, codec, seq, tsNanos, payloadOff, payloadLen, rawLen) -> {

                        seqs.add(seq);

                        byte[] p = new byte[payloadLen];
                        MemorySegment.copy(ms, payloadOff, MemorySegment.ofArray(p), 0, payloadLen);
                        payloads.add(p);

                        // stop after seq=2
                        return seq != 2;
                    });

            assertEquals(List.of(1L, 2L), seqs);
            assertArrayEquals("hello".getBytes(), payloads.get(0));
            assertArrayEquals("world".getBytes(), payloads.get(1));

            // Must advance past the entry where handler returned false.
            assertTrue(next > DATA_OFFSET);
            assertTrue(next < tailSnapshot);
        }
    }

    private static long writeEntry(MemorySegment ms,
                                   long entryStart,
                                   long seq,
                                   byte[] payload,
                                   int flags,
                                   int type,
                                   int codec) throws IOException {

        int payloadLen = payload.length;

        // padding to next ENTRY_ALIGNMENT boundary
        int base = ENTRY_HEADER_SIZE + payloadLen;
        int padding = (ENTRY_ALIGNMENT - (base & (ENTRY_ALIGNMENT - 1))) & (ENTRY_ALIGNMENT - 1);

        int totalLen = ENTRY_HEADER_SIZE + payloadLen + padding;

        long payloadOff = entryStart + ENTRY_HEADER_SIZE;

        // payload
        MemorySegment.copy(MemorySegment.ofArray(payload), 0, ms, payloadOff, payloadLen);

        // zero padding bytes (optional)
        for (int i = 0; i < padding; i++) {
            ms.set(ValueLayout.JAVA_BYTE, payloadOff + payloadLen + i, (byte) 0);
        }

        int payloadCrc = crc32c(payload);

        // header fields
        ms.set(INT_LE,   entryStart + ENT_MAGIC, MAGIC_ENTR);
        ms.set(SHORT_LE, entryStart + ENT_HEADER_BYTES, (short) ENTRY_HEADER_SIZE);
        ms.set(SHORT_LE, entryStart + ENT_VERSION, (short) VERSION);

        ms.set(INT_LE,   entryStart + ENT_FLAGS, flags);
        ms.set(SHORT_LE, entryStart + ENT_TYPE, (short) type);
        ms.set(SHORT_LE, entryStart + ENT_CODEC, (short) codec);

        ms.set(LONG_LE,  entryStart + ENT_SEQ, seq);
        ms.set(LONG_LE,  entryStart + ENT_TS_UNIX_NANOS, 123456789L + seq);

        ms.set(INT_LE,   entryStart + ENT_COMPRESSED_LEN, payloadLen);
        ms.set(INT_LE,   entryStart + ENT_UNCOMPRESSED_LEN, payloadLen);

        ms.set(INT_LE,   entryStart + ENT_TOTAL_LEN, totalLen);
        ms.set(SHORT_LE, entryStart + ENT_PADDING_LEN, (short) padding);

        // store payload CRC
        ms.set(INT_LE, entryStart + ENT_PAYLOAD_CRC, payloadCrc);

        // compute + store header CRC (with both crc fields treated as zero)
        int headerCrc = computeHeaderCrcZeroed(ms, entryStart);
        ms.set(INT_LE, entryStart + ENT_HEADER_CRC, headerCrc);

        return entryStart + totalLen;
    }

    private static int crc32c(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data, 0, data.length);
        return (int) crc.getValue();
    }

    private static int computeHeaderCrcZeroed(MemorySegment ms, long entryStart) {
        byte[] hdr = new byte[ENTRY_HEADER_SIZE];
        MemorySegment.copy(ms, entryStart, MemorySegment.ofArray(hdr), 0, ENTRY_HEADER_SIZE);

        // zero payload_crc and header_crc
        int p = (int) ENT_PAYLOAD_CRC;
        int h = (int) ENT_HEADER_CRC;

        hdr[p] = hdr[p + 1] = hdr[p + 2] = hdr[p + 3] = 0;
        hdr[h] = hdr[h + 1] = hdr[h + 2] = hdr[h + 3] = 0;

        CRC32C crc = new CRC32C();
        crc.update(hdr, 0, hdr.length);
        return (int) crc.getValue();
    }

}

package com.luisurdaneta.log.network.protocol.handler;

import com.luisurdaneta.log.memory.segment.LogSegment;
import com.luisurdaneta.log.memory.io.LogWriter;
import com.luisurdaneta.log.util.LogChannelIO;
import com.luisurdaneta.log.network.protocol.constants.LogMessageTypes;
import com.luisurdaneta.log.network.protocol.state.LogProtocolRules;
import com.luisurdaneta.log.network.protocol.state.LogConnContext;
import com.luisurdaneta.log.network.protocol.state.LogConnState;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import static com.luisurdaneta.log.network.protocol.state.LogConnState.*;
import static com.luisurdaneta.log.network.protocol.constants.LogMessageTypes.*;
import static com.luisurdaneta.log.network.protocol.constants.LogProtocolConstants.*;
import static com.luisurdaneta.log.network.protocol.state.LogProtocolRules.*;


/**
 *
 * HANDLES INCOMING TCP CONNECTIONS WITH THE FOLLOWING PROTOCOL:

 * =============================================================================
 * LOG TCP Streaming Protocol v1
 * =============================================================================
 *
 * <p>DESIGN PHILOSOPHY:
 * - Simple framing with length-prefixed messages
 * - The client logically sends a full segment as a length-prefixed stream.
 * - TCP transports it as a byte stream with no message boundaries. T
 * - Server incrementally reads and processes the payload in fixed-size 4kb chunks.
 * - HELLO handshake establishes connection
 * - Streaming WRITE and READ operations for segments
 * - PING/PONG for connection keepalive and latency measurement</p>
 *
 * <p>WIRE FORMAT:
 * ------------
 * Every message has a 16-byte header:

 * 0x00 u32 magic            = 0x514C4F47 ("QLOG" in ASCII)
 * 0x04 u32 message_type     (HELLO=1, ACK=2, WRITE=3, READ=4, DATA=5, ERROR=6, PING=7, PONG=8)
 * 0x08 u64 payload_length   (bytes following this header)</p>
 *
 * <p>MESSAGE FLOWS:
 * --------------</p>
 *
 * <p>1. CONNECTION HANDSHAKE
 * ────────────────────────
 * Client → Server: HELLO | length=16
 * [8 bytes protocol_version = 1]
 * [8 bytes client_capabilities]

 * Server → Client: ACK | length=16
 * [8 bytes protocol_version = 1]
 * [8 bytes server_capabilities]</p>
 *
 * <p>2. WRITE SEGMENT (Client pushes segment to server)
 * ───────────────────────────────────────────────────
 * Client → Server: WRITE | length=24
 * [8 bytes uuid_hi]
 * [8 bytes uuid_lo]
 * [8 bytes segment_total_bytes]

 * Server → Client: ACK | length=0

 * Client → Server: DATA | length=N (full segment, sent once)
 * [N bytes of segment data]

 * Server → Client: ACK | length=8
 * [8 bytes bytes_written]</p>
 *
 * <p>3. READ SEGMENT (Client pulls segment from server)
 * ───────────────────────────────────────────────────
 * Client → Server: READ | length=16
 * [8 bytes uuid_hi]
 * [8 bytes uuid_lo]

 * Server → Client: ACK | length=8
 * [8 bytes total_segment_bytes]

 * Server → Client: DATA | length=4096 (chunked)
 * [4096 bytes of data]
 * ... (repeat until complete)

 * Server → Client: DATA | length=remaining (<4096, final chunk)
 * [remaining bytes]

 * Client → Server: ACK | length=0</p>
 *
 * <p>4. ERROR HANDLING
 * ─────────────────
 * Either → Either: ERROR | length=N
 * [N bytes UTF-8 error message]</p>
 *
 * <p>5. PING/PONG (Keepalive & Latency Measurement)
 * ───────────────────────────────────────────────
 * Either → Either: PING | length=8
 * [8 bytes timestamp_nanos]

 * Either → Either: PONG | length=8
 * [8 bytes timestamp_nanos (echo from PING)]</p>
 *
 * <p>Purpose:
 * • Keepalive: Detect dead connections during idle periods
 * • Latency: Measure round-trip time (RTT)
 * • Health: Verify peer is responsive</p>
 *
 * <p>Behavior:
 * - PING/PONG can be sent at ANY time (even during WRITE/READ)
 * - Recipient MUST respond immediately with PONG echoing exact timestamp
 * - PING does NOT block or interrupt ongoing operations
 * - Both client and server can initiate PING</p>
 *
 * <p>Recommended Usage:
 * - Send PING every 30-60 seconds during idle periods
 * - If no PONG within 10 seconds, consider connection dead
 * - During active transfers, PING is optional (DATA proves liveness)</p>
 *
 * <p>Example Flow:
 * [30 seconds idle]
 * Client → Server: PING | length=8 [timestamp: 1234567890123456789]
 * Server → Client: PONG | length=8 [timestamp: 1234567890123456789]
 * Client calculates RTT = current_time - sent_timestamp</p>
 *
 * <p>PROTOCOL VALIDATION:
 * ────────────────────
 * - Every message MUST start with magic 0x514C4F47 ("QLOG")
 * - If magic doesn't match, immediately close connection
 * - This prevents accidental connections from non-QLOG clients</p>
 *
 * <p>CAPABILITIES FLAGS:
 * ───────────────────
 * Bit 0: Supports compression (reserved for future)
 * Bit 1: Supports encryption (reserved for future)
 * Bit 2-63: Reserved</p>
 */

public final class LogConnectionHandler implements ConnectionHandler {

    private static final long SERVER_CAPABILITIES = 0L;

    @Override
    public void handle(SocketChannel ch, LogSegment segment) {
        final LogConnContext ctx = new LogConnContext();

        // Inbound buffers (reused)
        final ByteBuffer inHeader = ByteBuffer.allocateDirect(HEADER_BYTES).order(WIRE_ORDER);
        final ByteBuffer inHello16 = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);
        final ByteBuffer inPing8 = ByteBuffer.allocateDirect(8).order(WIRE_ORDER);
        final ByteBuffer inRead16 = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);
        final ByteBuffer inWrite24 = ByteBuffer.allocateDirect(24).order(WIRE_ORDER);

        // Scratch drain buffer (reused, 4KB chunks)
        final ByteBuffer scratch4k = ByteBuffer.allocateDirect(4096);

        // Outbound buffers (reused)
        final ByteBuffer outHeader = ByteBuffer.allocateDirect(HEADER_BYTES).order(WIRE_ORDER);
        final ByteBuffer out8 = ByteBuffer.allocateDirect(8).order(WIRE_ORDER);
        final ByteBuffer out16 = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);

        final LogWriter writer = new LogWriter(segment);

        final MemorySegment scratchSeg = MemorySegment.ofBuffer(scratch4k);

        try {
            // 1) Must do HELLO first
            if (!performHelloHandshake(ch, ctx, inHeader, inHello16, outHeader, out16)) {
                return;
            }

            System.out.println("handshake done");

            // 2) After HELLO, main loop
            for(;;) {
                System.out.println("looping");

                // ---- Read next frame header ----
                inHeader.clear();
                if (!LogChannelIO.readFully(ch, inHeader)) {
                    return; // remote closed
                }
                inHeader.flip();

                LogProtocolRules.parseHeaderInto(ctx, inHeader);

                // ---- Validate header invariants ----
                int hv = LogProtocolRules.validateHeader(ctx);
                if (hv == ERR_BAD_MAGIC) {
                    // bad magic => immediate close
                    return;
                }
                if (hv != OK) {
                    sendErrorAndClose(ch, outHeader, "Invalid payload_length: " + ctx.hdrPayloadLen);
                    return;
                }

                // ---- Validate allowed by state ----
                if (!LogProtocolRules.isAllowed(ctx.state, ctx.hdrType)) {
                    sendErrorAndClose(ch, outHeader,
                            "Message " + LogMessageTypes.nameOf(ctx.hdrType) +
                                    " not allowed in state " + LogConnState.nameOf(ctx.state));
                    return;
                }

                // ---- Dispatch by message type (state machine actions only) ----
                switch (ctx.hdrType) {

                    // =========================================================
                    // CONTROL PLANE: PING/PONG/ERROR
                    // =========================================================
                    case PING: {
                        // payload must be 8
                        if (ctx.hdrPayloadLen != 8) {
                            sendErrorAndClose(ch, outHeader, "PING payload_length must be 8");
                            return;
                        }

                        // Read timestamp
                        inPing8.clear();
                        if (!LogChannelIO.readFully(ch, inPing8)) return;
                        inPing8.flip();
                        long ts = inPing8.getLong();

                        // Respond PONG echoing ts
                        writeHeader(outHeader, PONG, 8);
                        out8.clear();
                        out8.putLong(ts);
                        out8.flip();

                        LogChannelIO.writeFully(ch, outHeader);
                        LogChannelIO.writeFully(ch, out8);
                        break;
                    }

                    case PONG: {
                        // payload must be 8 (spec). We just drain it.
                        if (ctx.hdrPayloadLen != 8) {
                            sendErrorAndClose(ch, outHeader, "PONG payload_length must be 8");
                            return;
                        }
                        if (!LogChannelIO.drainFully(ch, 8, scratch4k)) return;
                        break;
                    }

                    case ERROR: {
                        // Peer sent an error; drain the payload and close.
                        if (!LogChannelIO.drainFully(ch, ctx.hdrPayloadLen, scratch4k)) return;
                        return;
                    }

                    // =========================================================
                    // READY: accept WRITE / READ
                    // =========================================================
                    case WRITE: {
                        // Only valid in READY due to isAllowed()
                        if (ctx.hdrPayloadLen != 24) {
                            sendErrorAndClose(ch, outHeader, "WRITE payload_length must be 24");
                            return;
                        }

                        inWrite24.clear();
                        if (!LogChannelIO.readFully(ch, inWrite24)) return;
                        inWrite24.flip();

                        long uuidHi = inWrite24.getLong();
                        long uuidLo = inWrite24.getLong();
                        long segmentBytes = inWrite24.getLong();

                        if (segmentBytes < 0 || segmentBytes > MAX_SEGMENT_BYTES) {
                            sendErrorAndClose(ch, outHeader, "Invalid segment_total_bytes: " + segmentBytes);
                            return;
                        }

                        // Track op + transition to WRITE_RECV
                        ctx.beginOp(uuidHi, uuidLo, segmentBytes);
                        ctx.state = WRITE_RECV;

                        // Reply ACK(payload=0)
                        writeHeader(outHeader, ACK, 0);
                        LogChannelIO.writeFully(ch, outHeader);

                        break;
                    }

                    case DATA: {
                        // Only valid in WRITE_RECV due to isAllowed()
                        final long payloadLen = ctx.hdrPayloadLen;

                        // For v1: payloadLen must equal segment_total_bytes (full segment sent once)
                        if (payloadLen != ctx.expectedSegmentBytes) {
                            sendErrorAndClose(ch, outHeader,
                                    "DATA length mismatch. expected=" + ctx.expectedSegmentBytes +
                                            " got=" + payloadLen);
                            return;
                        }

                        // Stream directly into mmap via LogWriter (no buffering of full payload)
                        try {
                            writer.beginStreaming(payloadLen);

                            long remaining = payloadLen;

                            while (remaining > 0) {
                                scratch4k.clear();

                                int toRead = (int) Math.min(scratch4k.capacity(), remaining);
                                scratch4k.limit(toRead);

                                // Fill scratch with exactly toRead bytes from the socket
                                if (!LogChannelIO.readFully(ch, scratch4k)) {
                                    // connection ended mid-stream; discard partial (uncommitted)
                                    writer.abortStreaming();
                                    return;
                                }

                                scratch4k.flip();

                                // Copy into mmap + update CRC incrementally (zero-alloc path)
                                writer.writeChunk(scratch4k, scratchSeg, toRead);

                                remaining -= toRead;
                            }
                            // Finalize header/padding + advance tail (still not visible until commit)
                            writer.finishStreaming();
                            writer.commit(false);
                        } catch (Throwable t) {
                            try { writer.abortStreaming(); } catch (Throwable ignored) {}
                            sendErrorAndClose(ch, outHeader, "Write failed: " + t.getMessage());
                            return;
                        }



                        ctx.bytesTransferred = payloadLen;

                        // Reply ACK(payload=8) bytes_written
                        writeHeader(outHeader, ACK, 8);
                        out8.clear();
                        out8.putLong(payloadLen);
                        out8.flip();

                        LogChannelIO.writeFully(ch, outHeader);
                        LogChannelIO.writeFully(ch, out8);

                        // Operation complete -> READY
                        ctx.clearOp();
                        ctx.state = READY;

                        break;
                    }

                    case READ: {
                        // Only valid in READY due to isAllowed()
                        if (ctx.hdrPayloadLen != 16) {
                            sendErrorAndClose(ch, outHeader, "READ payload_length must be 16");
                            return;
                        }

                        inRead16.clear();
                        if (!LogChannelIO.readFully(ch, inRead16)) return;
                        inRead16.flip();

                        long uuidHi = inRead16.getLong();
                        long uuidLo = inRead16.getLong();

                        // We are NOT reading real segment data yet.
                        // To keep state-machine correct + client unblocked, we simulate an empty segment:
                        long totalBytes = 0L;

                        ctx.beginOp(uuidHi, uuidLo, totalBytes);
                        ctx.state = READ_SEND;

                        // Send ACK(payload=8) total_segment_bytes = 0
                        writeHeader(outHeader, ACK, 8);
                        out8.clear();
                        out8.putLong(totalBytes);
                        out8.flip();
                        LogChannelIO.writeFully(ch, outHeader);
                        LogChannelIO.writeFully(ch, out8);

                        // Send DATA(payload=0) => empty segment (protocol-valid)
                        writeHeader(outHeader, DATA, 0);
                        LogChannelIO.writeFully(ch, outHeader);

                        // Now wait for client's final ACK(0) in READ_SEND state
                        break;
                    }

                    case ACK: {
                        // Only valid in READ_SEND due to isAllowed()
                        if (ctx.hdrPayloadLen != 0) {
                            sendErrorAndClose(ch, outHeader, "Final ACK payload_length must be 0");
                            return;
                        }

                        // READ is complete -> READY
                        ctx.clearOp();
                        ctx.state = READY;
                        break;
                    }

                    // =========================================================
                    // HELLO should never appear again
                    // =========================================================
                    case HELLO: {
                        sendErrorAndClose(ch, outHeader, "HELLO already completed");
                        return;
                    }

                    default: {
                        sendErrorAndClose(ch, outHeader, "Unknown message type: " + ctx.hdrType);
                        return;
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("Connection IO error: " + e.getMessage());
        } finally {
            System.out.println("exiting");
            closeQuietly(ch);
        }
    }

    private static boolean performHelloHandshake(
            SocketChannel ch,
            LogConnContext ctx,
            ByteBuffer inHeader,
            ByteBuffer inHello,
            ByteBuffer outHeader,
            ByteBuffer outAck16
    ) throws IOException {

        ctx.state = EXPECT_HELLO;

        // ---- Read header (16 bytes) ----
        inHeader.clear();
        if (!LogChannelIO.readFully(ch, inHeader)) {
            return false; // remote closed
        }
        inHeader.flip();

        // Parse header into ctx fields (no object allocation)
        LogProtocolRules.parseHeaderInto(ctx, inHeader);

        // Validate magic/length invariants
        int hv = LogProtocolRules.validateHeader(ctx);
        if (hv == ERR_BAD_MAGIC) {
            // Bad magic => close immediately (no ERROR)
            return false;
        }
        if (hv != OK) {
            // Bad payload length => protocol error
            sendErrorAndClose(ch, outHeader, "Invalid payload_length: " + ctx.hdrPayloadLen);
            return false;
        }

        // Enforce HELLO first
        if (ctx.hdrType != HELLO) {
            sendErrorAndClose(ch, outHeader, "HELLO required");
            return false;
        }

        // Validate HELLO payload length = 16
        if (ctx.hdrPayloadLen != 16) {
            sendErrorAndClose(ch, outHeader, "HELLO payload_length must be 16");
            return false;
        }

        // ---- Read HELLO payload (16 bytes) ----
        inHello.clear();
        if (!LogChannelIO.readFully(ch, inHello)) {
            return false; // remote closed mid-frame
        }
        inHello.flip();

        long clientVersion = inHello.getLong();
        long clientCaps    = inHello.getLong();

        // Validate protocol version
        if (clientVersion != PROTOCOL_VERSION) {
            sendErrorAndClose(ch, outHeader, "Unsupported protocol version: " + clientVersion);
            return false;
        }

        // Store negotiated info
        ctx.negotiatedPeerVersion = clientVersion;
        ctx.negotiatedPeerCaps = clientCaps;

        // ---- Send ACK payload=16 ----
        // header: MAGIC + ACK + payloadLen(16)
        writeHeader(outHeader, ACK, 16);

        // payload: u64 version=1, u64 serverCaps
        outAck16.clear();
        outAck16.putLong(PROTOCOL_VERSION);
        outAck16.putLong(SERVER_CAPABILITIES);
        outAck16.flip();

        LogChannelIO.writeFully(ch, outHeader);
        LogChannelIO.writeFully(ch, outAck16);

        // Transition state
        ctx.state = READY;
        return true;
    }

    /** Writes a 16-byte protocol header into outHeader buffer (reused). */
    private static void writeHeader(ByteBuffer outHeader, int msgType, long payloadLen) {
        outHeader.clear();
        outHeader.putInt(MAGIC_QLOG);
        outHeader.putInt(msgType);
        outHeader.putLong(payloadLen);
        outHeader.flip();
    }

    /** ERROR frame + close. (Allocations are OK here; error path only.) */
    private static void sendErrorAndClose(SocketChannel ch, ByteBuffer outHeader, String msg) {
        try {
            byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);

            writeHeader(outHeader, ERROR, bytes.length);
            LogChannelIO.writeFully(ch, outHeader);
            LogChannelIO.writeFully(ch, ByteBuffer.wrap(bytes));
        } catch (IOException ignored) {
            // if we can't send error, just close
        } finally {
            closeQuietly(ch);
        }
    }

    private static void closeQuietly(SocketChannel ch) {
        try { ch.close(); } catch (IOException ignored) {}
    }
}
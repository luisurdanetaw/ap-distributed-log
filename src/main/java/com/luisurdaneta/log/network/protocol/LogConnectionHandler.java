package com.luisurdaneta.log.network.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import static com.luisurdaneta.log.network.protocol.LogConnState.*;
import static com.luisurdaneta.log.network.protocol.LogMessageTypes.*;
import static com.luisurdaneta.log.network.protocol.LogProtocolConstants.*;
import static com.luisurdaneta.log.network.protocol.LogProtocolRules.*;


/**
 * =============================================================================
 * QLOG TCP Streaming Protocol v1
 * =============================================================================
 *
 * <p>DESIGN PHILOSOPHY:
 * - Simple framing with length-prefixed messages
 * - Client sends entire segments at once; server streams with 4KB chunks
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
 * • PING/PONG can be sent at ANY time (even during WRITE/READ)
 * • Recipient MUST respond immediately with PONG echoing exact timestamp
 * • PING does NOT block or interrupt ongoing operations
 * • Both client and server can initiate PING</p>
 *
 * <p>Recommended Usage:
 * • Send PING every 30-60 seconds during idle periods
 * • If no PONG within 10 seconds, consider connection dead
 * • During active transfers, PING is optional (DATA proves liveness)</p>
 *
 * <p>Example Flow:
 * [30 seconds idle]
 * Client → Server: PING | length=8 [timestamp: 1234567890123456789]
 * Server → Client: PONG | length=8 [timestamp: 1234567890123456789]
 * Client calculates RTT = current_time - sent_timestamp</p>
 *
 * <p>PROTOCOL VALIDATION:
 * ────────────────────
 * • Every message MUST start with magic 0x514C4F47 ("QLOG")
 * • If magic doesn't match, immediately close connection
 * • This prevents accidental connections from non-QLOG clients</p>
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
    public void handle(SocketChannel ch) {
        final LogConnContext ctx = new LogConnContext();

        final ByteBuffer inHeader  = ByteBuffer.allocateDirect(HEADER_BYTES).order(WIRE_ORDER);
        final ByteBuffer inHello   = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);

        final ByteBuffer outHeader = ByteBuffer.allocateDirect(HEADER_BYTES).order(WIRE_ORDER);
        final ByteBuffer outAck16  = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);

        try {
            if (!performHelloHandshake(ch, ctx, inHeader, inHello, outHeader, outAck16)) {
                return; // handshake failed or remote closed
            }

            while (true) {

                return; // Step 0/1 only
            }
        } catch (Exception e) {
            System.err.println("Connection error: " + e.getMessage());
        } finally {
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
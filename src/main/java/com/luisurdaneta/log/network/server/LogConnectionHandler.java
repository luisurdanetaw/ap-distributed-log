package com.luisurdaneta.log.network.server;
import java.io.EOFException;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Objects;

public final class LogConnectionHandler implements ConnectionHandler {

    // ---- Protocol constants ----
    private static final int MAGIC_QLWP = 0x514C5750; // "QLWP"
    private static final int VERSION_V1 = 1;

    private static final int HEADER_SIZE = 32;
    private static final int LEN_PREFIX_SIZE = 4;

    // Opcodes
    private static final int OP_HELLO = 0x0001;

    private static final int OP_OK = 0x8000;
    private static final int OP_ERROR = 0x8001;

    // HELLO feature bits (client advertised)
    private static final int FEAT_APPEND_STREAM = 1 << 0;  // supports APPEND_STREAM_* opcodes
    private static final int FEAT_CHUNK_ACK     = 1 << 1;  // supports per-chunk ACK (optional)
    private static final int FEAT_CHUNK_CRC32C  = 1 << 2;  // supports CRC32C on chunks (optional)

    // ---- Server config defaults ----
    private final int serverMaxFrameBytes;     // hard max frame bytes (header+body)
    private final int serverMaxChunkBytes;     // recommended chunk ceiling for streaming
    private final int supportedCodecsBitset;  // server supported codecs
    private final long nodeId;                // stable node id (or random)

    // Per-connection negotiated (HELLO sets these)
    private int negotiatedMaxFrameBytes;

    public LogConnectionHandler(
            int serverMaxFrameBytes,
            int serverMaxChunkBytes,
            int supportedCodecsBitset,
            long nodeId
    ) {
        if (serverMaxFrameBytes < HEADER_SIZE) {
            throw new IllegalArgumentException("serverMaxFrameBytes must be >= " + HEADER_SIZE);
        }
        this.serverMaxFrameBytes = serverMaxFrameBytes;
        this.serverMaxChunkBytes = serverMaxChunkBytes;
        this.supportedCodecsBitset = supportedCodecsBitset;
        this.nodeId = nodeId;

        this.negotiatedMaxFrameBytes = serverMaxFrameBytes; // until HELLO
    }

    @Override
    public void handle(SocketChannel ch) {
        Objects.requireNonNull(ch, "ch");

        try (ch) {
            // Virtual thread friendly: blocking IO is fine.
            ch.configureBlocking(true);

            final ByteBuffer lenBuf = ByteBuffer.allocate(LEN_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);

            while (true) {
                lenBuf.clear();
                if (!readFully(ch, lenBuf)) {
                    // clean EOF
                    return;
                }
                lenBuf.flip();

                int frameLen = lenBuf.getInt(); // bytes after length prefix (header + body)

                // Validate frameLen (unsigned u32 in spec, but we enforce sane bounds)
                if (frameLen < HEADER_SIZE) {
                    // protocol violation
                    sendError(ch, /*requestId*/ 0L, 1001, "frame_len < 32");
                    return;
                }
                if (frameLen > negotiatedMaxFrameBytes) {
                    sendError(ch, /*requestId*/ 0L, 1002, "frame_len exceeds negotiated max");
                    return;
                }

                ByteBuffer frameBuf = ByteBuffer.allocate(frameLen).order(ByteOrder.LITTLE_ENDIAN);
                if (!readFully(ch, frameBuf)) {
                    throw new EOFException("EOF mid-frame");
                }
                frameBuf.flip();

                // Parse common header (32 bytes)
                // We use absolute gets for clarity.
                int magic = frameBuf.getInt(0x00);
                int version = frameBuf.getShort(0x04) & 0xFFFF;
                int opcode = frameBuf.getShort(0x06) & 0xFFFF;
                // int flags = frameBuf.getInt(0x08); // unused for MVP
                // int headerCrc = frameBuf.getInt(0x0C); // ignored for MVP
                long requestId = frameBuf.getLong(0x10);
                int bodyLen = frameBuf.getInt(0x18);
                // int reserved = frameBuf.getInt(0x1C);

                if (magic != MAGIC_QLWP) {
                    sendError(ch, requestId, 1003, "bad magic");
                    return;
                }
                if (version != VERSION_V1) {
                    sendError(ch, requestId, 1004, "unsupported version");
                    return;
                }
                if (bodyLen != (frameLen - HEADER_SIZE)) {
                    sendError(ch, requestId, 1005, "body_len mismatch");
                    return;
                }

                // Slice body
                ByteBuffer body = frameBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                body.position(HEADER_SIZE);
                body.limit(HEADER_SIZE + bodyLen);

                // Dispatch
                switch (opcode) {
                    case OP_HELLO -> handleHello(ch, requestId, body);
                    default -> {
                        // Unknown opcode for now
                        sendError(ch, requestId, 1100, "unsupported opcode: 0x" + Integer.toHexString(opcode));
                        // Keep connection alive or close? MVP: close.
                        return;
                    }
                }
            }

        } catch (IOException e) {
            // Connection ended / IO failure
            // You can log here if you want:
            // log.debug("Connection closed: {}", e.toString());
        }
    }

    /**
     * HELLO request body:
     *   0x00 u32 max_frame_bytes
     *   0x04 u32 feature_bits
     *
     * HELLO OK response body (32 bytes):
     *   0x00 u32 accepted_max_frame_bytes
     *   0x04 u32 supported_codecs_bitset
     *   0x08 u64 node_id
     *   0x10 u64 committed_tail
     *   0x18 u32 accepted_max_chunk_bytes
     *   0x1C u32 reserved
     */
    private void handleHello(SocketChannel ch, long requestId, ByteBuffer body) throws IOException {
        if (body.remaining() < 8) {
            sendError(ch, requestId, 1200, "HELLO body too small");
            return;
        }

        int clientMaxFrameBytes = body.getInt(0x00);
        int clientFeatureBits = body.getInt(0x04);

        // Negotiate frame size
        int acceptedMaxFrameBytes = Math.min(
                clampMin(clientMaxFrameBytes, HEADER_SIZE),
                serverMaxFrameBytes
        );

        // For now we accept streaming chunk size regardless of client feature bits.
        // If you want: only advertise chunk_bytes when FEAT_APPEND_STREAM is set.
        int acceptedMaxChunkBytes = serverMaxChunkBytes;

        // committed_tail snapshot (for now 0 - hook this into your log writer/checkpointer)
        long committedTail = 0L;

        // Store negotiated max for this connection
        this.negotiatedMaxFrameBytes = acceptedMaxFrameBytes;

        // Build HELLO OK body (32 bytes)
        ByteBuffer okBody = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        okBody.putInt(0x00, acceptedMaxFrameBytes);
        okBody.putInt(0x04, supportedCodecsBitset);
        okBody.putLong(0x08, nodeId);
        okBody.putLong(0x10, committedTail);
        okBody.putInt(0x18, acceptedMaxChunkBytes);
        okBody.putInt(0x1C, 0);

        // Respond with OK (opcode=0x8000), echo request_id in header
        sendFrame(ch, OP_OK, requestId, okBody);
    }

    // ----------------- Frame send helpers -----------------

    private void sendError(SocketChannel ch, long requestId, int errorCode, String message) throws IOException {
        // Minimal ERROR body:
        //   0x00 u32 error_code
        //   0x04 u32 reserved
        //   0x08 u64 reserved
        //   0x10 u64 reserved
        //   0x18 u32 msg_len
        //   0x1C u32 reserved
        //
        // Then msg bytes (optional) - but to keep fixed-size, we omit msg bytes for now.
        //
        // Since your ERROR schema isn't finalized, keeping it simple:
        // body is 16 bytes: [error_code u32][reserved u32][reserved u64]
        ByteBuffer body = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        body.putInt(0x00, errorCode);
        body.putInt(0x04, 0);
        body.putLong(0x08, 0L);

        sendFrame(ch, OP_ERROR, requestId, body);

        // If you want to include message bytes later, expand the body schema.
        // For now, you can log it server-side:
        // log.warn("ERROR {} reqId={} msg={}", errorCode, requestId, message);
    }

    private void sendFrame(SocketChannel ch, int opcode, long requestId, ByteBuffer body) throws IOException {
        body = body.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        body.clear(); // careful: if caller set position/limit, we want full buffer contents
        // ^ If you prefer strict: remove clear() and require body.position=0,limit=filled.

        // Better approach: assume body is already fully populated and position=0.
        // So let's do it correctly:
        body = body.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        body.position(0);
        body.limit(body.capacity());

        int bodyLen = body.remaining();
        int frameLen = HEADER_SIZE + bodyLen; // bytes after length prefix

        ByteBuffer out = ByteBuffer.allocate(LEN_PREFIX_SIZE + frameLen).order(ByteOrder.LITTLE_ENDIAN);

        // length prefix
        out.putInt(frameLen);

        // header (32 bytes)
        out.putInt(MAGIC_QLWP);                 // 0x00
        out.putShort((short) VERSION_V1);       // 0x04
        out.putShort((short) opcode);           // 0x06
        out.putInt(0);                          // 0x08 flags
        out.putInt(0);                          // 0x0C header_crc32c (ignored)
        out.putLong(requestId);                 // 0x10 request_id echoed
        out.putInt(bodyLen);                    // 0x18 body_len
        out.putInt(0);                          // 0x1C reserved

        // body
        out.put(body);

        out.flip();
        writeFully(ch, out);
    }

    // ----------------- IO helpers -----------------

    /**
     * Reads until the buffer is full or EOF occurs.
     * Returns false if EOF encountered before reading anything.
     */
    private static boolean readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) {
                return total != 0;
            }
            if (n == 0) {
                // blocking SocketChannel should not usually return 0, but just in case:
                Thread.onSpinWait();
                continue;
            }
            total += n;
        }
        return true;
    }

    private static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.write(buf);
            if (n == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private static int clampMin(int value, int min) {
        if (value < min) return min;
        return value;
    }
}
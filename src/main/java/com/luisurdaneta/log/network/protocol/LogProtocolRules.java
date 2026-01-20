package com.luisurdaneta.log.network.protocol;
import java.nio.ByteBuffer;

import static com.luisurdaneta.log.network.protocol.LogConnState.*;
import static com.luisurdaneta.log.network.protocol.LogMessageTypes.*;
import static com.luisurdaneta.log.network.protocol.LogProtocolConstants.*;

public final class LogProtocolRules {
    private LogProtocolRules() {}

    // Error codes (avoid exceptions on hot path)
    public static final int OK               = 0;
    public static final int ERR_BAD_MAGIC    = -1;
    public static final int ERR_BAD_LENGTH   = -2;
    public static final int ERR_DISALLOWED   = -3;

    public static boolean isAllowed(int state, int msgType) {
        return switch (state) {
            case EXPECT_HELLO -> msgType == HELLO;
            case READY -> msgType == WRITE || msgType == READ || msgType == PING || msgType == PONG;
            case WRITE_RECV -> msgType == DATA || msgType == PING || msgType == PONG;
            case READ_SEND -> msgType == ACK || msgType == PING || msgType == PONG;
            default -> false;
        };
    }

    /**
     * Parse header bytes already in headerBuf into ctx.
     * headerBuf must be flipped and positioned at 0 with >= 16 bytes.
     */
    public static void parseHeaderInto(LogConnContext ctx, ByteBuffer headerBuf) {
        ctx.hdrMagic      = headerBuf.getInt();
        ctx.hdrType       = headerBuf.getInt();
        ctx.hdrPayloadLen = headerBuf.getLong();
    }

    /**
     * Step 0 invariants:
     * - bad magic => close immediately
     * - payload length sanity check
     */
    public static int validateHeader(LogConnContext ctx) {
        if (ctx.hdrMagic != MAGIC_QLOG) {
            return ERR_BAD_MAGIC;
        }
        if (ctx.hdrPayloadLen < 0 || ctx.hdrPayloadLen > MAX_FRAME_BYTES) {
            return ERR_BAD_LENGTH;
        }
        return OK;
    }

    /**
     * State-machine enforcement.
     */
    public static int validateAllowed(LogConnContext ctx) {
        return isAllowed(ctx.state, ctx.hdrType) ? OK : ERR_DISALLOWED;
    }
}

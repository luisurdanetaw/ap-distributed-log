package com.luisurdaneta.log.network.protocol.state;
import com.luisurdaneta.log.network.protocol.constants.LogMessageTypes;

import java.nio.ByteBuffer;

import static com.luisurdaneta.log.network.protocol.state.LogConnState.*;
import static com.luisurdaneta.log.network.protocol.constants.LogProtocolConstants.*;

public final class LogProtocolRules {
    private LogProtocolRules() {}

    public static final int OK               = 0;
    public static final int ERR_BAD_MAGIC    = -1;
    public static final int ERR_BAD_LENGTH   = -2;
    public static final int ERR_DISALLOWED   = -3;

    /** OPCODES ALLOWED PER CONNECTION STATE */
    public static boolean isAllowed(int state, int msgType) {

        if (msgType == LogMessageTypes.ERROR) return true;

        return switch (state) {
            case EXPECT_HELLO -> msgType == LogMessageTypes.HELLO;

            case READY -> msgType == LogMessageTypes.WRITE
                    || msgType == LogMessageTypes.READ
                    || msgType == LogMessageTypes.PING
                    || msgType == LogMessageTypes.PONG;

            case WRITE_RECV -> msgType == LogMessageTypes.DATA
                    || msgType == LogMessageTypes.PING
                    || msgType == LogMessageTypes.PONG;

            case READ_SEND -> msgType == LogMessageTypes.ACK
                    || msgType == LogMessageTypes.PING
                    || msgType == LogMessageTypes.PONG;

            default -> false;
        };
    }

    public static void parseHeaderInto(LogConnContext ctx, ByteBuffer headerBuf) {
        ctx.hdrMagic      = headerBuf.getInt();
        ctx.hdrType       = headerBuf.getInt();
        ctx.hdrPayloadLen = headerBuf.getLong();
    }

    public static int validateHeader(LogConnContext ctx) {
        if (ctx.hdrMagic != MAGIC_QLOG) {
            return ERR_BAD_MAGIC;
        }
        if (ctx.hdrPayloadLen < 0 || ctx.hdrPayloadLen > MAX_FRAME_BYTES) {
            return ERR_BAD_LENGTH;
        }
        return OK;
    }

    public static int validateAllowed(LogConnContext ctx) {
        return isAllowed(ctx.state, ctx.hdrType) ? OK : ERR_DISALLOWED;
    }
}

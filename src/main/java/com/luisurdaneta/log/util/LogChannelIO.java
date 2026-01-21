package com.luisurdaneta.log.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class LogChannelIO {
    private LogChannelIO() {}

    /** READ TIL BUFFER IS FULL. RETURN FALSE IF REMOTE CLOSED (-1). */
    public static boolean readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n == -1) return false;
        }
        return true;
    }

    public static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    /**
     * DRAIN (DISCARD) EXACTLY `bytes` FROM THE CH USING A SCRATCH BUFFER
     * RETURNS FALSE IF REMOTE CLOSES MID DRAIN
     */
    public static boolean drainFully(SocketChannel ch, long bytes, ByteBuffer scratch) throws IOException {
        while (bytes > 0) {
            scratch.clear();

            int chunk = (int) Math.min((long) scratch.capacity(), bytes);
            scratch.limit(chunk);

            // Read exactly chunk bytes
            while (scratch.hasRemaining()) {
                int n = ch.read(scratch);
                if (n == -1) return false;
            }

            bytes -= chunk;
        }
        return true;
    }
}

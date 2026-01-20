package com.luisurdaneta.log.network.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class LogChannelIO {
    private LogChannelIO() {}

    /** Reads until buf is full. Returns false if remote closed (-1). */
    public static boolean readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n == -1) return false;
        }
        return true;
    }

    /** Writes until buf is fully written. */
    public static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }
}

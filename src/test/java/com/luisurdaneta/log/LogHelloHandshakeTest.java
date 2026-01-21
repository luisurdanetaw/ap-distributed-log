package com.luisurdaneta.log;
/*
import com.luisurdaneta.log.network.protocol.handler.LogConnectionHandler;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.luisurdaneta.log.network.protocol.constants.LogMessageTypes.*;
import static com.luisurdaneta.log.network.protocol.constants.LogProtocolConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public final class LogHelloHandshakeTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    @Test
    void helloHappyPath_serverRepliesAck16() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (ServerSocketChannel server = ServerSocketChannel.open()) {
                server.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = ((InetSocketAddress) server.getLocalAddress()).getPort();

                ExecutorService es = Executors.newSingleThreadExecutor();
                Future<?> serverFuture = es.submit(() -> {
                    try (SocketChannel accepted = server.accept()) {
                        new LogConnectionHandler().handle(accepted);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                try (SocketChannel client = SocketChannel.open()) {
                    client.connect(new InetSocketAddress("127.0.0.1", port));

                    // ---- Client sends HELLO ----
                    writeHello(client, PROTOCOL_VERSION, /*clientCaps=*///0xAABBCCDDL);

                    // ---- Client reads ACK header ----
/*
                    ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES).order(LE);
                    readFullyWithDeadline(client, hdr, 1_000);
                    hdr.flip();

                    int magic = hdr.getInt();
                    int type  = hdr.getInt();
                    long len  = hdr.getLong();

                    assertEquals(MAGIC_QLOG, magic, "ACK magic mismatch");
                    assertEquals(ACK, type, "Expected ACK type");
                    assertEquals(16L, len, "ACK payload length must be 16");

                    // ---- Client reads ACK payload ----
                    ByteBuffer payload = ByteBuffer.allocate(16).order(LE);
                    readFullyWithDeadline(client, payload, 1_000);
                    payload.flip();

                    long version = payload.getLong();
                    long serverCaps = payload.getLong();

                    assertEquals(PROTOCOL_VERSION, version, "Server protocol version mismatch");
                    assertEquals(0L, serverCaps, "Expected server caps = 0 for now");
                } finally {
                    es.shutdownNow();
                    serverFuture.cancel(true);
                }
            }
        });
    }

    @Test
    void nonHelloFirst_serverRepliesErrorAndCloses() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (ServerSocketChannel server = ServerSocketChannel.open()) {
                server.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = ((InetSocketAddress) server.getLocalAddress()).getPort();

                ExecutorService es = Executors.newSingleThreadExecutor();
                Future<?> serverFuture = es.submit(() -> {
                    try (SocketChannel accepted = server.accept()) {
                        new LogConnectionHandler().handle(accepted);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                try (SocketChannel client = SocketChannel.open()) {
                    client.connect(new InetSocketAddress("127.0.0.1", port));

                    // Send READ first (violates HELLO-first)
                    ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + 16).order(LE);
                    frame.putInt(MAGIC_QLOG);
                    frame.putInt(READ);
                    frame.putLong(16L);
                    frame.putLong(1L); // uuid_hi dummy
                    frame.putLong(2L); // uuid_lo dummy
                    frame.flip();
                    writeFully(client, frame);

                    // Expect ERROR frame back
                    ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES).order(LE);
                    readFullyWithDeadline(client, hdr, 1_000);
                    hdr.flip();

                    int magic = hdr.getInt();
                    int type  = hdr.getInt();
                    long len  = hdr.getLong();

                    assertEquals(MAGIC_QLOG, magic);
                    assertEquals(ERROR, type);
                    assertTrue(len > 0, "ERROR should include a UTF-8 message");

                    ByteBuffer msg = ByteBuffer.allocate((int) len);
                    readFullyWithDeadline(client, msg, 1_000);
                    // not strictly required to decode string here — but you can if you want
                } finally {
                    es.shutdownNow();
                    serverFuture.cancel(true);
                }
            }
        });
    }

    @Test
    void wrongVersion_serverRepliesErrorAndCloses() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (ServerSocketChannel server = ServerSocketChannel.open()) {
                server.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = ((InetSocketAddress) server.getLocalAddress()).getPort();

                ExecutorService es = Executors.newSingleThreadExecutor();
                Future<?> serverFuture = es.submit(() -> {
                    try (SocketChannel accepted = server.accept()) {
                        new LogConnectionHandler().handle(accepted);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                try (SocketChannel client = SocketChannel.open()) {
                    client.connect(new InetSocketAddress("127.0.0.1", port));

                    // Send HELLO with wrong version
                    writeHello(client, /*version=*///999L, /*caps=*/0L);
/*
                    ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES).order(LE);
                    readFullyWithDeadline(client, hdr, 1_000);
                    hdr.flip();

                    int magic = hdr.getInt();
                    int type  = hdr.getInt();
                    long len  = hdr.getLong();

                    assertEquals(MAGIC_QLOG, magic);
                    assertEquals(ERROR, type);
                    assertTrue(len > 0, "ERROR must include message");
                } finally {
                    es.shutdownNow();
                    serverFuture.cancel(true);
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // Helpers (client-side framing + IO)
    // -------------------------------------------------------------------------

    private static void writeHello(SocketChannel ch, long version, long clientCaps) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + 16).order(LE);

        // header
        frame.putInt(MAGIC_QLOG);
        frame.putInt(HELLO);
        frame.putLong(16L);

        // payload
        frame.putLong(version);
        frame.putLong(clientCaps);

        frame.flip();
        writeFully(ch, frame);
    }

    private static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    /**
     * Blocking read-exact-N with a deadline (prevents hanging tests).
     */
/*
    private static void readFullyWithDeadline(SocketChannel ch, ByteBuffer buf, long timeoutMillis)
            throws IOException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;

        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n == -1) {
                fail("Remote closed connection early while reading");
            }
            if (n == 0) {
                if (System.nanoTime() > deadline) {
                    fail("Timed out while reading from socket");
                }
                // small spin; optional: Thread.onSpinWait() if you want
            }
        }
    }
}*/
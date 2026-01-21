package com.luisurdaneta.log;

public class LogConnectionHandlerIT {
/*
    @Test
    void hello_write_1mb_data_then_read_validate_acks() {
        assertTimeoutPreemptively(Duration.ofSeconds(6), () -> {
            final LogConnectionHandler handler = new LogConnectionHandler();

            try (ServerSocketChannel server = ServerSocketChannel.open()) {
                server.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = ((InetSocketAddress) server.getLocalAddress()).getPort();

                ExecutorService exec = Executors.newSingleThreadExecutor();
                Future<?> serverFuture = exec.submit(() -> {
                    try (SocketChannel accepted = server.accept()) {
                        accepted.configureBlocking(true);
                        handler.handle(accepted);
                    } catch (IOException e) {
                        // server thread ends here
                    }
                });

                // ---- Client ----
                try (SocketChannel client = SocketChannel.open()) {
                    client.configureBlocking(true);
                    client.connect(new InetSocketAddress("127.0.0.1", port));

                    // Reusable client buffers
                    ByteBuffer header = ByteBuffer.allocateDirect(HEADER_BYTES).order(WIRE_ORDER);
                    ByteBuffer payload16 = ByteBuffer.allocateDirect(16).order(WIRE_ORDER);
                    ByteBuffer payload24 = ByteBuffer.allocateDirect(24).order(WIRE_ORDER);
                    ByteBuffer payload8  = ByteBuffer.allocateDirect(8).order(WIRE_ORDER);

                    // ================
                    // 1) HELLO
                    // ================
                    long clientCaps = 0x1234;
                    payload16.clear();
                    payload16.putLong(PROTOCOL_VERSION);
                    payload16.putLong(clientCaps);
                    payload16.flip();

                    writeFrame(client, header, HELLO, payload16);

                    Frame ackHello = readFrame(client, header);
                    assertEquals(ACK, ackHello.type);
                    assertEquals(16, ackHello.payloadLen);

                    ByteBuffer ackHelloPayload = ByteBuffer.wrap(ackHello.payload).order(ByteOrder.LITTLE_ENDIAN);
                    long serverVersion = ackHelloPayload.getLong();
                    long serverCaps    = ackHelloPayload.getLong();
                    assertEquals(PROTOCOL_VERSION, serverVersion);
                    assertEquals(0L, serverCaps); // matches your SERVER_CAPABILITIES

                    // ================
                    // 2) WRITE(uuid, totalBytes=1MB)
                    // ================
                    long uuidHi = 0xAAAABBBBCCCCDDDDL;
                    long uuidLo = 0x1111222233334444L;
                    int oneMb = 1024 * 1024;

                    payload24.clear();
                    payload24.putLong(uuidHi);
                    payload24.putLong(uuidLo);
                    payload24.putLong(oneMb);
                    payload24.flip();

                    writeFrame(client, header, WRITE, payload24);

                    Frame ackWriteStart = readFrame(client, header);
                    assertEquals(ACK, ackWriteStart.type);
                    assertEquals(0, ackWriteStart.payloadLen);

                    // ================
                    // 3) DATA(1MB)
                    // ================
                    // generate deterministic-ish data
                    byte[] data = new byte[oneMb];
                    new Random(42).nextBytes(data);

                    writeFrame(client, header, DATA, ByteBuffer.wrap(data));

                    Frame ackWriteDone = readFrame(client, header);
                    assertEquals(ACK, ackWriteDone.type);
                    assertEquals(8, ackWriteDone.payloadLen);

                    ByteBuffer ackWriteDonePayload = ByteBuffer.wrap(ackWriteDone.payload).order(ByteOrder.LITTLE_ENDIAN);
                    long bytesWritten = ackWriteDonePayload.getLong();
                    assertEquals(oneMb, bytesWritten);

                    // ================
                    // 4) READ(uuid)
                    // ================
                    payload16.clear();
                    payload16.putLong(uuidHi);
                    payload16.putLong(uuidLo);
                    payload16.flip();

                    writeFrame(client, header, READ, payload16);

                    Frame ackRead = readFrame(client, header);
                    assertEquals(ACK, ackRead.type);
                    assertEquals(8, ackRead.payloadLen);

                    long totalSegmentBytes = ByteBuffer.wrap(ackRead.payload).order(ByteOrder.LITTLE_ENDIAN).getLong();

                    // IMPORTANT:
                    // Your current handler always simulates empty segments on READ (totalBytes=0).
                    assertEquals(0L, totalSegmentBytes);

                    // Next server frame should be DATA(length=0)
                    Frame dataFrame = readFrame(client, header);
                    assertEquals(DATA, dataFrame.type);
                    assertEquals(0, dataFrame.payloadLen);
                    assertEquals(0, dataFrame.payload.length);

                    // Client final ACK(length=0)
                    writeFrame(client, header, ACK, ByteBuffer.allocate(0));

                    // Close client to end server loop cleanly
                }

                // Shut down server thread
                exec.shutdownNow();
                try {
                    serverFuture.get(1, TimeUnit.SECONDS);
                } catch (TimeoutException ignored) {
                    // ok
                } finally {
                    exec.shutdownNow();
                }
            }
        });
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static void writeFrame(SocketChannel ch, ByteBuffer headerBuf, int type, ByteBuffer payload) throws IOException {
        long payloadLen = payload.remaining();

        // header: MAGIC + type + payloadLen
        headerBuf.clear();
        headerBuf.putInt(MAGIC_QLOG);
        headerBuf.putInt(type);
        headerBuf.putLong(payloadLen);
        headerBuf.flip();

        writeFully(ch, headerBuf);
        writeFully(ch, payload);
    }

    private static Frame readFrame(SocketChannel ch, ByteBuffer headerBuf) throws IOException {
        headerBuf.clear();
        readFully(ch, headerBuf);
        headerBuf.flip();

        int magic = headerBuf.getInt();
        int type = headerBuf.getInt();
        long payloadLen = headerBuf.getLong();

        assertEquals(MAGIC_QLOG, magic, "bad magic from server");

        if (payloadLen < 0 || payloadLen > Integer.MAX_VALUE) {
            fail("payloadLen insane: " + payloadLen);
        }

        byte[] payload = new byte[(int) payloadLen];
        if (payloadLen > 0) {
            readFully(ch, ByteBuffer.wrap(payload));
        }

        return new Frame(type, payloadLen, payload);
    }

    private static void readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n == -1) throw new IOException("remote closed");
        }
    }

    private static void writeFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private record Frame(int type, long payloadLen, byte[] payload) {}*/
}

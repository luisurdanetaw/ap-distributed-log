package com.luisurdaneta.log.network.server;

import com.luisurdaneta.log.memory.segment.LogSegmentPool;
import com.luisurdaneta.log.network.protocol.handler.ConnectionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public final class Server implements AutoCloseable {
    private final int port;

    private final ConnectionHandler handler;
    private final LogSegmentPool segmentPool;
    private final ExecutorService executor;

    private ServerSocketChannel server;

    public Server(int port,
                  Path segmentDir,
                  int maxSegments,
                  long initialCapacity,
                  ConnectionHandler handler) throws IOException {
        this.port = port;
        this.handler = handler;
        this.segmentPool = new LogSegmentPool(segmentDir, maxSegments, initialCapacity);
        this.executor = Executors.newFixedThreadPool(maxSegments);
    }

    public void start() throws IOException {
        server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress("0.0.0.0", port));
        server.configureBlocking(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { close(); } catch (Exception ignored) {}
        }));

        System.out.println("Server listening on port " + port);

        for(;;) {
            final SocketChannel client;
            try {
                client = server.accept(); // blocks
            } catch (AsynchronousCloseException e) {
                break; // shutdown
            }

            LogSegmentPool.Lease lease = segmentPool.tryAcquire();
            if (lease == null) {
                client.close(); // no free segment -> reject
                continue;
            }

            executor.execute(() -> {
                try (client; lease) {
                    handler.handle(client, lease.segment());
                } catch (IOException ignored) {
                    // client reset/disconnect/etc
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        if (server != null) server.close();
        executor.shutdown();
    }
}

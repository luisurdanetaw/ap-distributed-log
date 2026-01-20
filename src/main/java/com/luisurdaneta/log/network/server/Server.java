package com.luisurdaneta.log.network.server;

import com.luisurdaneta.log.network.protocol.ConnectionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Server implements AutoCloseable {
    private final int port;
    private final ConnectionLimiter limiter;
    private final ConnectionHandler handler;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private ServerSocketChannel server;

    public Server(int port, int maxConnections, ConnectionHandler handler) {
        this.port = port;
        this.limiter = new ConnectionLimiter(maxConnections);
        this.handler = handler;
    }

    public void start() throws IOException {
        server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress("0.0.0.0", port));
        server.configureBlocking(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { close(); } catch (Exception ignored) {}
        }));

        System.out.println("Server listening on port " + port);

        while (!Thread.currentThread().isInterrupted()) {
            SocketChannel client;
            try {
                client = server.accept(); // blocks
            } catch (AsynchronousCloseException e) {
                break; // shutdown
            }

            if (!limiter.tryAcquire()) {
                client.close();
                continue;
            }

            executor.execute(() -> {
                try (client) {
                    handler.handle(client);
                } catch (IOException ignored) {
                    // client reset/disconnect/etc
                } finally {
                    limiter.release();
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

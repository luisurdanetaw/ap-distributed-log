package com.luisurdaneta.log;

import com.luisurdaneta.log.network.protocol.handler.ConnectionHandler;
import com.luisurdaneta.log.network.protocol.handler.LogConnectionHandler;
import com.luisurdaneta.log.network.server.Server;

import java.nio.file.Path;

public final class Node {
    public static void main(String[] args) {
        int port = 8080;
        long initialCapacity = 1024L * 1024L; // 1 MiB default
        Path persistencePath = Path.of("/data/log");

        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            persistencePath = Path.of(args[1]);
        }
        if (args.length >= 3) {
            initialCapacity = parseSizeBytes(args[2]);
        }

        int numCpu = Runtime.getRuntime().availableProcessors();
        ConnectionHandler handler = new LogConnectionHandler();

        try (Server server = new Server(port, persistencePath, numCpu, initialCapacity, handler)) {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long parseSizeBytes(String s) {
        String x = s.trim().toLowerCase();

        long multiplier = 1;
        if (x.endsWith("k")) {
            multiplier = 1024L;
            x = x.substring(0, x.length() - 1);
        } else if (x.endsWith("m")) {
            multiplier = 1024L * 1024L;
            x = x.substring(0, x.length() - 1);
        } else if (x.endsWith("g")) {
            multiplier = 1024L * 1024L * 1024L;
            x = x.substring(0, x.length() - 1);
        }

        long base = Long.parseLong(x);
        if (base <= 0) throw new IllegalArgumentException("initialCapacity must be > 0");
        return base * multiplier;
    }
}
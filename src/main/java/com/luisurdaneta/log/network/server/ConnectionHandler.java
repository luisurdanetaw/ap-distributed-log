package com.luisurdaneta.log.network.server;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface ConnectionHandler {
    void handle(SocketChannel ch) throws IOException;
}

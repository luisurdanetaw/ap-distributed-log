package com.luisurdaneta.log.network.protocol.handler;

import com.luisurdaneta.log.memory.segment.LogSegment;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface ConnectionHandler {
    void handle(SocketChannel ch, LogSegment segment) throws IOException;
}

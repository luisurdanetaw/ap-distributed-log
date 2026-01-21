package com.luisurdaneta.log.network.protocol.constants;

public final class LogMessageTypes {
    private LogMessageTypes() {}

    public static final int HELLO = 1;
    public static final int ACK   = 2;
    public static final int WRITE = 3;
    public static final int READ  = 4;
    public static final int DATA  = 5;
    public static final int ERROR = 6;
    public static final int PING  = 7;
    public static final int PONG  = 8;

    public static String nameOf(int t) {
        return switch (t) {
            case HELLO -> "HELLO";
            case ACK   -> "ACK";
            case WRITE -> "WRITE";
            case READ  -> "READ";
            case DATA  -> "DATA";
            case ERROR -> "ERROR";
            case PING  -> "PING";
            case PONG  -> "PONG";
            default -> "UNKNOWN(" + t + ")";
        };
    }
}

package com.luisurdaneta.log.network.protocol.state;

public final class LogConnState {
    private LogConnState() {}

    public static final int EXPECT_HELLO = 0;
    public static final int READY        = 1;
    public static final int WRITE_RECV   = 2;
    public static final int READ_SEND    = 3;
    public static final int CLOSING      = 4;

    public static String nameOf(int s) {
        return switch (s) {
            case EXPECT_HELLO -> "EXPECT_HELLO";
            case READY        -> "READY";
            case WRITE_RECV   -> "WRITE_RECV";
            case READ_SEND    -> "READ_SEND";
            case CLOSING      -> "CLOSING";
            default -> "UNKNOWN(" + s + ")";
        };
    }
}
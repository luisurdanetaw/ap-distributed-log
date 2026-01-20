package com.luisurdaneta.log.network.protocol;

import java.nio.ByteOrder;
import java.util.EnumSet;

public final class LogProtocolConstants {

    public static final int MAGIC_QLOG = 0x514C4F47;
    public static final int HEADER_BYTES = 16; // Fixed header bytes: magic(u32) + type(u32) + payload_len(u64)
    public static final long PROTOCOL_VERSION = 1L;
    static final long MAX_FRAME_BYTES = 256L * 1024 * 1024;
    static final ByteOrder WIRE_ORDER = ByteOrder.LITTLE_ENDIAN;
}

package com.luisurdaneta.log.network.protocol;

public final class LogConnContext {

    public int state = LogConnState.EXPECT_HELLO;

    // Negotiated from HELLO
    public long negotiatedPeerVersion = 0;
    public long negotiatedPeerCaps = 0;

    // Current op metadata
    public long currentUuidHi = 0;
    public long currentUuidLo = 0;
    public long expectedSegmentBytes = 0;
    public long bytesTransferred = 0;

    // Parsed header fields (no FrameHeader allocation)
    public int  hdrMagic;
    public int  hdrType;
    public long hdrPayloadLen;

    public void beginOp(long uuidHi, long uuidLo, long expectedBytes) {
        this.currentUuidHi = uuidHi;
        this.currentUuidLo = uuidLo;
        this.expectedSegmentBytes = expectedBytes;
        this.bytesTransferred = 0;
    }

    public void clearOp() {
        this.currentUuidHi = 0;
        this.currentUuidLo = 0;
        this.expectedSegmentBytes = 0;
        this.bytesTransferred = 0;
    }
}


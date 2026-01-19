package com.luisurdaneta.log.network.server;

import java.util.concurrent.atomic.AtomicInteger;

public final class ConnectionLimiter {
    private final int max;
    private final AtomicInteger active = new AtomicInteger(0);

    public ConnectionLimiter(int max) {
        this.max = max;
    }

    public boolean tryAcquire() {
        int n = active.incrementAndGet(); // CAS under the hood
        if (n > max) {
            active.decrementAndGet();
            return false;
        }
        return true;
    }

    public void release() {
        active.decrementAndGet();
    }

    public int active() {
        return active.get();
    }
}
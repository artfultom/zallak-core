package my.artfultom.zallak.config;

import java.util.concurrent.TimeUnit;

public class Timeout {

    private final long timeout;
    private final TimeUnit unit;

    private Timeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    public static Timeout of(long timeout, TimeUnit unit) {
        // TODO null
        return new Timeout(timeout, unit);
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getUnit() {
        return unit;
    }
}

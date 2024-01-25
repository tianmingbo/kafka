package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.function.Supplier;

/**
 * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
 * of this class.
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        synchronized (obj) {
            while (true) {
                if (condition.get())
                    return;

                long currentTimeMs = milliseconds();
                if (currentTimeMs >= deadlineMs)
                    throw new TimeoutException("Condition not satisfied before deadline");

                obj.wait(deadlineMs - currentTimeMs);
            }
        }
    }

}

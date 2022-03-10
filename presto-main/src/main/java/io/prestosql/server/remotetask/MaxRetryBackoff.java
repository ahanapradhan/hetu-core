package io.prestosql.server.remotetask;

import io.airlift.units.Duration;
import io.prestosql.spi.failuredetector.IBackoff;

import java.util.concurrent.TimeUnit;

public class MaxRetryBackoff extends Backoff implements IBackoff {
    private static final int MAX_RETRIES = 10;
    private final int maxTries;
    MaxRetryBackoff(Duration maxFailureInterval, int maxTries) {
        super(maxFailureInterval);
        this.maxTries = (minTries < maxTries) ? maxTries : minTries;
    }

    public MaxRetryBackoff(int maxTries) {
       this(new Duration(5, TimeUnit.MINUTES), maxTries);
    }

    public MaxRetryBackoff() {
        this(new Duration(5, TimeUnit.MINUTES), MAX_RETRIES);
    }

    /**
     * @return true if max retry failed, now it is time to check node status from HeartbeatFailureDetector
     */

    public synchronized boolean maxRetryDone()
    {
        long now = ticker.read();

        lastFailureTime = now;
        updateFailureCount();
        if (lastRequestStart != 0) {
            failureRequestTimeTotal += now - lastRequestStart;
            lastRequestStart = 0;
        }

        if (firstFailureTime == 0) {
            firstFailureTime = now;
            // can not fail on first failure
            return false;
        }

        if (getFailureCount() < minTries) {
            return false;
        }
        return getFailureCount() >= maxTries;
    }

    /**
     * @return true if maxErrorDuration is passed. Does not matter how many retry has happened.
     */
    public synchronized boolean timeout()
    {
        long now = ticker.read();
        long failureDuration = now - firstFailureTime;
        return failureDuration >= maxFailureIntervalNanos;
    }
}

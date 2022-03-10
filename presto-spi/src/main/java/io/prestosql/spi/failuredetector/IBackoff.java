package io.prestosql.spi.failuredetector;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public interface IBackoff
{
    int MIN_RETRIES = 3;

    List<Duration> DEFAULT_BACKOFF_DELAY_INTERVALS = ImmutableList.<Duration>builder()
            .add(new Duration(0, MILLISECONDS))
            .add(new Duration(50, MILLISECONDS))
            .add(new Duration(100, MILLISECONDS))
            .add(new Duration(200, MILLISECONDS))
            .add(new Duration(500, MILLISECONDS))
            .build();


    boolean failure();

    void startRequest();

    long getBackoffDelayNanos();

    void success();

    long getFailureCount();

    Duration getFailureDuration();

    Duration getFailureRequestTimeTotal();
}

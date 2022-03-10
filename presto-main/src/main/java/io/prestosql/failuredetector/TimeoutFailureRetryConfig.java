package io.prestosql.failuredetector;

import io.airlift.units.Duration;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TimeoutFailureRetryConfig {

    private final String maxTimeoutDuration;

    public TimeoutFailureRetryConfig(Properties properties) {
            this.maxTimeoutDuration = properties.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION);
        }
    public Duration getMaxTimeoutDuration(){
        if (maxTimeoutDuration == null) {
            return Duration.valueOf(FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION);
        }
        Duration d = Duration.valueOf(maxTimeoutDuration);
        return d;
    }
}

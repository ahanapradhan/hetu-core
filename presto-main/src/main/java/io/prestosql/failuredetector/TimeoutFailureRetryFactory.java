package io.prestosql.failuredetector;

import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class TimeoutFailureRetryFactory extends FailureRetryPolicyFactory {
    @Override
    public AbstractFailureRetryPolicy getFailureRetryPolicy(Properties properties) {
        return new TimeoutFailureRetryPolicy(new TimeoutFailureRetryConfig(properties));
    }

    @Override
    public String getName() {
        return FailureRetryPolicy.TIMEOUT;
    }
}

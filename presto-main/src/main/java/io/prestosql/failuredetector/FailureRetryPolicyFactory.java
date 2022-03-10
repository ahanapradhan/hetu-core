package io.prestosql.failuredetector;

import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.failuredetector.FailureRetryFactory;

import java.util.Properties;

public abstract class FailureRetryPolicyFactory implements FailureRetryFactory {
    public abstract AbstractFailureRetryPolicy getFailureRetryPolicy(Properties properties);
}

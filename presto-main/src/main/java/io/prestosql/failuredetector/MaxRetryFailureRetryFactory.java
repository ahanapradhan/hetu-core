package io.prestosql.failuredetector;

import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import java.util.Properties;

public class MaxRetryFailureRetryFactory extends FailureRetryPolicyFactory {

    @Override
    public AbstractFailureRetryPolicy getFailureRetryPolicy(Properties properties) {
        return new MaxRetryFailureRetryPolicy(new MaxRetryFailureRetryConfig(properties));
    }

    @Override
    public String getName() {
        return FailureRetryPolicy.MAXRETRY;
    }
}

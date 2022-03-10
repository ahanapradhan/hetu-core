package io.prestosql.failuredetector;

import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.HostAddress;

public class TimeoutFailureRetryPolicy
                extends AbstractFailureRetryPolicy
{

    private TimeoutFailureRetryConfig config;

    public TimeoutFailureRetryPolicy(TimeoutFailureRetryConfig config)
    {
        super(new Backoff(config.getMaxTimeoutDuration()));
        this.config = config;
    }

    @Override
    public boolean hasFailed(HostAddress address)
    {
        return getBackoff().failure();
    }
}

package io.prestosql.failuredetector;

import io.prestosql.server.remotetask.MaxRetryBackoff;
import io.prestosql.spi.HostAddress;

public class MaxRetryFailureRetryPolicy
                extends AbstractFailureRetryPolicy
{
    private MaxRetryFailureRetryConfig config;

    public MaxRetryFailureRetryPolicy(MaxRetryFailureRetryConfig config)
    {
        super(new MaxRetryBackoff(config.getMaxRetryCount()));
        this.config = config;
    }

    @Override
    public boolean hasFailed(HostAddress address)
    {
        FailureDetector failureDetector = FailureDetectorManager.getDefaultFailureDetector();
        FailureDetector.State remoteHostState = failureDetector.getState(address);
        MaxRetryBackoff backoff = (MaxRetryBackoff) getBackoff();
        return (backoff.maxRetryDone() &&
                (FailureDetector.State.GONE.equals(remoteHostState)
                        || FailureDetector.State.UNRESPONSIVE.equals(remoteHostState)
                        || backoff.timeout()));
    }

}

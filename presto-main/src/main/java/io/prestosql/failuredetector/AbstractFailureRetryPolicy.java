package io.prestosql.failuredetector;

import io.prestosql.spi.HostAddress;
import io.prestosql.spi.failuredetector.IBackoff;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

public abstract class AbstractFailureRetryPolicy
                implements FailureRetryPolicy
{
    private IBackoff backoff;

    public AbstractFailureRetryPolicy(IBackoff backoff)
    {
        this.backoff = backoff;
    }

    public IBackoff getBackoff()
    {
        return this.backoff;
    }

    public abstract boolean hasFailed(HostAddress address);
}

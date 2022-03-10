package io.prestosql.spi.failuredetector;

import io.prestosql.spi.HostAddress;

public interface FailureRetryPolicy {

    IBackoff getBackoff();

    boolean hasFailed(HostAddress address);

    String FD_RETRY_TYPE = "fd.retry.type";
    String FD_RETRY_PROFILE = "fd.retry.profile";
    String MAX_RETRY_COUNT = "max.retry.count";
    String MAX_TIMEOUT_DURATION = "max.error.duration";

    int DEFAULT_RETRY_COUNT = 10;
    String DEFAULT_TIMEOUT_DURATION = "300s";

    // FailureRetryPolicy type names, to be used in profile parameter fd.retry.type
    String TIMEOUT = "timeout";
    String MAXRETRY = "max-retry";
}

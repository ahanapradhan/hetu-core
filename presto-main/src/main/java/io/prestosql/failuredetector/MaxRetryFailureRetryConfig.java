package io.prestosql.failuredetector;

import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import java.util.Properties;

public class MaxRetryFailureRetryConfig {

    private String maxRetryCount;
    public MaxRetryFailureRetryConfig(Properties properties) {
        this.maxRetryCount = properties.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT);
    }
    public int getMaxRetryCount() {
        if (maxRetryCount == null) {
            return FailureRetryPolicy.DEFAULT_RETRY_COUNT;
        }
        int count = Integer.parseInt(maxRetryCount);
        return count;
    }
}

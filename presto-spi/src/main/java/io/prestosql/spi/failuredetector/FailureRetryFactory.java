package io.prestosql.spi.failuredetector;

import java.util.Properties;

public interface FailureRetryFactory {
    FailureRetryPolicy getFailureRetryPolicy(Properties properties);
    String getName();
}

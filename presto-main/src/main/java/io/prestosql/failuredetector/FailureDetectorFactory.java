package io.prestosql.failuredetector;

public interface FailureDetectorFactory {
    FailureDetector createFailureDetector();
    String getName();
}

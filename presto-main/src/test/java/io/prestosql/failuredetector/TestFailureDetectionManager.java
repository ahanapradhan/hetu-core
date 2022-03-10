package io.prestosql.failuredetector;

import org.testng.annotations.Test;

public class TestFailureDetectionManager {
    @Test
    public void testDefaultRetryProfile(){
        FailureDetectorManager fm = new FailureDetectorManager();
    }
}

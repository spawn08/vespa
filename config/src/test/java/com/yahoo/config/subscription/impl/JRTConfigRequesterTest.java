// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.config.subscription.impl;

import com.yahoo.config.subscription.ConfigSourceSet;
import com.yahoo.config.subscription.ConfigSubscriber;
import com.yahoo.foo.SimpletypesConfig;
import com.yahoo.jrt.Request;
import com.yahoo.vespa.config.ConfigKey;
import com.yahoo.vespa.config.ConnectionPool;
import com.yahoo.vespa.config.ErrorCode;
import com.yahoo.vespa.config.ErrorType;
import com.yahoo.vespa.config.TimingValues;
import com.yahoo.vespa.config.protocol.JRTServerConfigRequestV3;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.yahoo.config.subscription.impl.JRTConfigRequester.calculateFailedRequestDelay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author hmusum
 */
public class JRTConfigRequesterTest {

    @Test
    public void testDelayCalculation() {
        TimingValues defaultTimingValues = new TimingValues();
        Random random = new Random(0); // Use seed to make tests predictable
        TimingValues timingValues = new TimingValues(defaultTimingValues, random);

        // transientFailures and fatalFailures are not set until after delay has been calculated,
        // so false is the case for the first failure
        boolean fatalFailures = false;
        boolean configured = false;

        // First time failure, not configured
        long delay = calculateFailedRequestDelay(ErrorType.TRANSIENT, fatalFailures, timingValues, configured);
        assertTransientDelay(timingValues.getUnconfiguredDelay(), delay);
        delay = calculateFailedRequestDelay(ErrorType.TRANSIENT, fatalFailures, timingValues, configured);
        assertTransientDelay(timingValues.getUnconfiguredDelay(), delay);


        delay = calculateFailedRequestDelay(ErrorType.FATAL, fatalFailures, timingValues, configured);
        assertTrue("delay=" + delay, delay > (1 - JRTConfigRequester.randomFraction) * timingValues.getFixedDelay());
        assertTrue("delay=" + delay,delay < (1 + JRTConfigRequester.randomFraction) * timingValues.getFixedDelay());
        assertEquals(4481, delay);

        // First time failure, configured
        configured = true;
        delay = calculateFailedRequestDelay(ErrorType.TRANSIENT, fatalFailures, timingValues, configured);
        assertTransientDelay(timingValues.getConfiguredErrorDelay(), delay);

        delay = calculateFailedRequestDelay(ErrorType.FATAL, fatalFailures, timingValues, configured);
        assertTrue(delay > (1 - JRTConfigRequester.randomFraction) * timingValues.getFixedDelay());
        assertTrue(delay < (1 + JRTConfigRequester.randomFraction) * timingValues.getFixedDelay());
        assertEquals(5275, delay);


        // nth time failure, not configured
        fatalFailures = true;
        configured = false;
        delay = calculateFailedRequestDelay(ErrorType.TRANSIENT, fatalFailures, timingValues, configured);
        assertTransientDelay(timingValues.getUnconfiguredDelay(), delay);
        delay = calculateFailedRequestDelay(ErrorType.FATAL, fatalFailures, timingValues, configured);
        final long l = timingValues.getFixedDelay() + timingValues.getUnconfiguredDelay();
        assertTrue(delay > (1 - JRTConfigRequester.randomFraction) * l);
        assertTrue(delay < (1 + JRTConfigRequester.randomFraction) * l);
        assertEquals(6121, delay);


        // nth time failure, configured
        fatalFailures = true;
        configured = true;
        delay = calculateFailedRequestDelay(ErrorType.TRANSIENT, fatalFailures, timingValues, configured);
        assertTransientDelay(timingValues.getConfiguredErrorDelay(), delay);
        delay = calculateFailedRequestDelay(ErrorType.FATAL, fatalFailures, timingValues, configured);
        final long l1 = timingValues.getFixedDelay() + timingValues.getConfiguredErrorDelay();
        assertTrue(delay > (1 - JRTConfigRequester.randomFraction) * l1);
        assertTrue(delay < (1 + JRTConfigRequester.randomFraction) * l1);
        assertEquals(20780, delay);
    }

    @Test
    public void testErrorTypes() {
        List<Integer> transientErrors = Arrays.asList(com.yahoo.jrt.ErrorCode.CONNECTION, com.yahoo.jrt.ErrorCode.TIMEOUT);
        List<Integer> fatalErrors = Arrays.asList(ErrorCode.UNKNOWN_CONFIG, ErrorCode.UNKNOWN_DEFINITION, ErrorCode.OUTDATED_CONFIG,
                ErrorCode.UNKNOWN_DEF_MD5, ErrorCode.ILLEGAL_NAME, ErrorCode.ILLEGAL_VERSION, ErrorCode.ILLEGAL_CONFIGID,
                ErrorCode.ILLEGAL_DEF_MD5, ErrorCode.ILLEGAL_CONFIG_MD5, ErrorCode.ILLEGAL_TIMEOUT, ErrorCode.INTERNAL_ERROR,
                9999); // unknown should also be fatal
        for (Integer i : transientErrors) {
            assertEquals(ErrorType.TRANSIENT, ErrorType.getErrorType(i));
        }
        for (Integer i : fatalErrors) {
            assertEquals(ErrorType.FATAL, ErrorType.getErrorType(i));
        }
    }

    @Test
    public void testFirstRequestAfterSubscribing() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);

        final MockConnection connection = new MockConnection();
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        assertEquals(requester.getConnectionPool(), connection);
        requester.request(sub);
        final Request request = connection.getRequest();
        assertNotNull(request);
        assertEquals(1, connection.getNumberOfRequests());
        JRTServerConfigRequestV3 receivedRequest = JRTServerConfigRequestV3.createFromRequest(request);
        assertTrue(receivedRequest.validateParameters());
        assertEquals(timingValues.getSubscribeTimeout(), receivedRequest.getTimeout());
        assertFalse(requester.getFatalFailures());
    }

    @Test
    public void testFatalError() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();

        final MockConnection connection = new MockConnection(new ErrorResponseHandler());
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(createSubscription(subscriber, timingValues));
        waitUntilResponse(connection);
        assertTrue(requester.getFatalFailures());
    }

    @Test
    public void testFatalErrorSubscribed() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);
        sub.setConfig(1L, false, config(), PayloadChecksum.empty());

        final MockConnection connection = new MockConnection(new ErrorResponseHandler());
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(sub);
        waitUntilResponse(connection);
        assertTrue(requester.getFatalFailures());
    }

    @Test
    public void testTransientError() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();

        final MockConnection connection = new MockConnection(new ErrorResponseHandler(com.yahoo.jrt.ErrorCode.TIMEOUT));
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(createSubscription(subscriber, timingValues));
        waitUntilResponse(connection);
        assertFalse(requester.getFatalFailures());
    }

    @Test
    public void testTransientErrorSubscribed() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);
        sub.setConfig(1L, false, config(), PayloadChecksum.empty());

        final MockConnection connection = new MockConnection(new ErrorResponseHandler(com.yahoo.jrt.ErrorCode.TIMEOUT));
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(sub);
        waitUntilResponse(connection);
        assertFalse(requester.getFatalFailures());
    }

    @Test
    public void testUnknownConfigDefinitionError() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);
        sub.setConfig(1L, false, config(), PayloadChecksum.empty());

        final MockConnection connection = new MockConnection(new ErrorResponseHandler(ErrorCode.UNKNOWN_DEFINITION));
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        assertEquals(requester.getConnectionPool(), connection);
        requester.request(sub);
        waitUntilResponse(connection);
        assertTrue(requester.getFatalFailures());
    }

    @Test
    public void testClosedSubscription() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);
        sub.close();

        final MockConnection connection = new MockConnection(new MockConnection.OKResponseHandler());
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(sub);
        assertEquals(1, connection.getNumberOfRequests());
        // Check that no further request was sent?
        try {
            Thread.sleep(timingValues.getFixedDelay()*2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(1, connection.getNumberOfRequests());
    }

    @Test
    public void testTimeout() {
        ConfigSubscriber subscriber = new ConfigSubscriber();
        final TimingValues timingValues = getTestTimingValues();
        JRTConfigSubscription<SimpletypesConfig> sub = createSubscription(subscriber, timingValues);
        sub.close();

        final MockConnection connection = new MockConnection(
                new DelayedResponseHandler(timingValues.getSubscribeTimeout()),
                2); // fake that we have more than one source
        JRTConfigRequester requester = new JRTConfigRequester(connection, timingValues);
        requester.request(createSubscription(subscriber, timingValues));
        // Check that no further request was sent?
        try {
            Thread.sleep(timingValues.getFixedDelay()*2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private JRTConfigSubscription<SimpletypesConfig> createSubscription(ConfigSubscriber subscriber, TimingValues timingValues) {
        return new JRTConfigSubscription<>(
                new ConfigKey<>(SimpletypesConfig.class, "testid"), subscriber, null, timingValues);
    }

    private SimpletypesConfig config() {
        SimpletypesConfig.Builder builder = new SimpletypesConfig.Builder();
        return new SimpletypesConfig(builder);
    }

    private void waitUntilResponse(MockConnection connection) {
        int i = 0;
        while (i < 1000 && connection.getRequest() == null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
    }

    public static TimingValues getTestTimingValues() { return new TimingValues(
            1000,  // successTimeout
            500,   // errorTimeout
            500,   // initialTimeout
            2000,  // subscribeTimeout
            250,   // unconfiguredDelay
            500,   // configuredErrorDelay
            250);   // fixedDelay
    }

    private static class ErrorResponseHandler extends MockConnection.OKResponseHandler {
        private final int errorCode;

        public ErrorResponseHandler() {
            this(ErrorCode.INTERNAL_ERROR);
        }

        public ErrorResponseHandler(int errorCode) {
            this.errorCode = errorCode;
        }

        @Override
        public void run() {
            System.out.println("Running error response handler");
            request().setError(errorCode, "error");
            requestWaiter().handleRequestDone(request());
        }
    }

    private static class DelayedResponseHandler extends MockConnection.OKResponseHandler {
        private final long waitTimeMilliSeconds;

        public DelayedResponseHandler(long waitTimeMilliSeconds) {
            this.waitTimeMilliSeconds = waitTimeMilliSeconds;
        }

        @Override
        public void run() {
            System.out.println("Running delayed response handler (waiting " + waitTimeMilliSeconds +
            ") before responding");
            try {
                Thread.sleep(waitTimeMilliSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            request().setError(com.yahoo.jrt.ErrorCode.TIMEOUT, "error");
            requestWaiter().handleRequestDone(request());
        }
    }

    @Test
    public void testManagedPool() {
        ConfigSourceSet sourceSet = ConfigSourceSet.createDefault();
        TimingValues timingValues = new TimingValues();
        JRTConfigRequester requester1 = JRTConfigRequester.create(sourceSet, timingValues);
        JRTConfigRequester requester2 = JRTConfigRequester.create(sourceSet, timingValues);
        assertNotSame(requester1, requester2);
        assertSame(requester1.getConnectionPool(), requester2.getConnectionPool());
        ConnectionPool firstPool = requester1.getConnectionPool();
        requester1.close();
        requester2.close();
        requester1 = JRTConfigRequester.create(sourceSet, timingValues);
        assertNotSame(firstPool, requester1.getConnectionPool());
        requester2 = JRTConfigRequester.create(new ConfigSourceSet("test-managed-pool-2"), timingValues);
        assertNotSame(requester1.getConnectionPool(), requester2.getConnectionPool());
        requester1.close();
        requester2.close();
    }

    private void assertTransientDelay(long maxDelay, long delay) {
        long minDelay = 0;
        assertTrue("delay=" + delay + ", minDelay=" + minDelay + ",maxDelay=" + maxDelay,
                   delay >= minDelay && delay <= maxDelay);
    }

}

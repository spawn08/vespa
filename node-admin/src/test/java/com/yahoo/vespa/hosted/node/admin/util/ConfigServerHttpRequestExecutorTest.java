// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.collections.ArraySet;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Basic testing of retry logic.
 *
 * @author dybis
 */
public class ConfigServerHttpRequestExecutorTest {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TestPojo {
        @JsonProperty("foo")
        public String foo;
        @JsonProperty("error-code")
        public Integer errorCode;
    }

    private final StringBuilder mockLog = new StringBuilder();
    private int mockReturnCode = 200;

    private CloseableHttpClient createClientMock() throws IOException {
        CloseableHttpClient httpMock = mock(CloseableHttpClient.class);
        when(httpMock.execute(any())).thenAnswer(invocationOnMock -> {
            HttpGet get = (HttpGet) invocationOnMock.getArguments()[0];
            mockLog.append(get.getMethod()).append(" ").append(get.getURI()).append("  ");
            CloseableHttpResponse response = mock(CloseableHttpResponse.class);
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(mockReturnCode);
            when(response.getStatusLine()).thenReturn(statusLine);
            if (mockReturnCode == 100000) throw new RuntimeException("FAIL");
            HttpEntity entity = mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            String returnMessage = "{\"foo\":\"bar\", \"no\":3, \"error-code\": " + mockReturnCode + "}";
            InputStream stream = new ByteArrayInputStream(returnMessage.getBytes(StandardCharsets.UTF_8));
            when(entity.getContent()).thenReturn(stream);
            return response;
        });
        doNothing().when(httpMock).close();
        return httpMock;
    }

    @Test
    public void testBasicParsingSingleServer() throws Exception {
        Set<String> configServers = new ArraySet<>(2);
        configServers.add("host1");
        configServers.add("host2");
        ConfigServerHttpRequestExecutor executor = new ConfigServerHttpRequestExecutor(configServers, createClientMock());
        TestPojo answer = executor.get("/path", 666, TestPojo.class);
        assertThat(answer.foo, is("bar"));
        assertLogStringContainsGETForAHost();
    }

    @Test(expected = HttpException.class)
    public void testBasicFailure() throws Exception {
        Set<String> configServers = new ArraySet<>(2);
        configServers.add("host1");
        configServers.add("host2");
        // Server is returning 400, no retries.
        mockReturnCode = 400;
        ConfigServerHttpRequestExecutor executor = new ConfigServerHttpRequestExecutor(configServers, createClientMock());

        TestPojo testPojo = executor.get("/path", 666, TestPojo.class);
        assertEquals(testPojo.errorCode.intValue(), mockReturnCode);
        assertLogStringContainsGETForAHost();
    }

    @Test
    public void testBasicSuccessWithNoRetries() throws Exception {
        Set<String> configServers = new ArraySet<>(2);
        configServers.add("host1");
        configServers.add("host2");
        // Server is returning 201, no retries.
        mockReturnCode = 201;
        ConfigServerHttpRequestExecutor executor = new ConfigServerHttpRequestExecutor(configServers, createClientMock());

        TestPojo testPojo = executor.get("/path", 666, TestPojo.class);
        assertEquals(testPojo.errorCode.intValue(), mockReturnCode);
        assertLogStringContainsGETForAHost();
    }

    @Test
    public void testRetries() throws Exception {
        Set<String> configServers = new ArraySet<>(2);
        configServers.add("host1");
        configServers.add("host2");
        // Client is throwing exception, should be retries.
        mockReturnCode = 100000;
        ConfigServerHttpRequestExecutor executor = new ConfigServerHttpRequestExecutor(configServers, createClientMock());
        try {
            executor.get("/path", 666, TestPojo.class);
            fail("Expected failure");
        } catch (Exception e) {
            // ignore
        }

        String[] log = mockLog.toString().split("  ");
        assertThat(log, arrayContainingInAnyOrder("GET http://host1:666/path", "GET http://host2:666/path",
                                         "GET http://host1:666/path", "GET http://host2:666/path"));
    }

    @Test
    public void testNotFound() throws Exception {
        Set<String> configServers = new ArraySet<>(2);
        configServers.add("host1");
        configServers.add("host2");
        // Server is returning 404, special exception is thrown.
        mockReturnCode = 404;
        ConfigServerHttpRequestExecutor executor = new ConfigServerHttpRequestExecutor(configServers, createClientMock());
        try {
            executor.get("/path", 666, TestPojo.class);
            fail("Expected exception");
        } catch (HttpException.NotFoundException e) {
            // ignore
        }
        assertLogStringContainsGETForAHost();
    }

    private void assertLogStringContainsGETForAHost() {
        String logString = mockLog.toString();
        //assertThat(logString, startsWith("GET http://host"));
        //assertThat(logString, endsWith(":666/path  "));
        assertTrue("log does not contain expected entries:" + logString,
                   (logString.equals("GET http://host1:666/path  ") || logString.equals("GET http://host2:666/path  ")));
    }
}

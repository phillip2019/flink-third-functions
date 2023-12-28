package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.chinagoods.bigdata.connectors.http.internal.config.SinkRequestEncryptionMode;
import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;
import okhttp3.OkHttpClient;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.utils.ThreadUtils;

public abstract class AbstractRequestSubmitter implements RequestSubmitter {

    protected static final int HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE = 1;

    protected static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    protected static final String DEFAULT_REQUEST_ENCRYPTION_MODE = SinkRequestEncryptionMode.PLAIN.getMode();

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    protected final ExecutorService publishingThreadPool;

    protected final int httpRequestTimeOutSeconds;

    protected final String[] headersAndValues;

    protected final OkHttpClient httpClient;

    public AbstractRequestSubmitter(
            Properties properties,
            String[] headersAndValues,
            OkHttpClient httpClient) {

        this.headersAndValues = headersAndValues;
        this.publishingThreadPool =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-response-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );

        this.httpClient = httpClient;
    }

    public abstract List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(String endpointUrl, List<HttpSinkRequestEntry> requestsToSubmit);
}

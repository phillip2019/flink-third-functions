package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.chinagoods.bigdata.connectors.http.internal.config.ConfigException;
import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.chinagoods.bigdata.connectors.http.internal.utils.ThreadUtils;

public class BatchRequestSubmitterFactory implements RequestSubmitterFactory {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    private final String maxBatchSize;

    public BatchRequestSubmitterFactory(int maxBatchSize) {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException(
                "Batch Request submitter batch size must be greater than zero.");
        }
        this.maxBatchSize = String.valueOf(maxBatchSize);
    }

    @Override
    public BatchRequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {
        String batchRequestSize =
            properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_BATCH_REQUEST_SIZE);
        if (StringUtils.isNullOrWhitespaceOnly(batchRequestSize)) {
            properties.setProperty(
                HttpConnectorConfigProperties.SINK_HTTP_BATCH_REQUEST_SIZE,
                maxBatchSize
            );
        } else {
            try {
                // TODO Create property validator someday.
                int batchSize = Integer.parseInt(batchRequestSize);
                if (batchSize < 1) {
                    throw new ConfigException(
                        String.format("Property %s must be greater than 0 but was: %s",
                            HttpConnectorConfigProperties.SINK_HTTP_BATCH_REQUEST_SIZE,
                            batchRequestSize)
                    );
                }
            } catch (NumberFormatException e) {
                // TODO Create property validator someday.
                throw new ConfigException(
                    String.format("Property %s must be an integer but was: %s",
                        HttpConnectorConfigProperties.SINK_HTTP_BATCH_REQUEST_SIZE,
                        batchRequestSize),
                    e
                );
            }
        }

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-batch-request-worker",
                    ThreadUtils.LOGGING_EXCEPTION_HANDLER)
            );

        return new BatchRequestSubmitter(
                properties,
                headersAndValues,
                JavaNetHttpClientFactory.createClient(properties, httpClientExecutor)
            );
    }
}

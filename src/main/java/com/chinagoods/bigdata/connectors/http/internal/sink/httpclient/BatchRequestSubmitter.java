package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.flink.annotation.VisibleForTesting;

import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;

import static com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties.PROP_DELIM;

/**
 * This implementation groups received events in batches and submits each batch as individual HTTP
 * requests. Batch is created based on batch size or based on HTTP method type.
 */
@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    private static final byte[] BATCH_START_BYTES = "[".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_END_BYTES = "]".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_ELEMENT_DELIM_BYTES = ",".getBytes(StandardCharsets.UTF_8);

    private final int httpRequestBatchSize;

    public BatchRequestSubmitter(
            Properties properties,
            String[] headersAndValue,
            OkHttpClient httpClient) {

        super(properties, headersAndValue, httpClient);

        this.httpRequestBatchSize = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_BATCH_REQUEST_SIZE)
        );
    }


    @VisibleForTesting
    int getBatchSize() {
        return httpRequestBatchSize;
    }

    private CompletableFuture<JavaNetHttpResponseWrapper> sendBatch(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestBatch) {

        HttpRequest httpRequest = buildHttpRequest(requestBatch, URI.create(endpointUrl));

        CompletableFuture<JavaNetHttpResponseWrapper> future = new CompletableFuture<>();

        httpClient
            .newCall(httpRequest.getHttpRequest())
                .enqueue(new Callback() {
                    @Override
                    public void onFailure(@NotNull Call call, @NotNull IOException e) {
                        log.error("Request fatally failed because of an exception", e);
                        future.completeExceptionally(e);
                    }

                    @Override
                    public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                        future.complete(new JavaNetHttpResponseWrapper(httpRequest, response));
                    }
                });
        return future;
    }

    private HttpRequest buildHttpRequest(List<HttpSinkRequestEntry> requestBatch, URI endpointUri) {
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");

        try {
            String method = requestBatch.get(0).method;
            List<byte[]> elements = new ArrayList<>(requestBatch.size());

            // By default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
            // into the HTTP body without any context.
            // What we do here is we pack every Json/byteArray into Json Array hence '[' and ']'
            // at the end, and we separate every element with comma.
            elements.add(BATCH_START_BYTES);
            for (HttpSinkRequestEntry entry : requestBatch) {
                elements.add(entry.element);
                elements.add(BATCH_ELEMENT_DELIM_BYTES);
            }
            elements.set(elements.size() - 1, BATCH_END_BYTES);
            RequestBody body = RequestBody.create(Strings.join(elements, ' '), mediaType);

            Headers.Builder headersBuilder = new Headers.Builder()
                    .add("Authorization", "Bearer your_token")
                    .add("Content-Type", "application/json");
            for (String hv : headersAndValues) {
                String[] keyValueArr = hv.split(PROP_DELIM, 1);
                headersBuilder.set(keyValueArr[0], keyValueArr[1]);
            }

            Request request = new Request.Builder()
                    .url(endpointUri.toURL())
                    .post(body)
                    .headers(headersBuilder.build())
                    .build();
            return new HttpRequest(request, elements, method);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(String endpointUrl, List<HttpSinkRequestEntry> requestsToSubmit) {
        if (requestsToSubmit.isEmpty()) {
            return Collections.emptyList();
        }

        List<CompletableFuture<JavaNetHttpResponseWrapper>> responseFutures = new ArrayList<>();
        String previousReqeustMethod = requestsToSubmit.get(0).method;
        List<HttpSinkRequestEntry> requestBatch = new ArrayList<>(httpRequestBatchSize);

        for (HttpSinkRequestEntry entry : requestsToSubmit) {
            if (requestBatch.size() == httpRequestBatchSize
                    || !previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                // break batch and submit
                responseFutures.add(sendBatch(endpointUrl, requestBatch));
                requestBatch.clear();
            }
            requestBatch.add(entry);
            previousReqeustMethod = entry.method;
        }

        // submit anything that left
        responseFutures.add(sendBatch(endpointUrl, requestBatch));
        return responseFutures;
    }
}

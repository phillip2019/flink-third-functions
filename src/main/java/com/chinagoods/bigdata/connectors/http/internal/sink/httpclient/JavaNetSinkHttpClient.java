package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.flink.annotation.VisibleForTesting;

import com.chinagoods.bigdata.connectors.http.HttpPostRequestCallback;
import com.chinagoods.bigdata.connectors.http.internal.HeaderPreprocessor;
import com.chinagoods.bigdata.connectors.http.internal.SinkHttpClient;
import com.chinagoods.bigdata.connectors.http.internal.SinkHttpClientResponse;
import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.chinagoods.bigdata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.chinagoods.bigdata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.chinagoods.bigdata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.chinagoods.bigdata.connectors.http.internal.utils.HttpHeaderUtils;

/**
 * An implementation of {@link SinkHttpClient} that uses OKHttp {@link OkHttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final RequestSubmitter requestSubmitter;

    public JavaNetSinkHttpClient(
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback,
            HeaderPreprocessor headerPreprocessor,
            RequestSubmitterFactory requestSubmitterFactory) {

        this.httpPostRequestCallback = httpPostRequestCallback;
        this.headerMap = HttpHeaderUtils.prepareHeaderMap(
            HttpConnectorConfigProperties.SINK_HEADER_PREFIX,
            properties,
            headerPreprocessor
        );

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(properties)
                .whiteListPrefix(HttpConnectorConfigProperties.HTTP_ERROR_SINK_CODE_WHITE_LIST)
                .errorCodePrefix(HttpConnectorConfigProperties.HTTP_ERROR_SINK_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);
        this.requestSubmitter = requestSubmitterFactory.createSubmitter(
            properties,
            headersAndValues
        );
    }

    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
        List<HttpSinkRequestEntry> requestEntries,
        String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl)
            .thenApply(responses -> prepareSinkHttpClientResponse(responses, endpointUrl));
    }

    private CompletableFuture<List<JavaNetHttpResponseWrapper>> submitRequests(
            List<HttpSinkRequestEntry> requestEntries,
            String endpointUrl) {

        List<CompletableFuture<JavaNetHttpResponseWrapper>> responseFutures = requestSubmitter.submit(endpointUrl, requestEntries);
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
        return allFutures.thenApply(_void -> responseFutures.stream().map(CompletableFuture::join)
            .collect(Collectors.toList()));
    }

    private SinkHttpClientResponse prepareSinkHttpClientResponse(
        List<JavaNetHttpResponseWrapper> responses,
        String endpointUrl) {
        List<HttpRequest> successfulResponses = new ArrayList<>();
        List<HttpRequest> failedResponses = new ArrayList<>();

        for (JavaNetHttpResponseWrapper response : responses) {
            HttpRequest sinkRequestEntry = response.getHttpRequest();
            Optional<okhttp3.Response> optResponse = response.getResponse();

            try {
                httpPostRequestCallback.call(
                    optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);
            } catch (IOException e) {
                log.error("http sink callback error, endpointUrl: {}, sinkRequestEntry: {}, response: {}", endpointUrl, sinkRequestEntry, optResponse.orElse(null), e);
                throw new RuntimeException(e);
            }

            // TODO Add response processor here and orchestrate it with statusCodeChecker.
            if (!optResponse.isPresent() ||
                statusCodeChecker.isErrorCode(optResponse.get().code())) {
                failedResponses.add(sinkRequestEntry);
            } else {
                successfulResponses.add(sinkRequestEntry);
            }
        }

        return new SinkHttpClientResponse(successfulResponses, failedResponses);
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}

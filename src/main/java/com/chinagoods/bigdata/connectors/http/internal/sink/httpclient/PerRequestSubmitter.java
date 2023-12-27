package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties.PROP_DELIM;

/**
 * This implementation creates HTTP requests for every processed event.
 */
@Slf4j
public class PerRequestSubmitter extends AbstractRequestSubmitter {

    public PerRequestSubmitter(
            Properties properties,
            String[] headersAndValues,
            OkHttpClient httpClient) {

        super(properties, headersAndValues, httpClient);
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestsToSubmit) {

        URI endpointUri = URI.create(endpointUrl);
        List<CompletableFuture<JavaNetHttpResponseWrapper>> responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        for (HttpSinkRequestEntry entry : requestsToSubmit) {
            HttpRequest httpRequest = buildHttpRequest(entry, endpointUri);
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
            responseFutures.add(future);
        }
        return responseFutures;
    }

    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(requestEntry.element, mediaType);
        Headers.Builder headersBuilder = new Headers.Builder()
                .add("Authorization", "Bearer your_token")
                .add("Content-Type", "application/json");
        for (String hv : headersAndValues) {
            String[] keyValueArr = hv.split(PROP_DELIM, 1);
            headersBuilder.set(keyValueArr[0], keyValueArr[1]);
        }
        URL url = null;
        try {
            url = endpointUri.toURL();
        } catch (MalformedURLException ignored) {
        }
        assert url != null;
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .headers(headersBuilder.build())
                .build();

        return new HttpRequest(request, Collections.singletonList(requestEntry.element), requestEntry.method);
    }
}

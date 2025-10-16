package com.chinagoods.bigdata.connectors.http.internal.table.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import com.chinagoods.bigdata.connectors.http.HttpPostRequestCallback;
import com.chinagoods.bigdata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.chinagoods.bigdata.connectors.http.internal.utils.ConfigUtils;
import okhttp3.Response;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpDynamicSink}.
 */
@Slf4j
public class Slf4jHttpPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {

    @Override
    public void call(Response response,
                     HttpRequest requestEntry,
                     String endpointUrl,
                     Map<String, String> headerMap) throws IOException {

        String requestBody = requestEntry.getElements().stream()
            .map(element -> new String(element, StandardCharsets.UTF_8))
            .collect(Collectors.joining());

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: null",
                requestEntry.getMethod(),
                requestBody
            );
        } else {
            AtomicReference<String> responseContentAR = new AtomicReference<>("");
            Optional.of(response.peekBody(Long.MAX_VALUE)).ifPresent(e -> {
                try {
                    responseContentAR.set(e.string());
                } catch (IOException ex) {
                    log.error("解析json内容异常");
                }
            });
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: {}\n    Body: {}",
                requestEntry.method,
                requestBody,
                response,
                responseContentAR.get().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
            );
        }
    }
}

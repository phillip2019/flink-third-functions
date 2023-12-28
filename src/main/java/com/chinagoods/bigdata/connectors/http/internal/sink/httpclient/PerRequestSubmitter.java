package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.config.SinkRequestEncryptionMode;
import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.chinagoods.bigdata.util.Base64Utils;
import com.chinagoods.bigdata.util.JacksonBuilder;
import com.chinagoods.bigdata.util.RSAUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    public String encryptionMode;

    public String encryptionXsykPubKey;

    public String encryptionXsykAppId;

    public PerRequestSubmitter(
            Properties properties,
            String[] headersAndValues,
            OkHttpClient httpClient) {

        super(properties, headersAndValues, httpClient);
        encryptionMode = properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_REQUEST_ENCRYPTION_MODE,
                DEFAULT_REQUEST_ENCRYPTION_MODE);

        encryptionXsykPubKey = properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_REQUEST_ENCRYPTION_XSYK_PUB_KEY,
                "");

        encryptionXsykAppId = properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_REQUEST_ENCRYPTION_XSYK_APP_ID,
                "CG001");
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

    public String xsykEncryption(byte[] bodyElement) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        JsonNode paramsData = null;
        try {
            paramsData = JacksonBuilder.mapper.readTree(bodyElement);
        } catch (IOException e) {
            log.error("The content is not json content, deserialization failed!");
            throw new RuntimeException("The content is not json content, deserialization failed!", e);
        }
        ObjectNode paramsOn = (JacksonBuilder.mapper.createObjectNode())
                .putPOJO("data", paramsData)
                .put("appId", encryptionXsykAppId)
                .put("timestamp", timestamp)
                ;
        String contentJsonStr = null; //报文json串
        try {
            contentJsonStr = JacksonBuilder.mapper.writeValueAsString(paramsOn);
        } catch (JsonProcessingException e) {
            log.error("The content is not json content, serialization failed!");
            throw new RuntimeException("The content is not json content, serialization failed!", e);
        }

        byte[] encrypted = null;
        String sign = null; //签名
        try {
            encrypted = RSAUtils.encryptByPublicKey(contentJsonStr.getBytes(), encryptionXsykPubKey);
            sign = Base64Utils.encode(encrypted);
        } catch (Exception e) {
            log.error("Encryption failed, the original content is {}, the secret key is {}!", contentJsonStr, encryptionXsykPubKey);
            throw new RuntimeException(String.format("Encryption failed, the original content is %s, the secret key is %s!", contentJsonStr, encryptionXsykPubKey), e);
        }
        paramsOn.put("sign", sign);
        try {
            return JacksonBuilder.mapper.writeValueAsString(paramsOn);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body;
        // 若加密方式为xsyk,则内容需要进行加密
        if (SinkRequestEncryptionMode.XSYK.getMode().equals(encryptionMode)) {
            body = RequestBody.create(xsykEncryption(requestEntry.element), mediaType);
        } else {
            body = RequestBody.create(requestEntry.element, mediaType);
        }
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

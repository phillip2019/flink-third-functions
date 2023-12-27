package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.chinagoods.bigdata.connectors.http.internal.sink.HttpSinkRequestEntry;

/**
 * Submits request via HTTP.
 */
public interface RequestSubmitter {

    List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
        String endpointUrl,
        List<HttpSinkRequestEntry> requestToSubmit);
}

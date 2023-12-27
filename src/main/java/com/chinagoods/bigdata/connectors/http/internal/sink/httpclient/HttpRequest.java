package com.chinagoods.bigdata.connectors.http.internal.sink.httpclient;

import java.util.List;

import lombok.Data;

@Data
public class HttpRequest {

    public final okhttp3.Request httpRequest;

    public final List<byte[]> elements;

    public final String method;

}

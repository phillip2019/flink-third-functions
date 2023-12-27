package com.chinagoods.bigdata.udf;

import com.chinagoods.bigdata.util.Base64Utils;
import com.chinagoods.bigdata.util.JacksonBuilder;
import com.chinagoods.bigdata.util.RSAUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpSinkUDF extends ScalarFunction {

    final Logger LOG = LoggerFactory.getLogger(getClass());
    private OkHttpClient client;

    /**
     * 应用标识
     **/
    public static final String APP_ID = "CG001";

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .writeTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
    }

    /*
     * @description UDF发送http数据
     * @author xiaowei.song
     * @date 27/12/2023 上午 9:14
     * @version v1.0.0
     * @params url url
     * @params pubKey 公钥
     * @params params 参数JSON字符串
     **/
    public Boolean eval(String url, String pubKey, String params) throws Exception {
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        String timestamp = String.valueOf(System.currentTimeMillis());
        JsonNode paramsData = JacksonBuilder.mapper.readTree(params);
        ObjectNode paramsOn = (JacksonBuilder.mapper.createObjectNode())
//                .put("data", params)
                .putPOJO("data", paramsData)
                .put("appId", APP_ID)
                .put("timestamp", timestamp)
                ;
        String contentJsonStr = JacksonBuilder.mapper.writeValueAsString(paramsOn); //报文json串
        byte[] encrypted = RSAUtils.encryptByPublicKey(contentJsonStr.getBytes(), pubKey);
        String sign = Base64Utils.encode(encrypted); //签名
        paramsOn.put("sign", sign);

        try {
            RequestBody body = RequestBody.create(mediaType, JacksonBuilder.mapper.writeValueAsString(paramsOn));
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    LOG.error("未知异常请求代码: code={}, 异常返回内容为： {}", response.code(), response);
//                    throw new IOException("Unexpected code " + response);
                    return Boolean.FALSE;
                }

                ResponseBody respBody = response.body();
                String contentStr = "{\"code\": -1, \"message\": \"返回内容为空\"}";
                try {
                    contentStr = respBody.string();
                } catch (IOException e) {
                    LOG.error("发送消息失败，获取消息内容异常", e);
                }
                JsonNode rspJn = (JacksonBuilder.mapper.createObjectNode()).put("code", -1).put("message", "返回内容异常");
                try {
                    rspJn = JacksonBuilder.mapper.readTree(contentStr);
                } catch (JsonProcessingException e) {
                    LOG.error("发送消息失败，反序列化消息内容异常，消息内容为: {}", contentStr, e);
                }

                int errorCode = rspJn.path("code").asInt(-1);
                if (errorCode != 20000) {
                    LOG.error("发送失败，接口返回错误码 errorCode: {}, errorInfo: {}", errorCode, rspJn.path("message").asText(""));
                    return Boolean.FALSE;
                }

                LOG.info("发送成功， {}", rspJn.toString());
                return Boolean.TRUE;
            } catch (IOException e) {
                LOG.error("Failed to send HTTP request: ", e);
                return Boolean.FALSE;
            }
        } catch (Exception e) {
            LOG.error("Failed to convert map to JSON: ", e);
            return Boolean.FALSE;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
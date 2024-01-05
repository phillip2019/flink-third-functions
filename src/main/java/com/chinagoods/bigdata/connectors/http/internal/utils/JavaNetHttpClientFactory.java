package com.chinagoods.bigdata.connectors.http.internal.utils;

import com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties;
import com.chinagoods.bigdata.connectors.http.internal.security.SecurityContext;
import com.chinagoods.bigdata.connectors.http.internal.security.SelfSignedTrustManager;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.flink.util.StringUtils;

import javax.net.ssl.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public class JavaNetHttpClientFactory {

    public static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    public static int httpRequestTimeOutSeconds;

    /**
     * Creates Java's {@link OkHttpClient} instance that will be using default, JVM shared {@link
     * java.util.concurrent.ForkJoinPool} for async calls.
     *
     * @param properties properties used to build {@link SSLContext}
     * @return new {@link OkHttpClient} instance.
     */
    public static OkHttpClient createClient(Properties properties) {
        SSLContext sslContext = getSslContext(properties);
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        httpRequestTimeOutSeconds = Integer.parseInt(
                properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_TIMEOUT_SECONDS,
                        DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );
        return new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                .connectTimeout(Duration.ofSeconds(httpRequestTimeOutSeconds)) // 设置连接超时时间为10秒
                .readTimeout(Duration.ofSeconds(httpRequestTimeOutSeconds)) // 设置读取超时时间为10秒
                .sslSocketFactory(sslSocketFactory)
                .build();
    }

    /**
     * Creates Java's {@link OkHttpClient} instance that will be using provided Executor for all async
     * calls.
     *
     * @param properties      properties used to build {@link SSLContext}
     * @param executorService {@link Executor} for async calls.
     * @return new {@link OkHttpClient} instance.
     */
    public static OkHttpClient createClient(Properties properties, ExecutorService executorService) {

        httpRequestTimeOutSeconds = Integer.parseInt(
                properties.getProperty(HttpConnectorConfigProperties.SINK_HTTP_TIMEOUT_SECONDS,
                        DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );

        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };
            SSLContext sslContext = getSslContext(properties);
            // Install the all-trusting trust manager
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final javax.net.ssl.SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.dispatcher(new Dispatcher(executorService))
                    .connectTimeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                    .sslSocketFactory(sslSocketFactory)
                    .hostnameVerifier(new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                    });
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates an {@link SSLContext} based on provided properties.
     * <ul>
     *     <li>{@link HttpConnectorConfigProperties#ALLOW_SELF_SIGNED}</li>
     *     <li>{@link HttpConnectorConfigProperties#SERVER_TRUSTED_CERT}</li>
     *     <li>{@link HttpConnectorConfigProperties#PROP_DELIM}</li>
     *     <li>{@link HttpConnectorConfigProperties#CLIENT_CERT}</li>
     *     <li>{@link HttpConnectorConfigProperties#CLIENT_PRIVATE_KEY}</li>
     * </ul>
     *
     * @param properties properties used to build {@link SSLContext}
     * @return new {@link SSLContext} instance.
     */
    private static SSLContext getSslContext(Properties properties) {
        SecurityContext securityContext = createSecurityContext(properties);

        boolean selfSignedCert = Boolean.parseBoolean(
                properties.getProperty(HttpConnectorConfigProperties.ALLOW_SELF_SIGNED, "false"));

        String[] serverTrustedCerts = properties
                .getProperty(HttpConnectorConfigProperties.SERVER_TRUSTED_CERT, "")
                .split(HttpConnectorConfigProperties.PROP_DELIM);

        String clientCert = properties
                .getProperty(HttpConnectorConfigProperties.CLIENT_CERT, "");

        String clientPrivateKey = properties
                .getProperty(HttpConnectorConfigProperties.CLIENT_PRIVATE_KEY, "");

        for (String cert : serverTrustedCerts) {
            if (!StringUtils.isNullOrWhitespaceOnly(cert)) {
                securityContext.addCertToTrustStore(cert);
            }
        }

        if (!StringUtils.isNullOrWhitespaceOnly(clientCert)
                && !StringUtils.isNullOrWhitespaceOnly(clientPrivateKey)) {
            securityContext.addMTlsCerts(clientCert, clientPrivateKey);
        }

        // NOTE TrustManagers must be created AFTER adding all certificates to KeyStore.
        TrustManager[] trustManagers = getTrustedManagers(securityContext, selfSignedCert);
        return securityContext.getSslContext(trustManagers);
    }

    private static TrustManager[] getTrustedManagers(
            SecurityContext securityContext,
            boolean selfSignedCert) {

        TrustManager[] trustManagers = securityContext.getTrustManagers();

        if (selfSignedCert) {
            return wrapWithSelfSignedManagers(trustManagers).toArray(new TrustManager[0]);
        } else {
            return trustManagers;
        }
    }

    private static List<TrustManager> wrapWithSelfSignedManagers(TrustManager[] trustManagers) {
        log.warn("Creating Trust Managers for self-signed certificates - not Recommended. "
                + "Use [" + HttpConnectorConfigProperties.SERVER_TRUSTED_CERT + "] "
                + "connector property to add certificated as trusted.");

        List<TrustManager> selfSignedManagers = new ArrayList<>(trustManagers.length);
        for (TrustManager trustManager : trustManagers) {
            selfSignedManagers.add(new SelfSignedTrustManager((X509TrustManager) trustManager));
        }
        return selfSignedManagers;
    }

    /**
     * Creates a {@link SecurityContext} with empty {@link java.security.KeyStore} or loaded from
     * file.
     *
     * @param properties Properties for creating {@link SecurityContext}
     * @return new {@link SecurityContext} instance.
     */
    private static SecurityContext createSecurityContext(Properties properties) {

        String keyStorePath =
                properties.getProperty(HttpConnectorConfigProperties.KEY_STORE_PATH, "");

        if (StringUtils.isNullOrWhitespaceOnly(keyStorePath)) {
            return SecurityContext.create();
        } else {
            char[] storePassword =
                    properties.getProperty(HttpConnectorConfigProperties.KEY_STORE_PASSWORD, "")
                            .toCharArray();
            if (storePassword.length == 0) {
                throw new RuntimeException("Missing password for provided KeyStore");
            }
            return SecurityContext.createFromKeyStore(keyStorePath, storePassword);
        }
    }
}

package com.chinagoods.bigdata.connectors.http.internal.table.sink;

import com.chinagoods.bigdata.connectors.http.internal.config.SinkRequestEncryptionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static com.chinagoods.bigdata.connectors.http.internal.config.HttpConnectorConfigProperties.*;

/**
 * Table API options for {@link HttpDynamicSink}.
 */
public class HttpDynamicSinkConnectorOptions {

    public static final ConfigOption<String> URL =
        ConfigOptions.key("url").stringType().noDefaultValue()
            .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> INSERT_METHOD =
        ConfigOptions.key("insert-method")
            .stringType()
            .defaultValue("POST")
            .withDescription("Method used for requests built from SQL's INSERT.");

    public static final ConfigOption<String> REQUEST_CALLBACK_IDENTIFIER =
        ConfigOptions.key(SINK_REQUEST_CALLBACK_IDENTIFIER)
            .stringType()
            .defaultValue(Slf4jHttpPostRequestCallbackFactory.IDENTIFIER);

    public static final ConfigOption<String> REQUEST_ENCRYPTION_MODE =
            ConfigOptions.key(SINK_HTTP_REQUEST_ENCRYPTION_MODE)
                    .stringType()
                    .defaultValue(SinkRequestEncryptionMode.PLAIN.getMode());

    public static final ConfigOption<String> REQUEST_ENCRYPTION_XSYK_PUB_KEY =
            ConfigOptions.key(SINK_HTTP_REQUEST_ENCRYPTION_XSYK_PUB_KEY)
                    .stringType()
                    .defaultValue("");

    public static final ConfigOption<String> REQUEST_ENCRYPTION_XSYK_APP_ID =
            ConfigOptions.key(SINK_HTTP_REQUEST_ENCRYPTION_XSYK_APP_ID)
                    .stringType()
                    .defaultValue("CG001");
}

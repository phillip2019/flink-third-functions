package com.chinagoods.bigdata.connectors.http.internal.config;

public enum SinkRequestEncryptionMode {

    PLAIN("plain"),
    /**
     * 小商迎客加密方式
     **/
    XSYK("xsyk");

    private final String mode;

    SinkRequestEncryptionMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}

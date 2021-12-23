package com.chinagoods.bigdata.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author xiaowei.song
 */
public class JavaDateTime extends ScalarFunction {
    public String eval(String str) {
        return new org.joda.time.DateTime().toString();
    }
}

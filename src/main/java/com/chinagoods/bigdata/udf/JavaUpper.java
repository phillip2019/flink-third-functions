package com.chinagoods.bigdata.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author xiaowei.song
 */
public class JavaUpper extends ScalarFunction {
    public String eval(String str) {
        return str.toUpperCase();
    }
}

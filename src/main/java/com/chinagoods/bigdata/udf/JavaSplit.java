package com.chinagoods.bigdata.udf;

import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;

/**
 * @author xiaowei.song
 */
public class JavaSplit extends TableFunction<String> {
    public void eval(String str, String split) {
        // use collect(...) to emit a row.
        // str.split("#").foreach(x => collect((x, x.length)))
        Arrays.stream(str.split(split)).forEach(x -> {
            collect(x);
        });
    }

    public void eval(String str) {
        eval(str, ",");
    }

}

package com.chinagoods.bigdata.udf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Accumulator for GroupConcatUDAF.
 * 存储聚合过程中的中间状态
 *
 * @author xiaowei.song
 */
public class GroupConcatAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public List<String> values = new ArrayList<>();
    public String separator = ","; // 默认分隔符
}

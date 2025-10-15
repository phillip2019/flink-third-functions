/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chinagoods.bigdata.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * GroupConcat UDAF - 实现类似Hive中group_concat的功能
 * 将同一组内的多个字符串值连接成一个字符串，使用指定的分隔符
 * 
 * 使用示例:
 * SELECT group_concat(name) FROM table GROUP BY category;
 * SELECT group_concat(name, '|') FROM table GROUP BY category;
 * 
 * @author xiaowei.song
 */
public class GroupConcatUDAF extends AggregateFunction<String, GroupConcatUDAF.GroupConcatAccumulator> {

    /**
     * 累加器类 - 存储聚合过程中的中间状态
     */
    public static class GroupConcatAccumulator {
        public List<String> values = new ArrayList<>();
        public String separator = ","; // 默认分隔符
    }

    @Override
    public GroupConcatAccumulator createAccumulator() {
        return new GroupConcatAccumulator();
    }

    /**
     * 累加方法 - 添加新值到累加器
     * @param accumulator 累加器
     * @param value 要添加的值
     */
    public void accumulate(GroupConcatAccumulator accumulator, String value) {
        if (value != null && !value.trim().isEmpty()) {
            accumulator.values.add(value);
        }
    }

    /**
     * 累加方法 - 添加新值到累加器，并指定分隔符
     * @param accumulator 累加器
     * @param value 要添加的值
     * @param separator 分隔符
     */
    public void accumulate(GroupConcatAccumulator accumulator, String value, String separator) {
        if (separator != null) {
            accumulator.separator = separator;
        }
        accumulate(accumulator, value);
    }

    /**
     * 撤回方法 - 从累加器中移除值（用于支持更新和删除操作）
     * @param accumulator 累加器
     * @param value 要移除的值
     */
    public void retract(GroupConcatAccumulator accumulator, String value) {
        if (value != null) {
            accumulator.values.remove(value);
        }
    }

    /**
     * Retract method - remove value from accumulator with separator
     * @param accumulator accumulator
     * @param value value to remove
     * @param separator separator
     */
    public void retract(GroupConcatAccumulator accumulator, String value, String separator) {
        if (separator != null) {
            accumulator.separator = separator;
        }
        retract(accumulator, value);
    }

    /**
     * Merge method - merge multiple accumulators
     * @param accumulator target accumulator
     * @param iterable accumulators to merge
     */
    public void merge(GroupConcatAccumulator accumulator, Iterable<GroupConcatAccumulator> iterable) {
        for (GroupConcatAccumulator otherAcc : iterable) {
            if (otherAcc.values != null) {
                accumulator.values.addAll(otherAcc.values);
            }
            // Use the last non-null separator
            if (otherAcc.separator != null) {
                accumulator.separator = otherAcc.separator;
            }
        }
    }

    /**
     * Get final result
     * @param accumulator accumulator
     * @return concatenated string
     */
    @Override
    public String getValue(GroupConcatAccumulator accumulator) {
        if (accumulator.values == null || accumulator.values.isEmpty()) {
            return null;
        }
        return String.join(accumulator.separator, accumulator.values);
    }
}
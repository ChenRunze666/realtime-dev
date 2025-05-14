package com.bg.dwd.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.bg.dwd.function.MapOsFunc
 * @Author Chen.Run.ze
 * @Date 2025/5/14 14:35
 * @description: 对相同 uid 不同的os进行聚合
 */
public class MapOsFunc extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    private transient ValueState<Long> pvState;
    private transient MapState<String, Set<String>> fieldsState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class)
        );

        // 初始化字段集合状态（使用TypeHint保留泛型信息）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {})
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }


    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);

        // 提取设备信息和搜索词
        String os = value.getString("os");
        String searchItem = value.containsKey("keyword") ? value.getString("keyword") : null;

        // 更新字段集合
        updateField("os", os);
        if (searchItem != null) {
            updateField("keyword", searchItem);
        }

        // 构建输出JSON
        JSONObject output = new JSONObject();
        output.put("uid", value.getString("uid"));
        output.put("pv", pv);
        output.put("os", String.join(",", getField("os")));
        output.put("keyword", String.join(",", getField("keyword")));

        out.collect(output);
    }

    // 辅助方法：更新字段集合
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    // 辅助方法：获取字段集合
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }


}
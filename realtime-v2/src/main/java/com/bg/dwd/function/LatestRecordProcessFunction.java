package com.bg.dwd.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.bg.dwd.function.MapLogTsNew
 * @Author Chen.Run.ze
 * @Date 2025/5/14 16:20
 * @description: 获取每条Log最新的数据
 */
public  class LatestRecordProcessFunction extends KeyedProcessFunction<Integer, JSONObject, JSONObject> {

    private transient ValueState<JSONObject> maxState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<JSONObject> descriptor =
                new ValueStateDescriptor<>("maxRecord", JSONObject.class);
        maxState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject current, Context ctx, Collector<JSONObject> out) throws Exception {


        long currentTs = current.getLong("ts");

        JSONObject maxRecord = maxState.value();
        if (maxRecord == null) {
            maxState.update(current);
            out.collect(current);
            return;
        }

        long maxTs = maxRecord.getLong("ts");
        if (currentTs > maxTs) {
            maxState.update(current);
            out.collect(current);
        }
    }
}

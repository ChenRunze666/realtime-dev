package com.bg.dwd.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.bg.dwd.function.UserDeduplicationProcessorFunc
 * @Author Chen.Run.ze
 * @Date 2025/5/15 10:58
 * @description: 用户数据去重处理器 - 根据user.id和user.name去重
 */
/**
 * 用户数据去重处理器 - 根据user.id和user.name去重
 */
public class UserDeduplicationProcessorFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    // 状态描述符：记录是否已存在
    private ValueState<Boolean> existsState;

    @Override
    public void open(Configuration parameters) {
        // 初始化状态：保存是否已经存在该用户的标记
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("userExistsState", Boolean.class);
        existsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            JSONObject value,
            Context ctx,
            Collector<JSONObject> out) throws Exception {

        // 获取当前状态值
        Boolean exists = existsState.value();

        // 首次出现时处理并更新状态
        if (exists == null) {
            // 1. 输出记录
            out.collect(value);
            // 2. 更新状态为已存在
            existsState.update(true);
        }
        // 已存在记录直接忽略
    }

    /**
     * 主程序调用示例：
     * DataStream<JSONObject> stream = ...;
     * stream
     *     .keyBy(json -> buildKey(json.getJSONObject("user")))
     *     .process(new UserDeduplicationProcessor())
     */

    /**
     * 构建复合键的方法
     */
    public static String buildKey(JSONObject user) {
        return user.getString("id") + "|" + user.getString("name");
    }
}

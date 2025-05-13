package com.bg.dwd.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.bg.dwd.function.LogFilterFun
 * @Author Chen.Run.ze
 * @Date 2025/5/13 14:28
 * @description: page Log 数据过滤处理
 */
public class LogFilterFun extends RichMapFunction<JSONObject,JSONObject>{

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject object = new JSONObject();
        if (jsonObject.containsKey("common")){
            JSONObject common = jsonObject.getJSONObject("common");
            object.put("uid",common.getString("uid"));
            object.put("ts",jsonObject.getLong("ts"));
            // 去掉版本直接获取 OS
            String os = common.getString("os").split(" ")[0];
            object.put("os",os);
            if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                JSONObject page = jsonObject.getJSONObject("page");
                if (page.containsKey("item_type") && page.getString("item_type").equals("keyword")){
                    object.put("keyword",page.getString("item"));
                }
            }
        }
        return object;
    }
}

package com.bg.dwd.function;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
/**
 * @Package com.bg.dwd.function.test
 * @Author Chen.Run.ze
 * @Date 2025/5/12 19:35
 * @description: 1
 */
public class test {
    public static void main(String[] args) throws Exception {
        JSONObject object = new JSONObject();
        object.put("name","zhangsan");
        object.put("age",18);
        JSONObject object1 = new JSONObject();
        object1.put("name1","lisi");
        object1.put("age1",19);
        JSONObject object2 = new JSONObject();

        object2.put("啊",object);
        object2.put("饿",object1);
        System.out.println(object2);


    }
}

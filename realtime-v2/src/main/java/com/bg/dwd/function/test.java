package com.bg.dwd.function;

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
        String jsonStr = "{\"name\": \"Alice\", \"age\": 25}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(jsonStr);

        // 转换为可修改的 ObjectNode
        if (rootNode instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) rootNode;
            // 更新字段值
            objectNode.put("age", 26);  // 直接覆盖原值

            // 转换为字符串输出
            String updatedJson = mapper.writeValueAsString(objectNode);
            System.out.println(updatedJson);
        }
    }
}

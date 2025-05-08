package com.bg.sensitive_words.APP;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.constant.Constant;
import com.bg.common.util.FlinkSinkUtil;
import com.bg.common.util.FlinkSourceUtil;
import com.bg.sensitive_words.funtion.FilterBloomDeduplicatorFunc;
import com.bg.sensitive_words.funtion.MapCheckRedisSensitiveWordsFunc;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.List;

/**
 * @Package com.bg.sensitive_words.APP.BlackListUserInfo
 * @Author Chen.Run.ze
 * @Date 2025/5/8 18:07
 * @description: 黑名单封禁 Task 04
 */
public class BlackListUserInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        env.setParallelism(10);
        //设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //设置操作用户
        System.setProperty("HADOOP_USER_NAME","root");


        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                FlinkSourceUtil.getKafkaSource(
                        Constant.TOPIC_fact_comment,
                        new Date().toString()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        ).uid("kafka_fact_comment_source").name("kafka_fact_comment_source");

        //7> {"info_original_total_amount":18165.0,"info_activity_reduce_amount":250.0,"commentTxt":"差评：联想Y9000P 2022游戏本，性能一般，散热差，价格过高。,王乐泉","info_province_id":8,"info_payment_way":"3501","ds":"20250504","info_refundable_time":1746999953000,"info_order_status":"1004","info_create_time":1746395153000,"id":207,"spu_id":10,"table":"comment_info","info_operate_time":1746395216000,"info_tm_ms":1746366984988,"op":"c","create_time":1746395216000,"info_user_id":239,"info_op":"u","info_trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3060 钛晶灰等3件商品","sku_id":29,"server_id":"1","dic_name":"1201","info_consignee_tel":"13254399868","info_total_amount":17915.0,"info_out_trade_no":"711713837931767","appraise":"1201","user_id":239,"info_id":2760,"info_coupon_reduce_amount":0.0,"order_id":2760,"info_consignee":"苏露瑶","ts_ms":1746366984978,"db":"gmall2024"}
        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("to_json_string").name("to_json_string");

        //9> {"info_original_total_amount":18996.0,"info_activity_reduce_amount":250.0,"commentTxt":"购买体验极差，手机质量差，信号不稳定，卡顿严重，不推荐购买。,脱衣","info_province_id":14,"info_payment_way":"3501","ds":"20250504","info_refundable_time":1746997054000,"info_order_status":"1004","info_create_time":1746392254000,"id":200,"spu_id":3,"table":"comment_info","info_operate_time":1746392320000,"info_tm_ms":1746366984162,"op":"c","create_time":1746392320000,"info_user_id":635,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 128GB 黑色 支持移动联通电信5G 双卡双待手机等2件商品","sku_id":12,"server_id":"1","dic_name":"1201","info_consignee_tel":"13661497136","info_total_amount":18746.0,"info_out_trade_no":"769394465594733","appraise":"1201","user_id":635,"info_id":2745,"info_coupon_reduce_amount":0.0,"order_id":2745,"info_consignee":"苗寒","ts_ms":1746366984159,"db":"gmall2024"}
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

        //2> {"msg":"商品质量差，颜色不对，味道刺鼻，使用后嘴唇干燥。,出售单管猎枪","consignee":"司空才","violation_grade":"","user_id":51,"violation_msg":"","is_violation":0,"ts_ms":1746366982888,"ds":"20250504"}
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");

        //8> {"msg":"Redmi 10X 4G游戏手机，电池续航一般，拍照效果差强人意，存储空间小，不值得购买。,高压警用电棍出售","consignee":"夏侯宁贵","violation_grade":"P0","user_id":159,"violation_msg":"高压警用电棍出售","is_violation":1,"ts_ms":1745824057013,"ds":"20250428"}
        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    for (int i = 0; i < msgSen.size(); i++) {
                        msgSen.get(i);
                    }
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        }).uid("second sensitive word check").name("second sensitive word check");

        //转换类型
        SingleOutputStreamOperator<String> map = secondCheckMap.map(o -> o.toJSONString());

        //写入Kafka
        map.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_sensitive_words));

        //写入Doris
        map.sinkTo(FlinkSinkUtil.getDorisSink("sensitive_words_user"));

        env.execute();
    }
}

package com.bg.sensitive_words;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.constant.Constant;
import com.bg.common.util.FlinkSourceUtil;
import com.bg.sensitive_words.funtion.AsyncHbaseDimBaseDicFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.bg.sensitive_words.CommentFactData
 * @Author Chen.Run.ze
 * @Date 2025/5/7 15:42
 * @description: Read MySQL CDC to kafka topics
 */
public class CommentFactData {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        env.setParallelism(4);
        //设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //设置操作用户
        System.setProperty("HADOOP_USER_NAME","root");

        // topic_db 数据
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                FlinkSourceUtil.getKafkaSource(
                        Constant.TOPIC_DB,
                        new Date().toString()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("topic_db_source").name("topic_db_source");

        //订单主表
        SingleOutputStreamOperator<JSONObject> FilterOrderInfo = kafkaCdcDbSource.map(o -> JSONObject.parseObject(o))
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info") && json.getJSONObject("after") != null)
                .uid("order_info_source").name("order_info_source");

        //评论表补充维度 补充："dic_name":"中评"
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .filter(json -> json.getJSONObject("after") != null && json.getJSONObject("after").getString("appraise") != null)
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));

        //2> {"op":"c","after":{"create_time":1746395216000,"user_id":239,"appraise":"1203","comment_txt":"评论内容：42468514271596899421353858361649752596863958362119","nick_name":"露瑶","sku_id":8,"id":206,"spu_id":3,"order_id":2760,"dic_name":"N/A"},"source":{"thread":685,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000025","connector":"mysql","pos":3378503,"name":"mysql_binlog_source","row":0,"ts_ms":1746366985000,"snapshot":"false","db":"gmall2024","table":"comment_info"},"ts_ms":1746366984977}
        DataStream<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        100
                ).uid("async_hbase_dim_base_dic_func")
                .name("async_hbase_dim_base_dic_func");


        // 提取 评论表 有用字段合成一个新的JSON数据
        //3> {"op":"c","create_time":1746386528000,"commentTxt":"评论内容：61336696944795731875619477563298138125531937715212","sku_id":32,"server_id":"1","dic_name":"N/A","appraise":"1201","user_id":51,"id":197,"spu_id":11,"order_id":2712,"ts_ms":1746366982890,"db":"gmall2024","table":"comment_info"}
        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject resJsonObj = new JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            resJsonObj.put("ts_ms", tsMs);
                            resJsonObj.put("db", dbName);
                            resJsonObj.put("table", tableName);
                            resJsonObj.put("server_id", serverId);
                            resJsonObj.put("appraise", after.getString("appraise"));
                            resJsonObj.put("commentTxt", after.getString("comment_txt"));
                            resJsonObj.put("op", jsonObject.getString("op"));
                            resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                            resJsonObj.put("create_time", after.getLong("create_time"));
                            resJsonObj.put("user_id", after.getLong("user_id"));
                            resJsonObj.put("sku_id", after.getLong("sku_id"));
                            resJsonObj.put("id", after.getLong("id"));
                            resJsonObj.put("spu_id", after.getLong("spu_id"));
                            resJsonObj.put("order_id", after.getLong("order_id"));
                            resJsonObj.put("dic_name", after.getString("dic_name"));
                            return resJsonObj;
                        }
                        return null;
                    }
                })
                .uid("map_order_comment_data")
                .name("map_order_comment_data");

        // 提取 订单表 有用字段合成一个新的JSON数据
        // 3> {"payment_way":"3501","refundable_time":1747005774000,"original_total_amount":8197.0,"order_status":"1002","consignee_tel":"13314656938","trade_body":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机等1件商品","id":2788,"operate_time":1746401009000,"op":"u","consignee":"茅蓉眉","create_time":1746400974000,"coupon_reduce_amount":0.0,"out_trade_no":"899883552957341","total_amount":8197.0,"user_id":256,"province_id":18,"tm_ms":1746366985879,"activity_reduce_amount":0.0}
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = FilterOrderInfo.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj = new JSONObject();;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map_order_info_data").name("map_order_info_data");

        orderInfoMapDs.print();




        env.execute();
    }
}

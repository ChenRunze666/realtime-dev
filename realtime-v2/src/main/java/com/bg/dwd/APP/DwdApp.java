package com.bg.dwd.APP;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.constant.Constant;
import com.bg.common.util.EnvironmentSettingUtils;
import com.bg.common.util.FlinkSinkUtil;
import com.bg.common.util.FlinkSourceUtil;
import com.bg.dwd.function.FilterBloomDeduplicatorFunc;
import com.bg.dwd.function.BirthdayAgeFunc;
import com.bg.dwd.function.JudgmentFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.bg.dwd.APP.DwdAge
 * @Author Chen.Run.ze
 * @Date 2025/5/12 11:26
 * @description: Dwd层  用户年龄标签
 */
public class DwdApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env,  "dwd_App");

        // TODO 1.从Kafka获取数据
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource("ods_db", "ods_db");
        // 设置水位线
        DataStreamSource<String> kafkaSource = env.fromSource(source,
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
                ), "kafka_source");
        SingleOutputStreamOperator<JSONObject> filterOp = kafkaSource.map(JSONObject::parseObject).filter(o -> !o.getString("op").equals("d"));
        // TODO 2.过滤出 User_info 表
        //{"before":null,"after":{"id":966,"login_name":"i50g1js64rl","nick_name":"真真","passwd":null,"name":"于真","phone_num":"13962311288","email":"i50g1js64rl@126.com","head_img":null,"user_level":"1","birthday":2838,"gender":null,"create_time":1746821342000,"operate_time":null,"status":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024","sequence":null,"table":"user_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1747019493933,"transaction":null}
        SingleOutputStreamOperator<JSONObject> filter = filterOp.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter_user_info").name("filter_user_info");

        // 去重数据
        // {"op":"r","after":{"birthday":7380,"gender":"M","create_time":1744902208000,"login_name":"s40eoj","nick_name":"锦黛","name":"陈善厚","user_level":"1","phone_num":"13699938755","id":510,"email":"s40eoj@hotmail.com","operate_time":1746662400000},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"user_info"},"ts_ms":1747019493918}
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = filter.keyBy(o -> o.getJSONObject("after").getInteger("id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.0001))
                .uid("FilterBloom").name("FilterBloom");
//        bloomFilterDs.print("布隆");

        // 将生日转换成日期格式
        // {"op":"r","after":{"birthday":"1990-03-17","gender":"M","create_time":1744902208000,"login_name":"s40eoj","nick_name":"锦黛","name":"陈善厚","user_level":"1","phone_num":"13699938755","id":510,"email":"s40eoj@hotmail.com","operate_time":1746662400000},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"user_info"},"ts_ms":1747019493918}
        SingleOutputStreamOperator<JSONObject> UserInfo = bloomFilterDs.map(o -> {
            JSONObject after = o.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer birthday = after.getInteger("birthday");
                if (birthday != null) {
                    LocalDate date = LocalDate.ofEpochDay(birthday);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                }
            }
            return o;
        }).uid("convert_birthday").name("convert_birthday");

        // TODO 3.提取字段
        //AgeDS: {"birthday":"1984-04-18","create_time":1744990166000,"gender":"F","constellation":"白羊座","Era":"1980年代","name":"卫萍","id":565,"ageGroup":"40-49","ts_ms":1747019493920,"age":41}
        SingleOutputStreamOperator<JSONObject> AgeDS = UserInfo.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject object = new JSONObject();

                if (jsonObject.getJSONObject("after") != null) {
                    Long tsMs = jsonObject.getLong("ts_ms");
                    Integer id = jsonObject.getJSONObject("after").getInteger("id");
                    String birthday = jsonObject.getJSONObject("after").getString("birthday");
                    String gender = jsonObject.getJSONObject("after").getString("gender");
                    String name = jsonObject.getJSONObject("after").getString("name");

                    object.put("id", id);
                    object.put("ts_ms", tsMs);
                    object.put("name", name);
                    object.put("birthday", birthday);

                    //性别
                    if (gender != null){
                        object.put("gender", gender);
                    }else {
                        object.put("gender", "home");
                    }

                    //年龄
                    int age = BirthdayAgeFunc.calculateAge(birthday);
                    object.put("age", age);

                    // 年龄段
                    object.put("ageGroup", JudgmentFunc.ageJudgment(age));

                    //年代
                    object.put("Era", JudgmentFunc.EraJudgment(birthday));

                    //星座
                    object.put("constellation", JudgmentFunc.ConstellationJudgment(birthday));

                }
                return object;
            }
        });
//        AgeDS.print("AgeDS");

        //TODO 4.过滤 身高体重表
        //{"op":"r","after":{"uid":245,"flag":"change","unit_height":"cm","create_ts":1747043547000,"weight":"50","id":245,"unit_weight":"kg","height":"179"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"user_info_sup_msg"},"ts_ms":1747019494539}
        SingleOutputStreamOperator<JSONObject> weightHeightInfo = filterOp.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter_user_info_sup_msg").name("filter_user_info_sup_msg");

        //TODO 5.关联身高体重
        //{"birthday":"1983-03-09","create_time":1746832594000,"gender":"home","weight":"55","ageGroup":"40-49","constellation":"双鱼座","unit_height":"cm","Era":"1980年代","name":"卫欣","id":982,"unit_weight":"kg","ts_ms":1747019493934,"age":42,"height":"184"}
        SingleOutputStreamOperator<JSONObject> IntervalDS = AgeDS
                .keyBy(o -> o.getInteger("id"))
                .intervalJoin(weightHeightInfo.keyBy(o -> o.getJSONObject("after").getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {
                        JSONObject after = jsonObject2.getJSONObject("after");

                            jsonObject.put("height", after.getString("height"));
                            jsonObject.put("unit_height", after.getString("unit_height"));
                            jsonObject.put("weight", after.getString("weight"));
                            jsonObject.put("unit_weight", after.getString("unit_weight"));

                            collector.collect(jsonObject);
                        }

                })
                .uid("intervalJoin")
                .name("intervalJoin");

        IntervalDS.print();

        //TODO 6.写入Kafka
        IntervalDS.map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.Topic_dwd_App));

        env.execute("DwdApp");
    }
}

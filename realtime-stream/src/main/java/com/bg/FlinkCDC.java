package com.bg;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.bg.FlinkCDC
 * @Author Chen.Run.ze
 * @Date 2025/4/7 19:31
 * @description: Flink CDC
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        // 创建 Properties 对象，用于配置 Debezium 相关属性
        Properties prop = new Properties();
        // 不使用 SSL 连接
        prop.put("useSSL","false");
        // 小数处理模式设置为使用 double 类型
        prop.put("decimal.handling.mode","double");
        // 时间精度模式设置为 connect
        prop.put("time.precision.mode","connect");
        // // 指定分块键列，用于增量快照的分块，这里使用 id 列
        prop.setProperty("scan.incremental.snapshot.chunk.key-column", "id");

        //构建 MySQL 数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                // MySQL 主机名
                .hostname("cdh03")
                // MySQL 端口号
                .port(3306)
                // 要捕获的数据库列表，这里指定为 gmall2024
                .databaseList("gmall2024") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                // 要捕获的表列表，这里指定为 gmall2024 下的所有表
                .tableList("gmall2024.*") // 设置捕获的表
                // MySQL 用户名
                .username("root")
                // MySQL 密码
                .password("root")
                // 设置 Debezium 属性
                .debeziumProperties(prop)
                // 启动选项，从最新位点开始捕获数据
                .startupOptions(StartupOptions.latest()) // 从最早位点启动
                // 反序列化器，将 SourceRecord 转换为 JSON 字符串
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        // 获取 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为 1
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        // 从 MySQL 数据源创建数据流
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        {"before":null,"after":{"id":389,"order_id":191,"order_status":"1002","create_time":1744061980000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1744033180000,"snapshot":"false","db":"gmall2024","sequence":null,"table":"order_status_log","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":5083465,"row":0,"thread":280,"query":null},"op":"c","ts_ms":1744077706871,"transaction":null}
        mySQLSource.print();

        // 构建 Kafka 下沉器
        KafkaSink<String> sink = KafkaSink.<String>builder()
                // Kafka 服务器地址
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        // Kafka 主题名
                        .setTopic("topic_db")
                        // 值序列化器，使用简单字符串序列化器
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // 设置消息传递保证为至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 将从 MySQL 捕获的数据发送到 Kafka
        mySQLSource.sinkTo(sink);

        // 执行 Flink 作业
        env.execute("Print MySQL Snapshot + Binlog");
    }
}

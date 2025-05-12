package com.bg.ods;

import com.bg.common.constant.Constant;
import com.bg.common.util.FlinkSinkUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.bg.ods.FlinkCDC
 * @Author Chen.Run.ze
 * @Date 2025/5/6 22:35
 * @description: Flink CDC 读取Mysql 数据
 */
public class Flink_CDC {
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        // 不使用 SSL 连接
        prop.put("useSSL","false");
        // 小数处理模式设置为使用 double 类型
        prop.put("decimal.handling.mode","double");
        // 时间精度模式设置为 connect
        prop.put("time.precision.mode","connect");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall2024") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("gmall2024.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(prop)
                // 启动选项，从最新位点开始捕获数据
                .startupOptions(StartupOptions.initial()) // 从最早位点启动
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();

        mySQLSource.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.Topic_ods_db));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

package com.bg.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bg.common.bean.TableProcessDim;
import com.bg.common.constant.Constant;
import com.bg.common.function.HBaseSinkFunction;
import com.bg.common.function.TableProcessFunction;
import com.bg.common.util.EnvironmentSettingUtils;
import com.bg.common.util.FlinkSourceUtil;
import com.bg.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.*;

/**
 * @Package com.bg.dim.DimApp
 * @Author Chen.Run.ze
 * @Date 2025/5/12 8:59
 * @description: Dim 维度表
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env,  "dim_app");
        env.setParallelism(4);

        // TODO 1.从Kafka 读取数据
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(Constant.Topic_ods_db, Constant.Topic_ods_db);
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(1);

        // TODO 2.处理数据
        SingleOutputStreamOperator<JSONObject> JsonObject = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject object = JSON.parseObject(s);
                String db = object.getJSONObject("source").getString("db");
                String op = object.getString("op");
                String after = object.getString("after");
                if ("gmall2024".equals(db) &&
                        ("c".equals(op) || "u".equals(op) || "d".equals(op) || "r".equals(op))
                        && after != null && after.length() > 2) {
                    collector.collect(object);
                }
            }
        }).uid("JsonObject").name("JsonObject");

        // TODO 3.读取配置信息表中配置信息
        //创建MysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall2024_config", "table_process_dim");
        // 读取数据 封装为流
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);
        // 引配置流中的数据类型进行转换 json->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mySQLSource.map((MapFunction<String, TableProcessDim>) s -> {
            JSONObject object = JSON.parseObject(s);
            String op = object.getString("op");
            TableProcessDim tableProcessDim;
            if ("d".equals(op)) {
                //对配置表进行一次删除操作  从before属性中获得删除前的配置信息
                tableProcessDim = object.getObject("before", TableProcessDim.class);
            } else {
                //对配置表进行了读取、添加、修改操作 从after属性中获取最新的配置信息
                tableProcessDim = object.getObject("after", TableProcessDim.class);
            }
            tableProcessDim.setOp(op);
            return tableProcessDim;
        }).setParallelism(1).uid("tpDS").name("tpDS");

        // TODO 4.根据配置表中的配置信息到 HBase 中执行建表或者删除表操作
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection HbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                HbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(HbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                //获取对配置表获取的操作类型
                String op = tp.getOp();
                //获取HBase中维度表表明
                String sinkTable = tp.getSinkTable();
                //获取在HBase中建表的列族
                String[] sinkFamilies = tp.getSinkFamily().split(",");
                if ("d".equals(op)){
                    //从配置表中删除了一条数据 将HBase中对应的表删除
                    HBaseUtil.dropHBaseTable(HbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                }else if ("r".equals(op) || "c".equals(op)){
                    //从配置表中读取了一条数据或者添加了一条配置
                    HBaseUtil.createHBaseTable(HbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }else {
                    //从配置表中的配置进行了修改   先把HBase中对应的表删除再创建新表
                    HBaseUtil.dropHBaseTable(HbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                    HBaseUtil.createHBaseTable(HbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                }
                return tp;
            }
        }).setParallelism(1).uid("HBaseFunction").name("HBaseFunction");

        // TODO 5.广播流过滤出维度数据
        // 配置流中的配置信息进行广播--broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcast = tpDS.broadcast(mapStateDescriptor);
        // 将主流业务数据和广播流配置信息进行关联--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connect = JsonObject.connect(broadcast);
        // 处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect.process(
                new TableProcessFunction(mapStateDescriptor)).setParallelism(1).uid("DimConnect").name("DimConnect");

        dimDS.print("dimDS-->");
        // TODO 6.将维度数据同步到HBase表中
        dimDS.addSink(new HBaseSinkFunction());


        env.execute("DimApp");
    }

}

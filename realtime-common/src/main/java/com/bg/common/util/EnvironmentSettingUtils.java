package com.bg.common.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.bg.common.base.defaultParameter
 * @Author Chen.Run.ze
 * @Date 2025/5/12 9:07
 * @description: 环境默认配置
 */
public class EnvironmentSettingUtils {
    public static void defaultParameter(StreamExecutionEnvironment env){
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        //2.6 设置状态后端以及检查点存储路径
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck" + ckAndGroupId);
        //2.7 设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","root");
    }
}

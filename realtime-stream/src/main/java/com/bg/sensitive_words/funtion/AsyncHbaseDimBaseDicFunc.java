package com.bg.sensitive_words.funtion;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSONObject;
import com.bg.sensitive_words.untis.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.bg.sensitive_words.funtion.AsyncHbaseDimBaseDicFunc
 * @Author Chen.Run.ze
 * @Date 2025/3/15 20:47
 * @description: Async DimBaseDic
 */
public class AsyncHbaseDimBaseDicFunc extends RichAsyncFunction<JSONObject,JSONObject> {

    private transient Connection hbaseConn;
    private transient Table dimTable;
    // 缓存：RowKey -> dic_name
    private transient Cache<String, String> cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = new HbaseUtils("cdh01:2181,cdh02:2181,cdh03:2181").getConnection();
        dimTable = hbaseConn.getTable(TableName.valueOf("gmall2024:dim_base_dic"));
//        System.out.println(dimTable);
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
//        System.out.println(cache);
        super.open(parameters);
    }


    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        // 获取 appraise
        String appraise = input.getJSONObject("after").getString("appraise");
        // 对row进行加密
        // 散列原则
//        String rowKey = MD5Hash.getMD5AsHex(appraise.getBytes(StandardCharsets.UTF_8));
//        //7501e5d4da87ac39d782741cd794002d
//        System.out.println(rowKey);
//        String cachedDicName = cache.getIfPresent(rowKey);
        // null
//        System.out.println(cachedDicName);
        if (appraise != null) {
            enrichAndEmit(input, appraise, resultFuture);
        }
        CompletableFuture.supplyAsync(() -> {
            Get get = new Get(appraise.getBytes(StandardCharsets.UTF_8));
//            System.out.println(get);
            try {
                Result result = dimTable.get(get);
//                System.out.println(result);
                if (result.isEmpty()) {
                    return null;
                }

                return Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dic_name")));
            } catch (IOException e) {
                throw new RuntimeException("Class: AsyncHbaseDimBaseDicFunc Line 66 HBase query failed ! ! !",e);
            }
        }).thenAccept(dicName -> {
//            System.out.println(dicName);
            if (dicName != null) {
                cache.put(appraise, dicName);
                enrichAndEmit(input, dicName, resultFuture);
            }else {
                enrichAndEmit(input, "N/A", resultFuture);
            }
        });
    }

    private void enrichAndEmit(JSONObject input, String dicName, ResultFuture<JSONObject> resultFuture) {
        JSONObject after = input.getJSONObject("after");
        after.put("dic_name", dicName);
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }

    @Override
    public void close() throws Exception {
        try {
            if (dimTable != null) dimTable.close();
            if (hbaseConn != null) hbaseConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }
}

package org.sqkb.consumer.stid2hbase;


import com.alibaba.fastjson.JSONObject;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.fire.spark.streaming.core.plugins.redis.RedisEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqkb.service.common.kit.IsvCode;
import org.sqkb.utils.HbaseUtil;
import org.sqkb.utils.MyRedisUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ThreadHandle extends Thread {

    private static Logger log = LoggerFactory.getLogger(ThreadHandle.class);

    private ConsumerRecords<String, String> consumerRecords;

    private final String TABLE1 = "stid_mapping_v1_test";

    private HTable hTable1;

    private final String TABLE2 = "unid_test";

    private HTable hTable2;
    int flag =0;

    public ThreadHandle(ConsumerRecords<String, String> records) throws Exception {
        this.hTable1 = new HTable(HbaseUtil.getConf(), TABLE1);
        this.hTable2 = new HTable(HbaseUtil.getConf(), TABLE2);
        this.consumerRecords = records;
    }

    @Override
    public void run() {
        try {
            handle(consumerRecords);
            System.out.println("flag======="+flag);
            this.release();
        } catch (Exception e) {
            log.info("插入失败！！！" + e);
            e.printStackTrace();
        }
    }

    public List<Put> handle(ConsumerRecords<String, String> consumerRecords) throws Exception {
        List<Put> list = new ArrayList<>();
        Jedis jedis = MyRedisUtil.getJedis();
        try {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String[] arr = record.key().split(":");
                // 处理stid
                if (arr.length == 3 && arr[1].equals("stid")) {
                    JSONObject jsonObject = JSONObject.parseObject(record.value());
                    String server_name = jsonObject.getString("server_name");
                    String server_time = jsonObject.getString("server_time");
                    String client_ip = jsonObject.getString("client_ip");
                    String biz_name = jsonObject.getString("biz_name");
                    JSONObject biz_info1 = jsonObject.getJSONObject("biz_info");
                    biz_info1.put("stid", arr[2]);
                    String biz_info = jsonObject.getString("biz_info");
                    String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(arr[2])).substring(0, 6) + arr[2];

                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("server_name"), Bytes.toBytes(server_name));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("server_time"), Bytes.toBytes(server_time));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_ip"), Bytes.toBytes(client_ip));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("biz_name"), Bytes.toBytes(biz_name));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("biz_info"), Bytes.toBytes(biz_info));
                    hTable1.put(put);

                    // redis 处理
                    String bkey = jedis.get("tu" + arr[2]);
                    if (bkey == null) {
                        String _bkey = IsvCode.getBkey(arr[2]);
                        String setex = jedis.setex("tu" + arr[2], 3600, _bkey);
                        if(setex.equals("OK")){
                            flag++;
                        }
                    }
                }

                // 处理unid
                if (arr.length == 3 && arr[1].equals("unid")) {
                    String id = "unid:" + arr[2];
                    String rowKey = MD5Hash.getMD5AsHex(id.getBytes()).substring(0, 4) + ":" + id;
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("log_info"), Bytes.toBytes(record.value()));
                    hTable2.put(put);
                    flag++;
                }
            }
        }
        catch (Exception e){
            log.error("error => ",e);
        }
        finally {
            MyRedisUtil.returnJedisOjbect(jedis);
        }

        return list;
    }

    public void release() throws IOException {
        if (consumerRecords != null) {
            consumerRecords = null;
        }
        if (hTable1 != null) {
            hTable1.close();
            hTable2 = null;
        }
        if (hTable2 != null) {
            hTable2.close();
            hTable2 = null;
        }
    }

}

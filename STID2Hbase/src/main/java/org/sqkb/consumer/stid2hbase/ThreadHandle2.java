package org.sqkb.consumer.stid2hbase;


import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqkb.service.common.kit.DateKit;
import org.sqkb.service.common.kit.IsvCode;
import org.sqkb.utils.HbaseUtil;
import org.sqkb.utils.MyRedisUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ThreadHandle2 extends Thread {

    private static Logger log = LoggerFactory.getLogger(ThreadHandle2.class);

    private ConsumerRecords<String, String> consumerRecords;


    private final String TABLE2 = "unid_test";

    private HTable hTable2;


    private int flag = 0;

    public ThreadHandle2(ConsumerRecords<String, String> records) throws Exception {
        this.hTable2 = new HTable(HbaseUtil.getConf(), TABLE2);
        this.consumerRecords = records;
    }

    @Override
    public void run() {
        try {
            handle(consumerRecords);
            this.release();
        } catch (Exception e) {
            log.info("插入失败！！！" + e);
            e.printStackTrace();
        }
    }

    public void handle(ConsumerRecords<String, String> consumerRecords) throws Exception {
        List<Put> list = new ArrayList<>();
        Jedis jedis = MyRedisUtil.getJedis();
        try {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String[] arr = record.key().split(":");
                // 处理unid
                String id = "unid:" + arr[2];
                String rowKey = MD5Hash.getMD5AsHex(id.getBytes()).substring(0, 4) + ":" + id;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("log_info"), Bytes.toBytes(record.value()));
                ++flag;
                list.add(put);
                String key = "unid";
                String filed = DateKit.now("yyyyMMddHH");
                jedis.hincrBy(key,filed,1);
            }
            System.out.println("flag=========="+flag);
            hTable2.put(list);
        } catch (Exception e) {
            log.error("error => ", e);
        } finally {
            MyRedisUtil.returnJedisOjbect(jedis);
        }
    }


    public void release() throws IOException {
        if (consumerRecords != null) {
            consumerRecords = null;
        }
        if (hTable2 != null) {
            hTable2.close();
            hTable2 = null;
        }
    }

}

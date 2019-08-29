package org.sqkb.consumers.applogtohbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqkb.utils.HbaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Applog2HbaseHandle extends Thread {

    private static Logger log = LoggerFactory.getLogger(Applog2HbaseHandle.class);

    private ConsumerRecords<String, String> consumerRecord;

    private final String TABLE = "spark_applog_day";

    private HTable hTable;

    private final byte[] cfB = "cf".getBytes();

    private final byte[] logB = "log".getBytes();


    public Applog2HbaseHandle(ConsumerRecords<String, String> records) throws Exception {
        this.hTable = new HTable(HbaseUtil.getConf(), TABLE);
        this.consumerRecord = records;
    }

    @Override
    public void run() {
        try {
            List<Put> puts = new ArrayList<>();
            for (ConsumerRecord<String, String> record : consumerRecord) {
                String rowkey = null;
                try {
                    rowkey = flatJson(record.value());
                } catch (Exception e) {
                    log.error(record.value(), e);
                }
                if (rowkey != null) {
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(cfB, logB, Bytes.toBytes(record.value()));
                    puts.add(put);
                }
               // log.info("Consumer Message:---ThreadName:{},Partition:{},Offset:{}",Thread.currentThread().getName(),record.partition(),record.offset());
            }
            this.hTable.put(puts);
            this.release();
        } catch (Exception e) {
            System.out.println("插入失败！！！");
            e.printStackTrace();
        }
    }


    public static String flatJson(String record) {
        JSONObject jsonObject = JSONObject.parseObject(record);
        String device_id = jsonObject.getString("device_id");
        String event_type = jsonObject.getString("event_type");
        String event_name = jsonObject.getString("event_name");
        long server_time = jsonObject.getLong("server_time");
        long a = Long.MAX_VALUE - server_time;
        if (device_id == null || device_id.equals("")) {
            return "error#"+a;
        }
        return device_id + "#" + event_type + "#" + event_name + "#" + a;
    }


    public void release() throws IOException {
        if (consumerRecord != null) {
            consumerRecord = null;
        }
        if (hTable != null) {
            hTable.close();
            hTable = null;
        }
    }
}

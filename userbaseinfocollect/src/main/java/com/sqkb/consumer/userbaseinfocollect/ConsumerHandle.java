package com.sqkb.consumer.userbaseinfocollect;


import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqkb.utils.HbaseUtil;
import org.sqkb.utils.HttpUtil;
import java.io.IOException;
import java.util.*;

public class ConsumerHandle extends Thread {

    private static Logger log = LoggerFactory.getLogger(ConsumerHandle.class);
    private ConsumerRecords<String, String> consumerRecord;
    private final String TABLE = "user_location_v1";
    private HTable hTable;

    public ConsumerHandle(ConsumerRecords<String, String> records) throws Exception {
        this.hTable = new HTable(HbaseUtil.getConf(), TABLE);
        this.consumerRecord = records;
    }

    @Override
    public void run() {
        try {
            Set<String> set = new HashSet();
            for (ConsumerRecord<String, String> record : consumerRecord) {
                String lnglat = flatJson(record.value());
                //去重
                set.add(lnglat);
            }
            //遍历set
            List<Put> puts = handle(set);
            this.hTable.put(puts);
            this.release();
            puts.clear();
            set.clear();
        } catch (Exception e) {
            log.info("插入失败！！！"+e);
            e.printStackTrace();
        }
    }

    public String flatJson(String record) {
        JSONObject jsonObject = JSONObject.parseObject(record);
        String lng = new Formatter().format("%.4f", jsonObject.getDouble("lng")).toString();
        String lat = new Formatter().format("%.4f", jsonObject.getDouble("lat")).toString();
        return lng + "#" + lat;
    }

    public List<Put> handle(Set<String> set) throws Exception {
        String uri = "https://restapi.amap.com/v3/geocode/regeo?output=json&location=%s,%s&key=675e1c4c4f2281017a4d2930d4b10ff9&radius=100&extensions=all";
        List<Put> puts = new ArrayList<>();
        byte[] rowKey;
        for (String s : set) {
            String[] split = s.split("#");
            String lngr = new StringBuilder(split[0]).reverse().toString();
            String latr = new StringBuilder(split[1]).reverse().toString();
            rowKey = Bytes.toBytes(lngr + "#" + latr);
            if (!hTable.exists(new Get(rowKey))) {
                String body = httpGet(uri, split[0], split[1]);
                System.out.println(body);
                JSONObject jsonObject = JSONObject.parseObject(body);
                if (jsonObject.getInteger("infocode") == 10000) {
                    Put put = new Put(rowKey);
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ln"), Bytes.toBytes(body));
                    puts.add(put);
                }
            }
        }
        return puts;
    }

    public String httpGet(String uri, String lng, String lat) {
        String format = String.format(uri, lng, lat);
        String json = HttpUtil.get(format);
        return json;
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

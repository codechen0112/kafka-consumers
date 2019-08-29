package com.sqkb.comsumer.addclick;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqkb.service.common.kit.DateKit;

import java.util.List;


public class AddClickHandle extends Thread {
    private int ttl = 24 * 60 * 60 * 2;

    private static Logger log = LoggerFactory.getLogger(AddClickHandle.class);

    private ConsumerRecords<String, String> consumerRecords;

    public AddClickHandle(ConsumerRecords<String, String> records) throws Exception {
        this.consumerRecords = records;
    }


    @Override
    public void run() {
        for (ConsumerRecord<String, String> record : consumerRecords) {
            handle(record);
        }
    }

    private void handle(ConsumerRecord<String, String> record) {
        JSONObject jsonObject = JSONObject.parseObject(record.value());
        String create_time = jsonObject.getString("create_time");
        if (create_time == null) create_time = DateKit.now("yyyy-MM-dd HH:mm:ss");
        String device_type = jsonObject.getString("device_type");
        String device_md5 = jsonObject.getString("device_md5");
        String ad_type = jsonObject.getString("ad_type");
        List<String> coupon_type = jsonObject.getJSONArray("coupon_type").toJavaList(String.class);
        List<String> coupon_id = jsonObject.getJSONArray("coupon_id").toJavaList(String.class);

        if (!coupon_type.isEmpty()) {
            String s = "{\"ad_type\":"+ad_type+",\"coupon_type\":" +coupon_type.toString()+ ",\"coupon_id\":" +coupon_id.toString()+"}";

            System.out.println(s);
        }


    }

    private void saveToRedis() {

    }


}

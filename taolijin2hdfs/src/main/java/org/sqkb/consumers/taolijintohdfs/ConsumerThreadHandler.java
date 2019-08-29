package org.sqkb.consumers.taolijintohdfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 阿土
 *
 * 逻辑处理
 */
public class ConsumerThreadHandler implements Runnable {

    private static Logger log = LoggerFactory.getLogger(ConsumerThreadHandler.class);

    private KafkaConsumer<String, String> consumer;

    public ConsumerThreadHandler(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }
    @Override
    public void run() {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        while (true) {
            try {
                FileSystem fs = FileSystem.get(configuration);
                String ymd = getDate("yyyyMMdd");
                String ymdh = getDate("yyyyMMddHH");
                String name = Thread.currentThread().getName();
                Path path = new Path("/user/hive/external/logs.db/taolijin_alimama_top_message/ymd=" + ymd
                        + "/alimama_top_message_" + name +"_"+ ymdh);
                if (!fs.exists(path)) {
                    fs.createNewFile(path);
                }
                FSDataOutputStream stream = fs.append(path);
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    JSONObject jsonObject = JSONObject.parseObject(consumerRecord.value());
                    String msg = flatJson(jsonObject);
                    stream.writeBytes(msg);
                    stream.hsync();
                   log.info("Consumer Message:"+Thread.currentThread().getName() + consumerRecord.value() + ",Partition:" + consumerRecord.partition() + " Offset:" + consumerRecord.offset());
                }
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getDate(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        String date = format.format(new Date());
        return date;
    }


    public static String flatJson(JSONObject jsonObject) {
        JSONObject content = jsonObject.getJSONObject("content");
        JSONObject msgBody = content.getJSONObject("msgBody");
        String rightsRecordId = msgBody.getString("rightsRecordId");
        String unid = msgBody.getString("unid");
        String sendSuccessTime = msgBody.getString("sendSuccessTime");
        String tbTradeMap = msgBody.getString("tbTradeMap");             //todo{}
        String rightsId = msgBody.getString("rightsId");
        String msgId = content.getString("msgId");
        String msgType = content.getString("msgType");
        String id = jsonObject.getString("id");
        String outgoingTime = jsonObject.getString("outgoingTime");
        String pubAppKey = jsonObject.getString("pubAppKey");
        String pubTime = jsonObject.getString("pubTime");
        JSONObject raw = jsonObject.getJSONObject("raw");
        String retried = raw.getString("retried");
        String kind = raw.getString("__kind");
        String notify = raw.getString("notify");
        String topic = jsonObject.getString("topic");

        return rightsRecordId + "\t" + unid + "\t" + sendSuccessTime + "\t" + tbTradeMap + "\t" + rightsId + "\t" + msgId + "\t" + msgType + "\t" + id + "\t"
                + outgoingTime + "\t" + pubAppKey + "\t" + pubTime + "\t" + retried + "\t" + kind + "\t" + notify + "\t" + topic + "\n";
    }

}

package org.sqkb.consumers.taolijintohdfs;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 阿土
 *
 * 创建线程
 */

public class ConsumerThread {

    private ExecutorService executor;
    String propsName;

    public ConsumerThread(String propsName) throws Exception {
        this.propsName = propsName;
    }

    public void start(int threadNumber) throws Exception {
     executor = Executors.newFixedThreadPool(threadNumber);
        for (int i = 0;i < threadNumber;i++){
            Properties properties = buildKafkaProperty(propsName);
            KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
            consumer.subscribe(Arrays.asList(properties.getProperty("topic").split(",")));
            executor.submit(new ConsumerThreadHandler(consumer));
        }
    }

    private static Properties buildKafkaProperty(String propsName) throws Exception {
        Properties properties = new Properties();
        properties.load(ConsumerThread.class.getClassLoader().getResourceAsStream(propsName));
        return properties;
    }

}

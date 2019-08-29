package com.sqkb.comsumer.addclick;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by 阿土 20190827
 * <p>
 * 创建线程
 */
public class AddClickThread {

    private ExecutorService executor;

    private final KafkaConsumer<String, String> consumer;
    private int threadNumber;


    public AddClickThread(String propsName, int threadNumber) throws Exception {
        this.threadNumber = threadNumber;
        Properties properties = buildKafkaProperty(propsName);
        consumer = new KafkaConsumer(properties);
        consumer.subscribe(Arrays.asList(properties.getProperty("topic").split(",")));
        executor = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(threadNumber), new ThreadPoolExecutor.CallerRunsPolicy());
    }


    public void start() throws Exception {
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(10000);
            if (!consumerRecords.isEmpty()) {
                executor.submit(new AddClickHandle(consumerRecords));
            }
        }
    }


    private static Properties buildKafkaProperty(String propsName) throws Exception {
        Properties properties = new Properties();
        properties.load(AddClickThread.class.getClassLoader().getResourceAsStream(propsName));
        return properties;
    }


}

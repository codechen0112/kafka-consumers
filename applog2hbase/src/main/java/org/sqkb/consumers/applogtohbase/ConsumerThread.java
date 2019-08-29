package org.sqkb.consumers.applogtohbase;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by 阿土
 * <p>
 * 创建线程
 */

public class ConsumerThread {

    private ExecutorService executor;

    private final KafkaConsumer<String, String> consumer;
    private int threadNumber;


    public ConsumerThread(String propsName, int threadNumber) throws Exception {
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
                executor.submit(new Applog2HbaseHandle(consumerRecords));
            }
        }
    }


    private static Properties buildKafkaProperty(String propsName) throws Exception {
        Properties properties = new Properties();
        properties.load(ConsumerThread.class.getClassLoader().getResourceAsStream(propsName));
        return properties;
    }

}

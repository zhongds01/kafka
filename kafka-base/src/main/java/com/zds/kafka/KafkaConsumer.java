package com.zds.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * KafkaConsumer
 *
 * @author zhongdongsheng
 * @since 2021/12/19
 */
public class KafkaConsumer {
    private static final String TOPIC_NAME = "topic-base";

    private static final String CONSUMER_GROUP = "topic-base-group";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.118:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> kafkaConsumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("收到消息：[topic:" + record.topic() + "] [partition:"
                        + record.partition() + "] [offset:" + record.offset() + "] [key:" +
                        record.key() + "] [value:" + record.value() + "]");
            }
        }
    }
}

package com.zds.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * KafkaProducer
 *
 * @author zhongdongsheng
 * @since 2021/12/19
 */
public class KafkaProducer {
    private static final String TOPIC_NAME = "topic-base";

    public static void main(String[] args) throws JsonProcessingException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.118:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // ack = 0 kafka集群不需要任何broker接收消息，直接返回ack。容易丢消息，但是效率高。
        // ack = 1 leader收到消息后，并将消息写入broker中，才会返回ack (默认就是1)
        // ack = -1/all 看min.insync.replicas=2配置，
        // 控制kafka集群返回ack给客户端的时机
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        // 发送失败重试
        properties.put(ProducerConfig.RETRIES_CONFIG, 2);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // 发送消息的客户端
        Producer<String, String> producer =
                new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        // 发送消息

        KafkaEntity entity = KafkaEntity.builder().id(1L).name("kafkaEntity").build();
        ObjectMapper mapper = new ObjectMapper();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "myKey",
                mapper.writeValueAsString(entity));
        // 同步发送
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = producer.send(producerRecord).get();
            System.out.println("同步发送消息结果：[topic:" + recordMetadata.topic() + "] [partition:"
                    + recordMetadata.partition() + "] [offset:" + recordMetadata.offset() + "] [timestamp:" +
                    recordMetadata.timestamp() + "]");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // 异步发送
        // producer.send(producerRecord, new Callback() {
        //     @Override
        //     public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        //         System.out.println("异步发送消息结果：[topic:" + recordMetadata.topic() + "] [partition:"
        //                 + recordMetadata.partition() + "] [offset:" + recordMetadata.offset() + "] [timestamp:" + recordMetadata.timestamp() + "]");
        //     }
        // });
    }
}

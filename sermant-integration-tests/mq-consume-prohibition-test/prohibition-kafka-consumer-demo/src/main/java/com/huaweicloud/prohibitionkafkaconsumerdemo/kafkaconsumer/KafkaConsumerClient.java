package com.huaweicloud.prohibitionkafkaconsumerdemo.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerClient.class);

    private KafkaConsumer<String, String> consumer;

    public void init(String kafkaAddress, Collection<String> topics) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaAddress);
        properties.put("group.id", "my-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
        System.out.println("consumer.listTopics():"+consumer.listTopics());
        System.out.println("consumer.assignment():"+consumer.assignment());
        System.out.println("consumer.groupMetadata():"+consumer.groupMetadata());
        System.out.println("consumer.metrics():"+consumer.metrics());
        System.out.println("consumer.paused():"+consumer.paused());
        System.out.println("consumer.subscription():"+consumer.subscription());
        System.out.println("consumer.partitionsFor():"+consumer.partitionsFor("abc"));

    }

    public List<String> consume() {
        ConsumerRecords<String, String> records = consumer.poll(100);
        List<String> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            LOGGER.info("receive the msg:" + record.value());
            result.add(record.value());
        }
        return result;
    }

    public void run() {
        while (true) {
            System.out.println("consumer start!");
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
                System.out.println("sleep err");
            }
            consume();
        }
    }

    public void close() {
        consumer.close();
    }
}

package com.huaweicloud.prohibitionkafkaproducerdemo.kafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerClient {
    private Producer<String, String> producer;

    private String topic;

    public void init(String kafkaAddress, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaAddress);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.topic = topic;
        producer = new KafkaProducer<>(properties);
    }

    public boolean produce(String msg) {
        if (producer != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                }
            });
            return true;
        } else {
            return false;
        }
    }

    public void run() {
        while (true) {
            System.out.println("produce start!");
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
                System.out.println("sleep err");
            }
            produce(topic+"-msg");
        }
    }

    public void close() {
        producer.close();
    }
}

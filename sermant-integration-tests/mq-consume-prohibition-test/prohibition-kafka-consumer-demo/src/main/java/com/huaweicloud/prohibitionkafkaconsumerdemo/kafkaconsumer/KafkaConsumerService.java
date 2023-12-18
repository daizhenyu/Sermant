package com.huaweicloud.prohibitionkafkaconsumerdemo.kafkaconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final Map<String, KafkaConsumerClient> consumerClientMap = new HashMap<>();

    private final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(5, 5, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100));

    public KafkaConsumerClient initKafkaConsumer(String address, String name, Collection<String> topics) {
        if (consumerClientMap.containsKey(name)) {
            return consumerClientMap.get(name);
        }
        KafkaConsumerClient consumer = new KafkaConsumerClient();
        consumer.init(address, topics);
        consumerClientMap.put(name, consumer);
        return consumer;
    }

    public KafkaConsumerClient getConsumerClient(String name) {
        if (consumerClientMap.containsKey(name)) {
            return consumerClientMap.get(name);
        }
        return null;
    }

    public void runConsumer(String name) {
        KafkaConsumerClient kafkaConsumerClient = getConsumerClient(name);
        if (kafkaConsumerClient != null) {
            threadPoolExecutor.execute(kafkaConsumerClient::run);
            LOGGER.info("run consumer success!");
        } else {
            LOGGER.info("run consumer error!");
        }
    }

    public List<String> consume(String name) {
        KafkaConsumerClient kafkaConsumerClient = getConsumerClient(name);
        if (kafkaConsumerClient != null) {
            return kafkaConsumerClient.consume();
        } else {
            LOGGER.info("kafkaConsumerClient not found in cache!");
            return Collections.emptyList();
        }
    }

    public void close(String name) {
        KafkaConsumerClient kafkaConsumerClient = consumerClientMap.get(name);
        if (kafkaConsumerClient != null) {
            consumerClientMap.remove(name);
            kafkaConsumerClient.close();
        }
    }
}

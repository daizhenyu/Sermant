package com.huaweicloud.prohibitionkafkaproducerdemo.kafkaproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);

    private final Map<String, KafkaProducerClient> producerClientMap = new HashMap<>();
    private final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(5, 5, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100));

    public KafkaProducerClient initKafkaProducer(String address, String name, String topic){
        if (producerClientMap.containsKey(name)){
            return producerClientMap.get(name);
        }
        KafkaProducerClient producer = new KafkaProducerClient();
        producer.init(address, topic);
        producerClientMap.put(name, producer);
        return producer;
    }

    public KafkaProducerClient getProducerClient(String name){
        if (producerClientMap.containsKey(name)){
            return producerClientMap.get(name);
        }
        return null;
    }

    public void startProduce(String name){
        KafkaProducerClient kafkaProducerClient = getProducerClient(name);
        if (kafkaProducerClient!=null){
            threadPoolExecutor.execute(kafkaProducerClient::run);
            LOGGER.info("run produce success!");
        }else {
            LOGGER.info("run produce fail!");
        }
    }

    public boolean produce(String msg, String name){
        KafkaProducerClient kafkaProducerClient = getProducerClient(name);
        if (kafkaProducerClient!=null){
            return kafkaProducerClient.produce(msg);
        }else {
            LOGGER.info("kafkaProducerClient not found in cache!");
            return false;
        }
    }

    public void close(String name){
        KafkaProducerClient kafkaProducerClient = producerClientMap.get(name);
        if (kafkaProducerClient!=null) {
            producerClientMap.remove(name);
            kafkaProducerClient.close();
        }
    }
}

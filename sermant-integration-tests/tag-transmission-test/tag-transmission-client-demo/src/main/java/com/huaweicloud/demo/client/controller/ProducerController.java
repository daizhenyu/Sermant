/*
 * Copyright (C) 2023-2023 Huawei Technologies Co., Ltd. All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.huaweicloud.demo.client.controller;

import com.huaweicloud.demo.client.kafka.consumer.KafkaWithConsumer;
import com.huaweicloud.demo.client.rocketmq.consumer.RocketMqConsumer;
import com.huaweicloud.demo.lib.common.MessageConstant;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 消息中间件生产者controller, 和消费者同一进程
 *
 * @author daizhenyu
 * @since 2023-09-28
 **/
@RestSchema(schemaId = "ProducerController")
@RequestMapping(value = "produce")
public class ProducerController {
    /**
     * roketmq生成一条消息
     *
     * @return string
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    @RequestMapping(value = "rocketmq", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testRocketMqProducer()
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        produceRocketData();
        return "rocketmq-produce-message-success";
    }

    /**
     * kafka生产一条消息
     *
     * @return string
     */
    @RequestMapping(value = "kafka", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testKafkaProducer() {
        produceKafkaData();
        return "kafka-produce-message-success";
    }

    /**
     * 查询kafka消费者消费消息后返回的流量标签透传
     *
     * @return string
     */
    @RequestMapping(value = "queryKafka", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String queryKafkaTag() {
        String trafficTag = KafkaWithConsumer.KAFKA_TAG_MAP.get("kafkaTag");

        // 删除流量标签，以免干扰下一次测试查询
        KafkaWithConsumer.KAFKA_TAG_MAP.remove("kafkaTag");
        return trafficTag;
    }

    /**
     * 查询rocketmq消费者消费消息后返回的流量标签透传
     *
     * @return string
     */
    @RequestMapping(value = "queryRocketmq", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String queryRocketmqTag() {
        String trafficTag = RocketMqConsumer.ROCKETMQ_TAG_MAP.get("rocketmqTag");

        // 删除流量标签，以免干扰下一次测试查询
        RocketMqConsumer.ROCKETMQ_TAG_MAP.remove("rocketmqTag");
        return trafficTag;
    }

    private void produceRocketData()
            throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(MessageConstant.ROCKETMQ_PRODUCE_GROUP);
        producer.setNamesrvAddr(MessageConstant.ROCKETMQ_IP_ADDRESS);
        producer.start();

        String messageBody = buildMessageBody(MessageConstant.MESSAGE_BODY_ROCKET);

        Message message = new Message(MessageConstant.TOPIC, MessageConstant.TAG,
                messageBody.getBytes(StandardCharsets.UTF_8));
        producer.send(message);
        producer.shutdown();
    }

    private void produceKafkaData() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", MessageConstant.KAFKA_IP_ADDRESS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(properties);
        String messageBody = buildMessageBody(MessageConstant.MESSAGE_BODY_KAFKA);
        ProducerRecord<String, String> record = new ProducerRecord<>(MessageConstant.TOPIC, MessageConstant.KAFKA_KEY,
                messageBody);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
            }
        });
        producer.close();
    }

    private String buildMessageBody(String body) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(MessageConstant.TIME_FORMAT);
        String messageBody = body + dtf.format(LocalDateTime.now());
        return messageBody;
    }
}

/*
 * Copyright (C) 2023-2023 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.agentcore.test.request;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 请求测试类，采用http请求调用方式测试
 *
 * @author tangle
 * @since 2023-09-07
 */
public class RequestTest {
    private static final String CONSUMER_1_URL = "http://127.0.0.1:8577";

    private static final String CONSUMER_2_URL = "http://127.0.0.1:8578";

    private static final String PRODUCER_URL = "http://127.0.0.1:8576";

    private static final String KAFKA_ADDRESS = "127.0.0.1:9092";

    private static final String TEST_TOPIC_MSG = "test-topic-msg";

    private static final String TEST_TOPIC_1_MSG = "test-topic-1-msg";

    private static final String TEST_TOPIC_2_MSG = "test-topic-2-msg";

    /**
     * 接口测试
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_TEST")
    public void testTest() throws IOException {
        RequestUtils.doPost(CONSUMER_1_URL + "/kafkaConsumer/newConsumer", new HashMap<String, Object>() {{
            put("address", KAFKA_ADDRESS);
            put("name", "consumer-test");
            put("topics", new String[]{"abc", "def", "grf"});
        }});
    }

    /**
     * 启动时开启消费、单个消费者消费单个topic
     * consumer-1：订阅test-topic
     * producer-1：生成test-topic
     * 正常消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T1C_AllowAll")
    public void test1Topic1ConsumerAllowAll() throws InterruptedException {
        // 新建消费者
        initConsumer1(new String[]{"test-topic"});
        // 新建生产者
        initProducer("producer-1", "test-topic");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertTrue(resultList.size() > 0);
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_MSG));
    }

    /**
     * 启动时开启消费、单个消费者消费多个topic
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 正常消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_AllowAll")
    public void test2Topic1ConsumerAllowAll() throws InterruptedException {
        // 新建消费者
        initConsumer1(new String[]{"test-topic-1", "test-topic-2"});
        // 新建生产者1
        initProducer("producer-1", "test-topic-1");
        // 新建生产者2
        initProducer("producer-2", "test-topic-2");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");
        startProduce("producer-2");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertTrue(resultList.size() > 0);
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_1_MSG));
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_2_MSG));
    }

    /**
     * 启动时禁止消费、单个消费者消费单个topic
     * consumer-1：订阅test-topic
     * producer-1：生成test-topic
     * 不消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T1C_ProhibitAll")
    public void test1Topic1ConsumerProhibitAll() throws InterruptedException {
        // 新建消费者
        initConsumer1(new String[]{"test-topic"});
        // 新建生产者
        initProducer("producer-1", "test-topic");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertEquals(0, resultList.size());
    }

    /**
     * 启动时禁止消费、单个消费者消费多个topic
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 不消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_ProhibitAll")
    public void test2Topic1ConsumerProhibitAll() throws InterruptedException {
        // 新建消费者
        initConsumer1(new String[]{"test-topic-1", "test-topic-2"});

        // 新建生产者1
        initProducer("producer-1", "test-topic-1");

        // 新建生产者2
        initProducer("producer-2", "test-topic-2");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");
        startProduce("producer-2");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertEquals(0, resultList.size());
    }

    /**
     * 启动时禁止消费一个topic、单个消费者消费多个topic
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 不消费test-topic-1判断
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_ProhibitTopicOne")
    public void test2Topic1ConsumerProhibitTopicOne() throws InterruptedException {
        // 新建消费者
        initConsumer1(new String[]{"test-topic-1", "test-topic-2"});

        // 新建生产者1
        initProducer("producer-1", "test-topic-1");

        // 新建生产者2
        initProducer("producer-2", "test-topic-2");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");
        startProduce("producer-2");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        // 不含topic1的消息
        Assertions.assertFalse(resultList.contains(TEST_TOPIC_1_MSG));
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_2_MSG));
    }

    /**
     * 启动时禁止消费一个topic、多个消费者消费单个topic
     * consumer-1：订阅test-topic
     * consumer-2：订阅test-topic
     * producer-1：生成test-topic
     * consumer-1不消费判断
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T2C_ProhibitConsumerOne")
    public void test1Topic2ConsumerProhibitConsumerOne() throws InterruptedException {
        // 新建消费者1
        initConsumer1(new String[]{"test-topic"});
        // 新建消费者2
        initConsumer2(new String[]{"test-topic"});
        // 新建生产者
        initProducer("producer-1", "test-topic");

        // 生产者生产消息(该接口开启线程一直生产消息)
        startProduce("producer-1");

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        // 消费者1已经被禁止
        Assertions.assertEquals(0, resultList.size());

        List<String> resultList2 = getConsumer2Result();
        // 消费者2正常消费
        Assertions.assertTrue(resultList2.size() > 0);
        Assertions.assertTrue(resultList2.contains(TEST_TOPIC_MSG));
    }

    /**
     * 启动时开启消费、单个消费者消费单个topic、随后禁止消费
     * consumer-1：订阅test-topic
     * producer-1：生成test-topic
     * 不消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T1C_RunProhibitAll")
    public void test1Topic1ConsumerRunProhibitAll() throws InterruptedException {
        // 前置步骤已调用成功 test1Topic1ConsumerAllowAll

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertEquals(0, resultList.size());
    }

    /**
     * 启动时开启消费、单个消费者消费多个topic、随后禁止消费
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 不消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_RunProhibitAll")
    public void test2Topic1ConsumerRunProhibitAll() throws InterruptedException {
        // 前置步骤已调用成功 test2Topic1ConsumerAllowAll

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertEquals(0, resultList.size());
    }

    /**
     * 启动时开启消费、单个消费者消费多个topic、随后禁止消费test-topic-1
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 不消费test-topic-1判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_RunProhibitTopicOne")
    public void test2Topic1ConsumerRunProhibitTopicOne() throws InterruptedException {
        // 前置步骤已调用成功 test2Topic1ConsumerAllowAll

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertFalse(resultList.contains(TEST_TOPIC_1_MSG));
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_2_MSG));
    }

    /**
     * 启动时开启消费、多个消费者消费单个topic、随后禁止消费1
     * consumer-1：订阅test-topic
     * consumer-2：订阅test-topic
     * producer-1：生成test-topic
     * consumer-1不消费判断
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T2C_RunProhibitConsumerOne")
    public void test1Topic2ConsumerRunProhibitConsumerOne() throws InterruptedException {
        // 前置步骤已调用成功 test2Topic1ConsumerAllowAll

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        // 消费者1已经被禁止
        Assertions.assertEquals(0, resultList.size());

        List<String> resultList2 = getConsumer2Result();
        // 消费者2正常消费
        Assertions.assertTrue(resultList2.size() > 0);
        Assertions.assertTrue(resultList2.contains(TEST_TOPIC_MSG));
    }

    /**
     * 启动时禁止消费、单个消费者消费单个topic、随后开启消费
     * consumer-1：订阅test-topic
     * producer-1：生成test-topic
     * 重新消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T1C_RunAllowAll")
    public void test1Topic1ConsumerRunAllowAll() throws InterruptedException {
        // 前置步骤已调用成功 test2Topic1ConsumerAllowAll

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertTrue(resultList.size() > 0);
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_MSG));
    }

    /**
     * 启动时禁止消费、单个消费者消费多个topic、随后开启消费
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 重新消费判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_RunAllowAll")
    public void test2Topic1ConsumerRunAllowAll() throws InterruptedException {
        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertTrue(resultList.size() > 0);
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_1_MSG));
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_2_MSG));
    }

    /**
     * 启动时禁止消费、单个消费者消费多个topic、随后开启消费test-topic-1
     * consumer-1：订阅test-topic-1、test-topic-2
     * producer-1：生成test-topic-1
     * producer-2：生成test-topic-2
     * 重新消费test-topic-1判定
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_2T1C_RunAllowTopicOne")
    public void test2Topic1ConsumerRunAllowTopicOne() throws InterruptedException {
        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_1_MSG));
        Assertions.assertFalse(resultList.contains(TEST_TOPIC_2_MSG));
    }

    /**
     * 启动时禁止消费一个topic、多个消费者消费单个topic、随后开启消费者1
     * consumer-1：订阅test-topic
     * consumer-2：订阅test-topic
     * producer-1：生成test-topic
     * consumer-1重新消费判断
     */
    @Test
    @EnabledIfSystemProperty(named = "mq.consumer.prohibition.test.type", matches = "TEST_KAFKA_1T2C_RunAllowConsumerOne")
    public void test1Topic2ConsumerRunAllowConsumerOne() throws InterruptedException {

        // 验证消费者消费
        Thread.sleep(2000);
        List<String> resultList = getConsumer1Result();
        // 消费者1重新消费
        Assertions.assertTrue(resultList.size() > 0);
        Assertions.assertTrue(resultList.contains(TEST_TOPIC_MSG));

        List<String> resultList2 = getConsumer2Result();
        // 消费者2正常消费
        Assertions.assertTrue(resultList2.size() > 0);
        Assertions.assertTrue(resultList2.contains(TEST_TOPIC_MSG));
    }

    /**
     * 获取消费者1的消费结果
     *
     * @return 消费结果
     */
    @Test
    private List<String> getConsumer1Result(){
        return RequestUtils.doPostGetList(CONSUMER_1_URL + "/kafkaConsumer/consume",
                new HashMap<String,
                        Object>() {{
                    put("name", "consumer-1");
                }});
    }

    /**
     * 获取消费者2的消费结果
     *
     * @return 消费结果
     */
    @Test
    private List<String> getConsumer2Result(){
        return RequestUtils.doPostGetList(CONSUMER_2_URL + "/kafkaConsumer/consume",
                new HashMap<String,
                        Object>() {{
                    put("name", "consumer-1");
                }});
    }

    /**
     * 初始化消费者1
     *
     * @param topics topic数组
     */
    private void initConsumer1(String[] topics){
        RequestUtils.doPost(CONSUMER_1_URL + "/kafkaConsumer/newConsumer", new HashMap<String, Object>() {{
            put("address", KAFKA_ADDRESS);
            put("name", "consumer-1");
            put("topics", topics);
        }});
    }

    /**
     * 初始化消费者2
     *
     * @param topics topic数组
     */
    private void initConsumer2(String[] topics){
        RequestUtils.doPost(CONSUMER_2_URL + "/kafkaConsumer/newConsumer", new HashMap<String, Object>() {{
            put("address", KAFKA_ADDRESS);
            put("name", "consumer-2");
            put("topics", topics);
        }});
    }

    /**
     * 初始化生产者
     *
     * @param producerName 生产者名称
     * @param topic topic
     */
    private void initProducer(String producerName, String topic){
        RequestUtils.doPost(PRODUCER_URL + "/kafkaProducer/newProducer", new HashMap<String,
                Object>() {{
            put("address", KAFKA_ADDRESS);
            put("name", producerName);
            put("topic", topic);
        }});
    }

    /**
     * 开始循环生产数据
     *
     * @param producerName 生产者名称
     */
    private void startProduce(String producerName){
        RequestUtils.doPost(PRODUCER_URL + "/kafkaProducer/startProduce", new HashMap<String,
                Object>() {{
            put("name", producerName);
        }});
    }
}

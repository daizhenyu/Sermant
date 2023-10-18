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

package com.huaweicloud.demo.lib.common;

/**
 * 消息中间件的公共常量
 *
 * @author daizhenyu
 * @since 2023-09-28
 **/
public class MessageConstant {
    /**
     * 消息中间件的topic
     */
    public static final String TOPIC = "traffic_tag_test";

    /**
     * 时间格式
     */
    public static final String TIME_FORMAT = "yyyy/MM/dd HH:mm:ss";

    /**
     * rocketmq消息的tag
     */
    public static final String TAG = "";

    /**
     * rocketmq消费者消费消息的标签范围
     */
    public static final String TAG_SCOPE = "*";

    /**
     * rocketmq服务端的ip地址
     */
    public static final String ROCKETMQ_IP_ADDRESS = "127.0.0.1:9876";

    /**
     * rocketmq生产者组
     */
    public static final String ROCKETMQ_PRODUCE_GROUP = "producer_group";

    /**
     * rocketmq消费者组
     */
    public static final String CONSUME_GROUP = "consume_group";

    /**
     * rocketmq消息体
     */
    public static final String MESSAGE_BODY_ROCKET = "hello inner rocketmq:";

    /**
     * kafka发送消息的key
     */
    public static final String KAFKA_KEY = "trafficTag";

    /**
     * kafka服务端地址
     */
    public static final String KAFKA_IP_ADDRESS = "127.0.0.1:9092";

    /**
     * kafka消息体
     */
    public static final String MESSAGE_BODY_KAFKA = "hello inner kafka:";

    /**
     * kafka拉去消息的timeout
     */
    public static final int KAFKA_CONSUMER_TIMEOUT = 100;

    private MessageConstant() {
    }
}

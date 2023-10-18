/*
 *  Copyright (C) 2023-2023 Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huaweicloud.demo.client.rocketmq.consumer;

import com.huaweicloud.demo.lib.common.MessageConstant;
import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * rocketmq 消费者
 *
 * @author daizhenyu
 * @since 2023-09-08
 **/
@Component
public class RocketMqConsumer implements CommandLineRunner {
    /**
     * 存储消费者调用http服务端返回的流量标签
     */
    public static final Map<String, String> ROCKETMQ_TAG_MAP = new HashMap<>();

    @Value("${commonServerUrl}")
    private String commonServerUrl;

    @Override
    public void run(String... args) {
        Thread rocketMqConsumerThread = new Thread(() -> {
            try {
                consumeData();
            } catch (MQClientException e) {
                // ignore
            }
        });
        rocketMqConsumerThread.start();
    }

    private void consumeData() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MessageConstant.CONSUME_GROUP);
        consumer.setNamesrvAddr(MessageConstant.ROCKETMQ_IP_ADDRESS);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(MessageConstant.TOPIC, MessageConstant.TAG_SCOPE);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messageExts,
                    ConsumeOrderlyContext context) {
                if (messageExts != null) {
                    for (MessageExt ext : messageExts) {
                        ext.getBody();
                        ROCKETMQ_TAG_MAP.put("rocketmqTag", HttpClientUtils.doHttpClientV4Get(commonServerUrl));
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }
}
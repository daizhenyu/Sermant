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

package com.huaweicloud.demo.tagtransmission.rocketmq.consumer;

import com.huaweicloud.demo.tagtransmission.midware.common.MessageConstant;
import com.huaweicloud.demo.tagtransmission.util.HttpClientUtils;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kafka消费者
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

    @Value("${common.server.url}")
    private String commonServerUrl;

    @Value("${rocketmq.address}")
    private String rocketMqAddress;

    @Override
    public void run(String[] args) throws MQClientException {
        consumeData();
    }

    private void consumeData() throws MQClientException {
        DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(MessageConstant.ROCKETMQ_CONSUME_GROUP);
        pullConsumer.setNamesrvAddr(rocketMqAddress);
        pullConsumer.subscribe(MessageConstant.TOPIC);
        pullConsumer.setPullBatchSize(1);
        pullConsumer.start();
        try {
            while (true) {
                List<MessageExt> messageExts = pullConsumer.poll();
                if (messageExts != null) {
                    for (MessageExt messageExt : messageExts) {
                        messageExt.getBody();
                        ROCKETMQ_TAG_MAP.put("rocketmqTag", HttpClientUtils.doHttpUrlConnectionGet(commonServerUrl));
                    }
                }
            }
        } finally {
            pullConsumer.shutdown();
        }
    }
}
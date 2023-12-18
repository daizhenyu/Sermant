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

package com.huaweicloud.sermant.rocketmq.controller;

import com.huaweicloud.sermant.core.common.LoggerFactory;
import com.huaweicloud.sermant.rocketmq.cache.RocketMqConsumerCache;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultMqPushConsumerWrapper;
import com.huaweicloud.sermant.utils.RocketmqWrapperUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * push消费者控制类
 *
 * @author daizhenyu
 * @since 2023-12-04
 **/
public class RocketMqPushConsumerController {
    private static final Logger LOGGER = LoggerFactory.getLogger();

    private RocketMqPushConsumerController() {
    }

    /**
     * 禁止push消费者消费
     *
     * @param wrapper push消费者wrapper
     * @param topics 禁止消费的topic
     */
    public static void disablePushConsumption(DefaultMqPushConsumerWrapper wrapper, Set<String> topics) {
        Set<String> subscribedTopic = wrapper.getSubscribedTopics();
        if (subscribedTopic.stream().anyMatch(topics::contains)) {
            suspendPushConsumer(wrapper);
            return;
        }
        resumePushConsumer(wrapper);
    }

    private static void suspendPushConsumer(DefaultMqPushConsumerWrapper wrapper) {
        if (wrapper.isPause()) {
            LOGGER.log(Level.INFO, "Success to prohibit consumption, consumer instance name : {0}, "
                            + "consumer group : {1}, topic : {2}",
                    new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
            return;
        }

        DefaultMQPushConsumerImpl pushConsumerImpl = wrapper.getPushConsumerImpl();
        String consumerGroup = wrapper.getConsumerGroup();

        pushConsumerImpl.persistConsumerOffset();
        wrapper.getClientFactory().unregisterConsumer(consumerGroup);
        pushConsumerImpl.doRebalance();
        wrapper.setPause(true);

        LOGGER.log(Level.INFO, "Success to prohibit consumption, consumer instance name : {0}, "
                        + "consumer group : {1}, topic : {2}",
                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
    }

    private static void resumePushConsumer(DefaultMqPushConsumerWrapper wrapper) {
        if (!wrapper.isPause()) {
            LOGGER.log(Level.INFO, "Success to open consumption, consumer "
                            + "instance name : {0}, consumer group : {1}, topic : {2}",
                    new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
            return;
        }

        DefaultMQPushConsumerImpl pushConsumerImpl = wrapper.getPushConsumerImpl();
        String consumerGroup = wrapper.getConsumerGroup();

        wrapper.getClientFactory().registerConsumer(consumerGroup, pushConsumerImpl);
        pushConsumerImpl.doRebalance();
        wrapper.setPause(false);
        LOGGER.log(Level.INFO, "Success to open consumption, consumer "
                        + "instance name : {0}, consumer group : {1}, topic : {2}",
                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
    }

    /**
     * 添加PushConsumer包装类实例
     *
     * @param pushConsumer pushConsumer实例
     */
    public static void cachePushConsumer(DefaultMQPushConsumer pushConsumer) {
        Optional<DefaultMqPushConsumerWrapper> pushConsumerWrapperOptional = RocketmqWrapperUtils
                .wrapPushConsumer(pushConsumer);
        if (pushConsumerWrapperOptional.isPresent()) {
            RocketMqConsumerCache.PUSH_CONSUMERS_CACHE.add(pushConsumerWrapperOptional.get());
            LOGGER.log(Level.INFO, "Success to cache consumer, "
                            + "consumer instance name : {0}, consumer group : {1}, topic : {2}",
                    new Object[]{pushConsumer.getInstanceName(), pushConsumer.getConsumerGroup(),
                            pushConsumerWrapperOptional.get().getSubscribedTopics()});
            return;
        }
        LOGGER.log(Level.SEVERE, "Fail to cache consumer, consumer instance name : {0}, consumer group : {1}",
                new Object[]{pushConsumer.getInstanceName(), pushConsumer.getConsumerGroup()});
    }

    /**
     * 移除PushConsumer包装类实例
     *
     * @param pushConsumer pushConsumer实例
     */
    public static void removePushConsumer(DefaultMQPushConsumer pushConsumer) {
        Iterator<DefaultMqPushConsumerWrapper> iterator = RocketMqConsumerCache.PUSH_CONSUMERS_CACHE.iterator();
        while (iterator.hasNext()) {
            DefaultMqPushConsumerWrapper next = iterator.next();
            if (next.getPushConsumer().equals(pushConsumer)) {
                iterator.remove();
                LOGGER.log(Level.INFO, "Success to remove consumer, consumer instance name : {0}, consumer group "
                                + ": {1}, topic : {2}",
                        new Object[]{next.getInstanceName(), next.getConsumerGroup(), next.getSubscribedTopics()});
            }
        }
    }

    /**
     * 获取PushConsumer缓存
     *
     * @return PushConsumer缓存
     */
    public static Set<DefaultMqPushConsumerWrapper> getPushConsumerCache() {
        return RocketMqConsumerCache.PUSH_CONSUMERS_CACHE;
    }
}

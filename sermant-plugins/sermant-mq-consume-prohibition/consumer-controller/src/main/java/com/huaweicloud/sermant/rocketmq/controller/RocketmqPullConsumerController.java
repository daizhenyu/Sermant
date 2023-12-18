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
import com.huaweicloud.sermant.core.utils.ReflectUtils;
import com.huaweicloud.sermant.rocketmq.cache.RocketMqConsumerCache;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultLitePullConsumerWrapper;
import com.huaweicloud.sermant.utils.RocketmqWrapperUtils;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pull消费者控制类
 *
 * @author daizhenyu
 * @since 2023-12-15
 **/
public class RocketmqPullConsumerController {
    private static final long MAX_DELAY_TIME = 5000L;

    private static final int THREAD_SIZE = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger();

    private static volatile ScheduledExecutorService executorService;

    private RocketmqPullConsumerController() {
    }

    /**
     * 禁止pull消费者消费
     *
     * @param wrapper pull消费者wrapper
     * @param topics 禁止消费的topic
     */
    public static void disablePullConsumption(DefaultLitePullConsumerWrapper wrapper, Set<String> topics) {
        Set<String> subscribedTopic = wrapper.getSubscribedTopics();
        if (subscribedTopic.stream().anyMatch(topics::contains)) {
            suspendPullConsumer(wrapper);
            return;
        }
        resumePullConsumer(wrapper);
    }

    private static void suspendPullConsumer(DefaultLitePullConsumerWrapper wrapper) {
        switch (wrapper.getSubscriptionType()) {
            case SUBSCRIBE:
                suspendSubscriptiveConsumer(wrapper);
                break;
            case ASSIGN:
                suspendAssignedConsumer(wrapper);
                break;
            default:
                break;
        }
    }

    private static void suspendSubscriptiveConsumer(DefaultLitePullConsumerWrapper wrapper) {
        if (wrapper.isPause()) {
            LOGGER.log(Level.INFO, "Success to prohibit consumption, consumer instance name : {0}, "
                            + "consumer group : {1}, topic : {2}",
                    new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
            return;
        }
        wrapper.getPullConsumerImpl().persistConsumerOffset();
        wrapper.getClientFactory().unregisterConsumer(wrapper.getConsumerGroup());
        doRebalance(wrapper);
        wrapper.setPause(true);
    }

    private static void suspendAssignedConsumer(DefaultLitePullConsumerWrapper wrapper) {
        wrapper.getPullConsumer().pause(wrapper.getAssignedMessageQueues());
        LOGGER.log(Level.INFO, "Success to prohibit consumption, consumer instance name : {0}, consumer group : {1}, "
                        + "message queue : {2}",
                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(),
                        wrapper.getAssignedMessageQueues()});
    }

    private static void doRebalance(DefaultLitePullConsumerWrapper wrapper) {
        initExecutorService();
        executorService.schedule(() -> {
            RebalanceImpl rebalance = wrapper.getRebalanceImpl();
            ConcurrentMap<String, SubscriptionData> subTable = rebalance.getSubscriptionInner();
            if (subTable != null) {
                for (Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                    String topic = entry.getKey();
                    List<String> consumerIdList = wrapper.getClientFactory()
                            .findConsumerIdList(topic, wrapper.getConsumerGroup());
                    if (consumerIdList != null && consumerIdList.contains(wrapper.getClientFactory().getClientId())) {
                        LOGGER.log(Level.WARNING, "Consumer exiting the {0} consumer group timeout may cause "
                                        + "a failure to reallocate the message queue, "
                                        + "consumer instance name : {0}, consumer group : {1}, topic : {2}. "
                                        + "Please deliver the configuration again.",
                                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(),
                                        wrapper.getSubscribedTopics()});
                        break;
                    }

                    // 清空消费者的processQunues、assignedMessageQueues和pullTask，从而停止消费
                    ReflectUtils.invokeMethod(rebalance, "updateProcessQueueTableInRebalance",
                            new Class[]{String.class, Set.class, boolean.class},
                            new Object[]{topic, new HashSet<MessageQueue>(), false});
                    Set<MessageQueue> messageQueuesSet = rebalance.getTopicSubscribeInfoTable().get(topic);
                    rebalance.messageQueueChanged(topic, messageQueuesSet, new HashSet<>());
                    LOGGER.log(Level.INFO, "Success to prohibit consumption, consumer instance name : {0}, "
                                    + "consumer group : {1}, topic : {2}",
                            new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(),
                                    wrapper.getSubscribedTopics()});
                }
            }
        }, MAX_DELAY_TIME, TimeUnit.SECONDS);
    }

    private static void resumePullConsumer(DefaultLitePullConsumerWrapper wrapper) {
        switch (wrapper.getSubscriptionType()) {
            case SUBSCRIBE:
                resumeSubscriptiveConsumer(wrapper);
                break;
            case ASSIGN:
                resumeAssignedConsumer(wrapper);
                break;
            default:
                break;
        }
    }

    private static void resumeSubscriptiveConsumer(DefaultLitePullConsumerWrapper wrapper) {
        if (!wrapper.isPause()) {
            LOGGER.log(Level.INFO, "Success to open consumption, consumer "
                            + "instance name : {0}, consumer group : {1}, topic : {2}",
                    new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
            return;
        }
        String consumerGroup = wrapper.getConsumerGroup();
        DefaultLitePullConsumerImpl pullConsumerImpl = wrapper.getPullConsumerImpl();

        wrapper.getClientFactory().registerConsumer(consumerGroup, pullConsumerImpl);
        pullConsumerImpl.doRebalance();
        wrapper.setPause(false);
        LOGGER.log(Level.INFO, "Success to open consumption, "
                        + "consumer instance name : {0}, consumer group : {1}, topic : {2}",
                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(), wrapper.getSubscribedTopics()});
    }

    private static void resumeAssignedConsumer(DefaultLitePullConsumerWrapper wrapper) {
        wrapper.getPullConsumer().resume(wrapper.getAssignedMessageQueues());
        LOGGER.log(Level.INFO, "Success to open consumption, "
                        + "consumer instance name : {0}, consumer group : {1}, message queue : {2}",
                new Object[]{wrapper.getInstanceName(), wrapper.getConsumerGroup(),
                        wrapper.getAssignedMessageQueues()});
    }

    /**
     * 添加PullConsumer包装类实例
     *
     * @param pullConsumer pullConsumer实例
     */
    public static void cachePullConsumer(DefaultLitePullConsumer pullConsumer) {
        Optional<DefaultLitePullConsumerWrapper> pullConsumerWrapperOptional = RocketmqWrapperUtils
                .wrapPullConsumer(pullConsumer);
        if (pullConsumerWrapperOptional.isPresent()) {
            RocketMqConsumerCache.PULL_CONSUMERS_CACHE.add(pullConsumerWrapperOptional.get());
            LOGGER.log(Level.INFO, "Success to cache consumer, "
                            + "consumer instance name : {0}, consumer group : {1}, topic : {2}",
                    new Object[]{pullConsumer.getInstanceName(), pullConsumer.getConsumerGroup(),
                            pullConsumerWrapperOptional.get().getSubscribedTopics()});
            return;
        }
        LOGGER.log(Level.SEVERE, "Fail to cache consumer, consumer instance name : {0}, consumer group : {1}",
                new Object[]{pullConsumer.getInstanceName(), pullConsumer.getConsumerGroup()});
    }

    /**
     * 移除PullConsumer包装类实例
     *
     * @param pullConsumer pullConsumer实例
     */
    public static void removePullConsumer(DefaultLitePullConsumer pullConsumer) {
        Iterator<DefaultLitePullConsumerWrapper> iterator = RocketMqConsumerCache.PULL_CONSUMERS_CACHE.iterator();
        while (iterator.hasNext()) {
            DefaultLitePullConsumerWrapper next = iterator.next();
            if (next.getPullConsumer().equals(pullConsumer)) {
                iterator.remove();
                LOGGER.log(Level.INFO, "Success to remove consumer, consumer instance name : {0}, consumer group "
                                + ": {1}, topic : {2}",
                        new Object[]{next.getInstanceName(), next.getConsumerGroup(), next.getSubscribedTopics()});
            }
        }
    }

    /**
     * 获取PullConsumer缓存
     *
     * @return PullConsumer缓存
     */
    public static Set<DefaultLitePullConsumerWrapper> getPullConsumerCache() {
        return RocketMqConsumerCache.PULL_CONSUMERS_CACHE;
    }

    private static void initExecutorService() {
        if (executorService == null) {
            synchronized (RocketMqPushConsumerController.class) {
                if (executorService == null) {
                    executorService = Executors.newScheduledThreadPool(THREAD_SIZE);
                }
            }
        }
    }
}

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

package com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor;

import com.huaweicloud.sermant.config.ProhibitionConfigManager;
import com.huaweicloud.sermant.core.plugin.agent.entity.ExecuteContext;
import com.huaweicloud.sermant.core.plugin.agent.interceptor.AbstractInterceptor;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.cache.RocketMqAssignedMessageQueueCache;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.cache.RocketMqSubscribedTopicCache;
import com.huaweicloud.sermant.rocketmq.constant.SubscriptionType;
import com.huaweicloud.sermant.rocketmq.controller.RocketmqPullConsumerController;
import com.huaweicloud.sermant.rocketmq.extension.RocketMqConsumerHandler;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultLitePullConsumerWrapper;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * RocketMq pullConsumer启动拦截器
 *
 * @author daizhenyu
 * @since 2023-12-04
 **/
public class RocketMqPullConsumerStartInterceptor extends AbstractInterceptor {
    private RocketMqConsumerHandler handler;

    /**
     * 无参构造方法
     */
    public RocketMqPullConsumerStartInterceptor() {
    }

    /**
     * 有参构造方法
     *
     * @param handler 拦截点处理器
     */
    public RocketMqPullConsumerStartInterceptor(RocketMqConsumerHandler handler) {
        this.handler = handler;
    }

    @Override
    public ExecuteContext before(ExecuteContext context) throws Exception {
        if (handler != null) {
            handler.doBefore(context);
        }
        return context;
    }

    @Override
    public ExecuteContext after(ExecuteContext context) throws Exception {
        Object consumerObject = context.getObject();
        if (consumerObject != null && consumerObject instanceof DefaultLitePullConsumer) {
            DefaultLitePullConsumer pullConsumer = (DefaultLitePullConsumer) consumerObject;
            RocketmqPullConsumerController.cachePullConsumer(pullConsumer);
        }

        // 获取存入缓存的消费者包装类实例
        Optional<DefaultLitePullConsumerWrapper> pullConsumerWrapperOptional =
                RocketmqPullConsumerController.getPullConsumerCache()
                        .stream()
                        .filter(obj -> obj.getPullConsumer().equals(consumerObject))
                        .findFirst();
        DefaultLitePullConsumerWrapper pullConsumerWrapper = null;
        if (pullConsumerWrapperOptional.isPresent()) {
            pullConsumerWrapper = pullConsumerWrapperOptional.get();

            // 从topic缓存中获取消费者启动前订阅的topic，传入wrapper，并指明消费者的消费类型为subscribe
            Optional<Set<String>> subscribedTopicsOptional = RocketMqSubscribedTopicCache
                    .getSubscribedTopics(consumerObject);
            if (subscribedTopicsOptional.isPresent()) {
                pullConsumerWrapper.setSubscribedTopics(subscribedTopicsOptional.get());
                RocketMqSubscribedTopicCache.removeConsumerSubscribedTopics(consumerObject);
                pullConsumerWrapper.setSubscriptionType(SubscriptionType.SUBSCRIBE);
            } else {
                // 从消息队列缓存中获取消费者启动前指定的消息队列，传入wrapper，并指明消费者的消费类型为assign
                Optional<Collection<MessageQueue>> assignedMessageQueueOptional = RocketMqAssignedMessageQueueCache
                        .getAssignedMessageQueue(consumerObject);
                if (assignedMessageQueueOptional.isPresent()) {
                    Collection<MessageQueue> messageQueues = assignedMessageQueueOptional.get();
                    pullConsumerWrapper.setAssignedMessageQueues(messageQueues);
                    pullConsumerWrapper.setSubscribedTopics(getMessageQueueTopics(messageQueues));
                    RocketMqAssignedMessageQueueCache.removeConsumerAssignedMessageQueue(consumerObject);
                }
            }
        }
        if (handler != null) {
            handler.doAfter(context);
            return context;
        }
        if (pullConsumerWrapper != null) {
            // 消费者启动会根据缓存的禁消费配置对消费者执行禁消费
            RocketmqPullConsumerController.disablePullConsumption(pullConsumerWrapper,
                    ProhibitionConfigManager.getRocketMqProhibitionTopics());
        }
        return context;
    }

    @Override
    public ExecuteContext onThrow(ExecuteContext context) {
        if (handler != null) {
            handler.doOnThrow(context);
        }
        return context;
    }

    private Set<String> getMessageQueueTopics(Collection<MessageQueue> messageQueues) {
        HashSet<String> topics = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            topics.add(messageQueue.getTopic());
        }
        return topics;
    }
}
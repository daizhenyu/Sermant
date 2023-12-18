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
import com.huaweicloud.sermant.mq.prohibition.rocketmq.cache.RocketMqAssignedMessageQueueCache;
import com.huaweicloud.sermant.rocketmq.controller.RocketmqPullConsumerController;
import com.huaweicloud.sermant.rocketmq.extension.RocketMqConsumerHandler;
import com.huaweicloud.sermant.rocketmq.wrapper.AbstractConsumerWrapper;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultLitePullConsumerWrapper;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * RocketMq pullConsumer指定队列拦截器
 *
 * @author daizhenyu
 * @since 2023-12-15
 **/
public class RocketMqPullConsumerAssignInterceptor extends AbstractConsumerInterceptor {
    /**
     * 无参构造方法
     */
    public RocketMqPullConsumerAssignInterceptor() {
    }

    /**
     * 有参构造方法
     *
     * @param handler 处理器
     */
    public RocketMqPullConsumerAssignInterceptor(RocketMqConsumerHandler handler) {
        super(handler);
    }

    @Override
    protected ExecuteContext doBefore(ExecuteContext context) {
        if (handler != null) {
            handler.doBefore(context);
        }
        return context;
    }

    @Override
    protected ExecuteContext doAfter(ExecuteContext context, AbstractConsumerWrapper wrapper) {
        Object consumerObject = context.getObject();
        Object[] argumentObject = context.getArguments();
        if (consumerObject == null || argumentObject == null || argumentObject.length == 0) {
            return context;
        }
        Object messageQueueObject = argumentObject[0];
        if (messageQueueObject == null || !(messageQueueObject instanceof Collection)) {
            return context;
        }

        Collection<MessageQueue> messageQueues = (Collection<MessageQueue>) messageQueueObject;

        DefaultLitePullConsumerWrapper pullConsumerWrapper = null;
        if (wrapper == null) {
            RocketMqAssignedMessageQueueCache.updateSubscribedTopic(consumerObject, messageQueues);
        } else {
            pullConsumerWrapper = (DefaultLitePullConsumerWrapper) wrapper;
            pullConsumerWrapper.setAssignedMessageQueues(messageQueues);
            pullConsumerWrapper.setSubscribedTopics(getMessageQueueTopics(messageQueues));
        }

        if (handler != null) {
            handler.doAfter(context);
            return context;
        }

        if (pullConsumerWrapper != null) {
            // 指定消费的队列后，需根据禁消费的topic配置对消费者开启或禁止消费
            RocketmqPullConsumerController.disablePullConsumption(pullConsumerWrapper,
                    ProhibitionConfigManager.getRocketMqProhibitionTopics());
        }

        return context;
    }

    @Override
    protected ExecuteContext doOnThrow(ExecuteContext context) {
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

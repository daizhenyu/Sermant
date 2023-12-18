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

package com.huaweicloud.sermant.mq.prohibition.rocketmq.cache;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于存储pull消费者指定的消息队列
 *
 * @author daizhenyu
 * @since 2023-12-15
 **/
public class RocketMqAssignedMessageQueueCache {
    private static ConcurrentHashMap<Object, Collection<MessageQueue>> assignedMessageQueueCache =
            new ConcurrentHashMap<>();

    private RocketMqAssignedMessageQueueCache() {
    }

    /**
     * 更新消息队列缓存
     *
     * @param object 消费者
     * @param messageQueues 指定的消息队列
     */
    public static void updateSubscribedTopic(Object object, Collection<MessageQueue> messageQueues) {
        assignedMessageQueueCache.put(object, messageQueues);
    }

    /**
     * 删除消费者实例指定的消息队列
     *
     * @param object 消费者
     */
    public static void removeConsumerAssignedMessageQueue(Object object) {
        assignedMessageQueueCache.remove(object);
    }

    /**
     * 获取消费者实例的所指定的消息队列
     *
     * @param object 消费者
     * @return 消费者所订阅的消息队列
     */
    public static Optional<Collection<MessageQueue>> getAssignedMessageQueue(Object object) {
        Collection<MessageQueue> messageQueues = assignedMessageQueueCache.get(object);
        if (messageQueues == null) {
            return Optional.empty();
        }
        return Optional.of(messageQueues);
    }
}

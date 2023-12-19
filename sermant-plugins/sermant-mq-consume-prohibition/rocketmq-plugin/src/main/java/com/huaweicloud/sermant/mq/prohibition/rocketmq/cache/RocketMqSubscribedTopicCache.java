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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于存储消费者订阅时的topic
 *
 * @author daizhenyu
 * @since 2023-12-15
 **/
public class RocketMqSubscribedTopicCache {
    private static ConcurrentHashMap<Object, Set<String>> subscribedTopicCache = new ConcurrentHashMap<>();

    private RocketMqSubscribedTopicCache() {
    }

    /**
     * 添加topic
     *
     * @param object 消费者
     * @param topic 订阅主题
     */
    public static void addSubscribedTopic(Object object, String topic) {
        Set<String> topics = subscribedTopicCache.get(object);
        if (topics == null) {
            topics = new HashSet<>();
        }
        topics.add(topic);
        subscribedTopicCache.putIfAbsent(object, topics);
    }

    /**
     * 删除topic
     *
     * @param object 消费者
     * @param topic 订阅主题
     */
    public static void removeSubscribedTopic(Object object, String topic) {
        Set<String> topics = subscribedTopicCache.get(object);
        if (topics == null) {
            topics = new HashSet<>();
        }
        topics.remove(topic);
        subscribedTopicCache.putIfAbsent(object, topics);
    }

    /**
     * 删除消费者实例的所有topic
     *
     * @param object 消费者
     */
    public static void removeConsumerSubscribedTopics(Object object) {
        subscribedTopicCache.remove(object);
    }

    /**
     * 获取消费者实例的所订阅的topic
     *
     * @param object 消费者
     * @return 所订阅的topics
     */
    public static Optional<Set<String>> getSubscribedTopics(Object object) {
        Set<String> topics = subscribedTopicCache.get(object);
        if (topics == null) {
            return Optional.empty();
        }
        return Optional.of(topics);
    }
}

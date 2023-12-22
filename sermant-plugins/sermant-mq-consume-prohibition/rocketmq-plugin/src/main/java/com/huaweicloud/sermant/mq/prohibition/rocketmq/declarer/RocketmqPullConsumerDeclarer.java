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

package com.huaweicloud.sermant.mq.prohibition.rocketmq.declarer;

import com.huaweicloud.sermant.core.plugin.agent.declarer.AbstractPluginDeclarer;
import com.huaweicloud.sermant.core.plugin.agent.declarer.InterceptDeclarer;
import com.huaweicloud.sermant.core.plugin.agent.matcher.ClassMatcher;
import com.huaweicloud.sermant.core.plugin.agent.matcher.MethodMatcher;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor.RocketMqPullConsumerAssignInterceptor;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor.RocketMqPullConsumerShutdownInterceptor;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor.RocketMqPullConsumerStartInterceptor;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor.RocketMqPullConsumerSubscribeInterceptor;
import com.huaweicloud.sermant.mq.prohibition.rocketmq.interceptor.RocketMqPullConsumerUnsubscribeInterceptor;

/**
 * pullConsumer声明器，支持rocketmq4.8+版本
 *
 * @author daizhenyu
 * @since 2023-12-04
 **/
public class RocketmqPullConsumerDeclarer extends AbstractPluginDeclarer {
    @Override
    public ClassMatcher getClassMatcher() {
        return ClassMatcher.nameEquals("org.apache.rocketmq.client.consumer.DefaultLitePullConsumer");
    }

    @Override
    public InterceptDeclarer[] getInterceptDeclarers(ClassLoader classLoader) {
        return new InterceptDeclarer[]{
                InterceptDeclarer.build(MethodMatcher.nameEquals("start"),
                        new RocketMqPullConsumerStartInterceptor()),
                InterceptDeclarer.build(MethodMatcher.nameEquals("subscribe"),
                        new RocketMqPullConsumerSubscribeInterceptor()),
                InterceptDeclarer.build(MethodMatcher.nameEquals("unsubscribe"),
                        new RocketMqPullConsumerUnsubscribeInterceptor()),
                InterceptDeclarer.build(MethodMatcher.nameEquals("assign"),
                        new RocketMqPullConsumerAssignInterceptor()),
                InterceptDeclarer.build(MethodMatcher.nameEquals("shutdown"),
                        new RocketMqPullConsumerShutdownInterceptor())
        };
    }
}

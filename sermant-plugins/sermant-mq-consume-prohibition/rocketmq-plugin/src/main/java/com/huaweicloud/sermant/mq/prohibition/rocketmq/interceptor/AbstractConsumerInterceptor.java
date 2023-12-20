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

import com.huaweicloud.sermant.core.plugin.agent.entity.ExecuteContext;
import com.huaweicloud.sermant.core.plugin.agent.interceptor.AbstractInterceptor;
import com.huaweicloud.sermant.rocketmq.controller.RocketMqPushConsumerController;
import com.huaweicloud.sermant.rocketmq.controller.RocketmqPullConsumerController;
import com.huaweicloud.sermant.rocketmq.extension.RocketMqConsumerHandler;
import com.huaweicloud.sermant.rocketmq.wrapper.AbstractConsumerWrapper;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultLitePullConsumerWrapper;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultMqPushConsumerWrapper;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;

import java.util.Optional;

/**
 * 抽象拦截器
 *
 * @author daizhenyu
 * @since 2023-12-04
 **/
public abstract class AbstractConsumerInterceptor extends AbstractInterceptor {
    /**
     * 外部扩展处理器
     */
    protected RocketMqConsumerHandler handler;

    /**
     * 无参构造方法
     */
    public AbstractConsumerInterceptor() {
    }

    /**
     * 有参构造方法
     *
     * @param handler 外部扩展处理器
     */
    public AbstractConsumerInterceptor(RocketMqConsumerHandler handler) {
        this.handler = handler;
    }

    @Override
    public ExecuteContext before(ExecuteContext context) {
        return doBefore(context);
    }

    @Override
    public ExecuteContext after(ExecuteContext context) {
        Object consumerObject = context.getObject();
        if (consumerObject != null && consumerObject instanceof DefaultMQPushConsumer) {
            Optional<DefaultMqPushConsumerWrapper> pushConsumerWrapperOptional =
                    RocketMqPushConsumerController.getPushConsumerWrapper(consumerObject);
            if (pushConsumerWrapperOptional.isPresent()) {
                return doAfter(context, pushConsumerWrapperOptional.get());
            }
        }

        if (consumerObject != null && consumerObject instanceof DefaultLitePullConsumer) {
            Optional<DefaultLitePullConsumerWrapper> pullConsumerWrapperOptional = RocketmqPullConsumerController
                    .getPullConsumerWrapper(consumerObject);
            if (pullConsumerWrapperOptional.isPresent()) {
                return doAfter(context, pullConsumerWrapperOptional.get());
            }
        }

        // 消费者未启动前不会缓存，此时传入null即可
        return doAfter(context, null);
    }

    @Override
    public ExecuteContext onThrow(ExecuteContext context) throws Exception {
        return doOnThrow(context);
    }

    /**
     * 前置方法
     *
     * @param context 执行上下文
     * @return ExecuteContext
     */
    protected abstract ExecuteContext doBefore(ExecuteContext context);

    /**
     * 后置方法
     *
     * @param context 执行上下文
     * @param wrapper 消费者包装类
     * @return ExecuteContext
     */
    protected abstract ExecuteContext doAfter(ExecuteContext context, AbstractConsumerWrapper wrapper);

    /**
     * 异常时方法
     *
     * @param context 执行上下文
     * @return ExecuteContext
     */
    protected abstract ExecuteContext doOnThrow(ExecuteContext context);
}

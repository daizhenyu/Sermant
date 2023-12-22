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
import com.huaweicloud.sermant.rocketmq.controller.RocketMqPullConsumerController;
import com.huaweicloud.sermant.rocketmq.extension.RocketMqConsumerHandler;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;

/**
 * RocketMq pullConsumer关闭拦截器
 *
 * @author daizhenyu
 * @since 2023-12-04
 **/
public class RocketMqPullConsumerShutdownInterceptor extends AbstractInterceptor {
    private RocketMqConsumerHandler handler;

    /**
     * 无参构造方法
     */
    public RocketMqPullConsumerShutdownInterceptor() {
    }

    /**
     * 有参构造方法
     *
     * @param handler 拦截点处理器
     */
    public RocketMqPullConsumerShutdownInterceptor(RocketMqConsumerHandler handler) {
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
            RocketMqPullConsumerController.removePullConsumer(pullConsumer);
        }

        if (handler != null) {
            handler.doAfter(context);
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
}
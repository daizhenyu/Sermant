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
import com.huaweicloud.sermant.mq.prohibition.rocketmq.utils.ProhibitConsumptionUtils;
import com.huaweicloud.sermant.rocketmq.extension.RocketMqConsumerHandler;
import com.huaweicloud.sermant.rocketmq.wrapper.AbstractConsumerWrapper;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultMqPushConsumerWrapper;

/**
 * RocketMq pushConsumer订阅拦截器
 *
 * @author daizhenyu
 * @since 2023-12-15
 **/
public class RocketMqPushConsumerSubscribeInterceptor extends AbstractConsumerInterceptor {
    /**
     * 无参构造方法
     */
    public RocketMqPushConsumerSubscribeInterceptor() {
    }

    /**
     * 有参构造方法
     *
     * @param handler 处理器
     */
    public RocketMqPushConsumerSubscribeInterceptor(RocketMqConsumerHandler handler) {
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
        if (handler != null) {
            handler.doAfter(context);
            return context;
        }

        if (wrapper != null) {
            // 增加topic订阅后，消费者订阅信息发生变化，需根据禁消费的topic配置对消费者开启或禁止消费
            ProhibitConsumptionUtils.disablePushConsumption((DefaultMqPushConsumerWrapper) wrapper);
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
}

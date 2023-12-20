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

package com.huaweicloud.sermant.mq.prohibition.rocketmq.utils;

import com.huaweicloud.sermant.config.ProhibitionConfigManager;
import com.huaweicloud.sermant.rocketmq.controller.RocketMqPushConsumerController;
import com.huaweicloud.sermant.rocketmq.controller.RocketmqPullConsumerController;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultLitePullConsumerWrapper;
import com.huaweicloud.sermant.rocketmq.wrapper.DefaultMqPushConsumerWrapper;

/**
 * 用于操作消费者wrapper，执行禁消费操作
 *
 * @author daizhenyu
 * @since 2023-12-20
 **/
public class ProhibitConsumptionUtils {
    private ProhibitConsumptionUtils() {
    }

    /**
     * pullconsumer 执行禁消费操作
     *
     * @param pullConsumerWrapper pullconsumer包装类实例
     */
    public static void disablePullConsumption(DefaultLitePullConsumerWrapper pullConsumerWrapper) {
        if (pullConsumerWrapper != null) {
            RocketmqPullConsumerController.disablePullConsumption(pullConsumerWrapper,
                    ProhibitionConfigManager.getRocketMqProhibitionTopics());
        }
    }

    /**
     * pushconsumer 执行禁消费操作
     *
     * @param pushConsumerWrapper pushconsumer包装类实例
     */
    public static void disablePushConsumption(DefaultMqPushConsumerWrapper pushConsumerWrapper) {
        if (pushConsumerWrapper != null) {
            RocketMqPushConsumerController.disablePushConsumption(pushConsumerWrapper,
                    ProhibitionConfigManager.getRocketMqProhibitionTopics());
        }
    }
}

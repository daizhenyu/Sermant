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

/**
 * 判断是否为Sermant调用
 *
 * @author daizhenyu
 * @since 2023-12-19
 **/
public class InvokeUtils {
    private static final String ROCKETMQ_PULL_CONSUMER_CLASS_NAME =
            "org.apache.rocketmq.client.consumer.DefaultLitePullConsumer";

    private static final String CONSUMER_CONTROLLER_CLASS_NAME =
            "com.huaweicloud.sermant.rocketmq.controller.RocketMqPullConsumerController";

    private InvokeUtils() {
    }

    /**
     * 判断是否Sermant发起的调用
     *
     * @return 是否Sermant发起的调用
     */
    public static boolean isInvokeBySermant() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int stackTraceIdxMax = stackTrace.length - 1;
        for (int i = 0; i < stackTrace.length; i++) {
            if (!ROCKETMQ_PULL_CONSUMER_CLASS_NAME.equals(stackTrace[i].getClassName())) {
                continue;
            }
            if (i == stackTraceIdxMax) {
                break;
            }
            if (ROCKETMQ_PULL_CONSUMER_CLASS_NAME.equals(stackTrace[i + 1].getClassName())) {
                continue;
            }
            if (CONSUMER_CONTROLLER_CLASS_NAME.equals(stackTrace[i + 1].getClassName())) {
                return true;
            }
        }
        return false;
    }
}
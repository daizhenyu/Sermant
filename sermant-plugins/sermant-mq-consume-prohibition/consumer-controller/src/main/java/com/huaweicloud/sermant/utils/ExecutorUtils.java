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

package com.huaweicloud.sermant.utils;

import com.huaweicloud.sermant.rocketmq.controller.RocketMqPushConsumerController;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 *
 * @author daizhenyu
 * @since 2023-12-22
 **/
public class ExecutorUtils {
    private static final int THREAD_SIZE = 1;

    private static final long THREAD_KEEP_ALIVE_TIME = 60L;

    private static final int THREAD_QUEUE_CAPACITY = 20;

    private static volatile ThreadPoolExecutor executor;

    private ExecutorUtils() {
    }

    /**
     * 提交线程任务
     *
     * @param runnable 线程任务
     */
    public static void submit(Runnable runnable) {
        initExecutor();
        executor.submit(runnable);
    }

    private static void initExecutor() {
        if (executor == null) {
            synchronized (RocketMqPushConsumerController.class) {
                if (executor == null) {
                    executor = new ThreadPoolExecutor(THREAD_SIZE, THREAD_SIZE, THREAD_KEEP_ALIVE_TIME,
                            TimeUnit.SECONDS, new ArrayBlockingQueue<>(THREAD_QUEUE_CAPACITY));
                    executor.allowCoreThreadTimeOut(true);
                }
            }
        }
    }
}

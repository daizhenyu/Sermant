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

package com.huaweicloud.demo.client.controller;

import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 验证流量标签传递 跨线程场景
 *
 * @author daizhenyu
 * @since 2023-09-11
 **/
@RestController
@RequestMapping(value = "thread")
public class ThreadController {
    /**
     * 存储消费者调用http服务端返回的流量标签
     */
    public static final Map<String, String> THREAD_TAG_MAP = new HashMap<>();

    private static final int CORE_POOL_SIZE = 2;

    private static final int MAX_POOL_SIZE = 2;

    private static final int KEEP_ALIVE_TIME = 10;

    private static final int QUEUE_CAPACITY = 20;

    private static final int INITIAL_DELAY = 1;

    private static final int DELAY = 10;

    private static final int SLEEP = 5000;

    private static final String THREAD_TAG = "threadTag";

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_CAPACITY));

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(CORE_POOL_SIZE);

    @Value("${commonServerUrl}")
    private String commonServerUrl;

    /**
     * 新建线程
     *
     * @return 透传标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "newthread", method = RequestMethod.GET)
    public String newThreadTest() throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(() -> HttpClientUtils.doHttpClientV4Get(commonServerUrl));
        Thread thread = new Thread(futureTask);
        thread.start();
        return futureTask.get();
    }

    /**
     * 普通线程池执行executor方法提交线程任务
     *
     * @return 透传标签值
     * @throws InterruptedException
     */
    @RequestMapping(value = "executor", method = RequestMethod.GET)
    public String executorTest() throws InterruptedException {
        String trafficTag = null;
        executor.execute(() -> THREAD_TAG_MAP.put(THREAD_TAG, HttpClientUtils.doHttpClientV4Get(commonServerUrl)));
        Thread.sleep(SLEEP);
        trafficTag = THREAD_TAG_MAP.get(THREAD_TAG);

        // 删除流量标签，以免干扰下一次测试查询
        THREAD_TAG_MAP.remove(THREAD_TAG);
        return trafficTag;
    }

    /**
     * 普通线程池执行submit方法提交线程任务
     *
     * @return 透传标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "submit", method = RequestMethod.GET)
    public String submitTest() throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(() -> HttpClientUtils.doHttpClientV4Get(commonServerUrl));
        executor.submit(futureTask);
        return futureTask.get();
    }

    /**
     * 定时线程池执行schedule方法提交线程任务
     *
     * @return 透传标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "schedule", method = RequestMethod.GET)
    public String scheduleTest() throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(() -> HttpClientUtils.doHttpClientV4Get(commonServerUrl));
        scheduledExecutor.schedule(futureTask, INITIAL_DELAY, TimeUnit.SECONDS);
        return futureTask.get();
    }

    /**
     * 定时线程池执行scheduleAtFixedRate方法提交线程任务
     *
     * @return 透传标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "scheduleAtFixedRate", method = RequestMethod.GET)
    public String scheduleAtFixedRateTest() throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(() -> HttpClientUtils.doHttpClientV4Get(commonServerUrl));
        scheduledExecutor.scheduleAtFixedRate(futureTask, INITIAL_DELAY, DELAY, TimeUnit.SECONDS);
        return futureTask.get();
    }

    /**
     * 定时线程池执行scheduleWithFixedDelay方法提交线程任务
     *
     * @return 透传标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "scheduleWithFixedDelay", method = RequestMethod.GET)
    public String scheduleWithFixedDelayTest() throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(() -> HttpClientUtils.doHttpClientV4Get(commonServerUrl));
        scheduledExecutor.scheduleWithFixedDelay(futureTask, INITIAL_DELAY, DELAY, TimeUnit.SECONDS);
        return futureTask.get();
    }

    /**
     * 关闭线程池
     *
     * @throws InterruptedException
     */
    @RequestMapping(value = "shutdown", method = RequestMethod.GET)
    public void shutdownThreadPool() throws InterruptedException {
        // 延迟五秒关闭线程池，以防后续线程任务执行
        Thread.sleep(SLEEP);

        if (executor != null) {
            executor.shutdown();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
    }
}
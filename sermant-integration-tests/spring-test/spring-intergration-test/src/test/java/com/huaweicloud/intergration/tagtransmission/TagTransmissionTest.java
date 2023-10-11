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

package com.huaweicloud.intergration.tagtransmission;

import com.huaweicloud.intergration.common.utils.RequestUtils;

import com.alibaba.fastjson.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 流量标签透传插件测试
 *
 * @author daizhenyu
 * @since 2023-10-10
 **/
public class TagTransmissionTest {
    private static final Map<String, String> EXACT_TAG_MAP = new HashMap<>();

    @BeforeAll
    public void before() {
        EXACT_TAG_MAP.put("id", "001");
    }

    @Test
    public void httpClientTest() {
        // 测试 httpclient调用服务端, 包括调用同一进程服务端和不同进程服务端
        Map<String, String> clientUrlMap = new HashMap<>();
        clientUrlMap.put("innerHttpClientV3", "http://127.0.0.1:9041/inner/httpClientV3");
        clientUrlMap.put("innerHttpClientV4", "http://127.0.0.1:9041/inner/httpClientV4");
        clientUrlMap.put("innerOkHttp", "http://127.0.0.1:9041/inner/okHttp");
        clientUrlMap.put("innerJdkHttp", "http://127.0.0.1:9041/inner/jdkHttp");
        clientUrlMap.put("outerHttpClientV3", "http://127.0.0.1:9041/outer/httpClientV3");
        clientUrlMap.put("outerHttpClientV4", "http://127.0.0.1:9041/outer/httpClientV4");
        clientUrlMap.put("outerOkHttp", "http://127.0.0.1:9041/outer/okHttp");
        clientUrlMap.put("outerJdkHttp", "http://127.0.0.1:9041/outer/jdkHttp");
        for (String key : clientUrlMap.keySet()) {
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(clientUrlMap.get(key),
                    EXACT_TAG_MAP));
            Assertions.assertEquals("001", tagMap.get("id"), "httpclient transmit traffic tag failed");
        }
    }

    @Test
    public void threadTest() {
        // 测试 跨线程透传流量标签
        Map<String, String> threadUrlMap = new HashMap<>();
        threadUrlMap.put("newThread", "http://127.0.0.1:9041/thread/newThread");
        threadUrlMap.put("executor", "http://127.0.0.1:9041/thread/executor");
        threadUrlMap.put("submit", "http://127.0.0.1:9041/thread/submit");
        threadUrlMap.put("schedule", "http://127.0.0.1:9041/thread/schedule");
        threadUrlMap.put("scheduleAtFixedRate", "http://127.0.0.1:9041/thread/scheduleAtFixedRate");
        threadUrlMap.put("scheduleWithFixedDelay", "http://127.0.0.1:9041/thread/scheduleWithFixedDelay");
        for (String key : threadUrlMap.keySet()) {
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(threadUrlMap.get(key),
                    EXACT_TAG_MAP));
            Assertions.assertEquals("001", tagMap.get("id"), "cross thread transmit traffic tag failed");
        }

        // 调用服务端提供的接口销毁线程池
        RequestUtils.get("http://127.0.0.1:9041/thread/schedule/shutdown", new HashMap<>());
    }

    @Test
    public void messageMidwareTest() throws InterruptedException {
        // 测试 消息中间件透传流量标签，包括同一进程和不同进程生产消费消息
        Map<String, List<String>> midwareUrlMap = new HashMap<>();
        midwareUrlMap.put("innerRocketmq", Arrays.asList("http://127.0.0.1:9041/produce/rocketmq",
                "http://127.0.0.1:9041/produce/queryRocketmq"));
        midwareUrlMap.put("innerKafka", Arrays.asList("http://127.0.0.1:9041/produce/kafka",
                "http://127.0.0.1:9041/produce/queryKafka"));
        midwareUrlMap.put("outerRocketmq", Arrays.asList("http://127.0.0.1:9042/produce/rocketmq",
                "http://127.0.0.1:9042/produce/queryRocketmq"));
        midwareUrlMap.put("outerKafka", Arrays.asList("http://127.0.0.1:9042/produce/kafka",
                "http://127.0.0.1:9041/produce/queryKafka"));
        for (String key : midwareUrlMap.keySet()) {
            RequestUtils.get(midwareUrlMap.get(key).get(0), EXACT_TAG_MAP);

            // sleep五秒，等待消费者消费
            Thread.sleep(5000);
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(midwareUrlMap.get(key).get(1),
                    new HashMap<>()));
            Assertions.assertEquals("001", tagMap.get("id"), "message midware transmit traffic tag failed");
        }
    }

    @Test
    public void grpcTest() {
        // 测试 grpc组件透传流量标签
        Map<String, String> grpcUrlMap = new HashMap<>();
        grpcUrlMap.put("innerGrpcStub", "http://127.0.0.1:9041/inner/grpcStub");
        grpcUrlMap.put("innerGrpcNoStub", "http://127.0.0.1:9041/inner/grpcNoStub");
        grpcUrlMap.put("outerGrpcStub", "http://127.0.0.1:9041/outer/grpcStub");
        grpcUrlMap.put("outerGrpcNoStub", "http://127.0.0.1:9041/outer/grpcNoStub");
        for (String key : grpcUrlMap.keySet()) {
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(grpcUrlMap.get(key),
                    EXACT_TAG_MAP));
            Assertions.assertEquals("001", tagMap.get("id"), "grpc transmit traffic tag failed");
        }
    }

    @Test
    public void serviceCombByHttpTest() {
        // 测试 通过http访问servicecomb服务端透传流量标签
        Map<String, String> serviceCombUrlMap = new HashMap<>();
        serviceCombUrlMap.put("innerServiceComb", "http://127.0.0.1:9041/inner/serviceCombByHttp");
        serviceCombUrlMap.put("outerServiceComb", "http://127.0.0.1:9041/outer/serviceCombByHttp");
        for (String key : serviceCombUrlMap.keySet()) {
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(serviceCombUrlMap.get(key),
                    EXACT_TAG_MAP));
            Assertions.assertEquals("001", tagMap.get("id"), "grpc transmit traffic tag failed");
        }
    }

    @Test
    public void rpcTest() {
        // 测试 rpc服务透传流量标签，包括alibaba dubbo、apache dubbo、sofarpc、servicecomb rpc，此外本次测试也包括jetty服务端
        Map<String, String> rpcUrlMap = new HashMap<>();
        rpcUrlMap.put("innerRpc", "http://127.0.0.1:9041/inner/dubbo");
        rpcUrlMap.put("outerRpc", "http://127.0.0.1:9043/jetty/httpserver");
        for (String key : rpcUrlMap.keySet()) {
            Map<String, String> tagMap = convertJson2Map(RequestUtils.get(rpcUrlMap.get(key),
                    EXACT_TAG_MAP));
            Assertions.assertEquals("001", tagMap.get("id"), "rpc transmit traffic tag failed");
        }
    }

    @Test
    public void configTest() throws Exception {
        // 测试 流量标签前缀匹配
        Map<String, String> prefixTagMap = new HashMap<>();
        prefixTagMap.put("x-sermant-test", "tag-test-prefix");
        Map<String, String> returnPrefixTagMap = convertJson2Map(
                RequestUtils.get("http://127.0.0.1:9042/outerServer/httpserver", prefixTagMap));
        Assertions.assertEquals("tag-test-prefix", returnPrefixTagMap.get("x-sermant-test"),
                "transmit prefix traffic tag failed");

        // 测试 流量标签后缀匹配
        Map<String, String> suffixTagMap = new HashMap<>();
        suffixTagMap.put("tag-sermant", "tag-test-suffix");
        Map<String, String> returnSuffixTagMap = convertJson2Map(
                RequestUtils.get("http://127.0.0.1:9042/outerServer/httpserver", suffixTagMap));
        Assertions.assertEquals("tag-test-suffix", returnSuffixTagMap.get("tag-sermant"),
                "transmit suffix traffic tag failed");

        // 动态配置
        CuratorFramework curator = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                new ExponentialBackoffRetry(1000, 3));
        curator.start();
        String nodePath = "/sermant/tag-transmission-plugin/tag-config";
        String lineSeparator = System.getProperty("line.separator");
        String oldNodeValue = "enabled: true" + lineSeparator +
                "matchRule:" + lineSeparator +
                "  exact: [\"id\", \"name\"]" + lineSeparator +
                "  prefix: [\"x-sermant-\"]" + lineSeparator +
                "  suffix: [\"-sermant\"]";
        String newNodeValue = "enabled: true" + lineSeparator +
                "matchRule:" + lineSeparator +
                "  exact: [\"dynamic\", \"name\"]";

        Stat stat = curator.checkExists().forPath(nodePath);
        if (stat == null) {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath,
                    newNodeValue.getBytes(StandardCharsets.UTF_8));
        } else {
            curator.setData().forPath(nodePath, newNodeValue.getBytes(StandardCharsets.UTF_8));
        }

        // sleep三秒等待动态配置生效
        Thread.sleep(3000);

        Map<String, String> dynamicTagMap = new HashMap<>();
        dynamicTagMap.put("dynamic", "tag-test-dynamic");
        Map<String, String> returnDynamicTagMap = convertJson2Map(
                RequestUtils.get("http://127.0.0.1:9042/outerServer/httpserver", dynamicTagMap));

        // 切换为原来的流量标签配置
        curator.setData().forPath(nodePath, oldNodeValue.getBytes(StandardCharsets.UTF_8));
        curator.close();
        Assertions.assertEquals("tag-test-dynamic", returnDynamicTagMap.get("dynamic"),
                "dynamic config failed");
    }

    private Map<String, String> convertJson2Map(Optional<String> jsonOptional) {
        Map<String, String> tagMap = new HashMap<>();
        if (jsonOptional.isPresent()) {
            tagMap = JSON.parseObject(jsonOptional.get(), Map.class);
        }
        return tagMap;
    }
}

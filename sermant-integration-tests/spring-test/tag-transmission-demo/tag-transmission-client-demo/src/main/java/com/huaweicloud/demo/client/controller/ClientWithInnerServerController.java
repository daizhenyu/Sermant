/*
 * Copyright (C) 2023-2023 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.demo.client.controller;

import com.huaweicloud.demo.lib.dubbo.service.GreetingInnerService;
import com.huaweicloud.demo.lib.servicecomb.service.ProviderService;
import com.huaweicloud.demo.lib.sofarpc.service.HelloService;
import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import com.alipay.sofa.rpc.config.ConsumerConfig;

import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.servicecomb.provider.pojo.RpcReference;
import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 使用客户端调用同一进程的服务端
 *
 * @author daizhenyu
 * @since 2023-09-07
 **/
@Lazy
@RestController
@RestSchema(schemaId = "InnerConsumerController")
@RequestMapping(value = "inner")
public class ClientWithInnerServerController {
    private static final int SOFARPC_TIMEOUT = 10000;

    @Value("${innerServerUrl}")
    private String innerHttpServerUrl;

    @Value("${inner.sofarpc.url}")
    private String innerSofaRpcUrl;

    @Value("${inner.servicecomb.url}")
    private String innerServiceCombUrl;

    @DubboReference(loadbalance = "random")
    private GreetingInnerService greetingInnerService;

    @RpcReference(schemaId = "InnerProviderController", microserviceName = "demo-consumer")
    private ProviderService providerService;

    /**
     * 验证httpclient3.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpclientv3", method = RequestMethod.GET)
    public String testHttpClientV3() {
        return HttpClientUtils.doHttpClientV3Get(innerHttpServerUrl);
    }

    /**
     * 验证httpclient4.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpclientv4", method = RequestMethod.GET)
    public String testHttpClientV4() {
        return HttpClientUtils.doHttpClientV4Get(innerHttpServerUrl);
    }

    /**
     * 验证okhttp透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "okhttp", method = RequestMethod.GET)
    public String testOkHttp() {
        return HttpClientUtils.doOkHttpGet(innerHttpServerUrl);
    }

    /**
     * 验证jdk http透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "jdkhttp", method = RequestMethod.GET)
    public String testJdkHttp() {
        return HttpClientUtils.doHttpUrlConnectionGet(innerHttpServerUrl);
    }

    /**
     * 验证同一进程的apache dubbo透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "dubbo", method = RequestMethod.GET)
    @ResponseBody
    public String testInnerDubbo() {
        return greetingInnerService.sayHello();
    }

    /**
     * 验证同一进程的sofarpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "sofarpc", method = RequestMethod.GET)
    public String testInnerSofaRpc() {
        ConsumerConfig<HelloService> consumerConfig = new ConsumerConfig<HelloService>()
                .setInterfaceId(HelloService.class.getName())
                .setDirectUrl(innerSofaRpcUrl)
                .setConnectTimeout(SOFARPC_TIMEOUT);
        return consumerConfig.refer().sayHello();
    }

    /**
     * 验证同一进程的servicecomb rpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "servicecomb", method = RequestMethod.GET)
    public String testInnerServiceCombRpc() {
        return providerService.sayHello();
    }

    /**
     * 验证同一进程的servicecomb透传流量标签，使用httpclient调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "servicecomb", method = RequestMethod.GET)
    public String testInnerServiceCombRpcByHttp() {
        return HttpClientUtils.doHttpClientV4Get(innerServiceCombUrl);
    }
}
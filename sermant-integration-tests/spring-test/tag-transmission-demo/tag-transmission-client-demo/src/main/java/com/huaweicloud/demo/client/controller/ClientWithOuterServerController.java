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

import com.huaweicloud.demo.lib.dubbo.service.GreetingOuterService;
import com.huaweicloud.demo.lib.servicecomb.service.ProviderService;
import com.huaweicloud.demo.lib.sofarpc.service.HelloService;
import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import com.alipay.sofa.rpc.config.ConsumerConfig;

import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.servicecomb.provider.pojo.RpcReference;
import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * 使用http客户端调用不同进程的服务端
 *
 * @author daizhenyu
 * @since 2023-09-07
 **/
@Controller
@ResponseBody
@RequestMapping(value = "outer")
@RestSchema(schemaId = "OuterConsumerController")
public class ClientWithOuterServerController {
    private static final int SOFARPC_TIMEOUT = 10000;

    @Value("${outerServerUrl}")
    private String outerHttpServerUrl;

    @Value("${outer.sofarpc.url}")
    private String outerSofaRpcUrl;

    @Value("${outer.servicecomb.url}")
    private String outerServiceCombUrl;

    @DubboReference(loadbalance = "random")
    private GreetingOuterService greetingOuterService;

    @RpcReference(schemaId = "OuterProviderController", microserviceName = "demo-server")
    private ProviderService providerService;

    /**
     * 验证httpclient3.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpclientv3", method = RequestMethod.GET)
    public String testHttpClientV3() {
        return HttpClientUtils.doHttpClientV3Get(outerHttpServerUrl);
    }

    /**
     * 验证httpclient4.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpclientv4", method = RequestMethod.GET)
    public String testHttpClientV4() {
        return HttpClientUtils.doHttpClientV4Get(outerHttpServerUrl);
    }

    /**
     * 验证okhttp透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "okhttp", method = RequestMethod.GET)
    public String testOkHttp() {
        return HttpClientUtils.doOkHttpGet(outerHttpServerUrl);
    }

    /**
     * 验证jdk http透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "jdkhttp", method = RequestMethod.GET)
    public String testJdkHttp() {
        return HttpClientUtils.doHttpUrlConnectionGet(outerHttpServerUrl);
    }

    /**
     * 验证不同进程的apache dubbo透传流量标签
     *
     * @return string 流量标签值
     */
    @RequestMapping(value = "dubbo", method = RequestMethod.GET)
    @ResponseBody
    public String testOuterDubbo() {
        return greetingOuterService.sayHello();
    }

    /**
     * 验证不同进程的sofarpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "sofarpc", method = RequestMethod.GET)
    public String testOuterSofaRpc() {
        ConsumerConfig<HelloService> consumerConfig = new ConsumerConfig<HelloService>()
                .setInterfaceId(HelloService.class.getName())
                .setDirectUrl(outerSofaRpcUrl)
                .setConnectTimeout(SOFARPC_TIMEOUT);
        return consumerConfig.refer().sayHello();
    }

    /**
     * 验证不同进程的servicecomb rpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "servicecomb", method = RequestMethod.GET)
    public String testOuterServiceCombRpc() {
        return providerService.sayHello();
    }

    /**
     * 验证不同进程的servicecomb透传流量标签，使用httpclient调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "servicecomb", method = RequestMethod.GET)
    public String testInnerServiceCombRpcByHttp() {
        return HttpClientUtils.doHttpClientV4Get(outerServiceCombUrl);
    }
}

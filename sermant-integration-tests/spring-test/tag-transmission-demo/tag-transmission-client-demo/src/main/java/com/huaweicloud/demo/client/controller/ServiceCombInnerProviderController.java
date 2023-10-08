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

import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * servicecomb 供同一进程消费端调用的服务端
 *
 * @author daizhenyu
 * @since 2023-10-07
 **/
@RestSchema(schemaId = "InnerProviderController")
@RequestMapping(value = "innerprovider")
public class ServiceCombInnerProviderController {
    @Value("${commonServerUrl}")
    private String commonServerUrl;

    /**
     * 验证同一进程的servicecomb rpc透传流量标签的服务端方法
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "sayhello", method = RequestMethod.GET)
    public String sayHello() {
        return HttpClientUtils.doHttpClientV4Get(commonServerUrl);
    }
}

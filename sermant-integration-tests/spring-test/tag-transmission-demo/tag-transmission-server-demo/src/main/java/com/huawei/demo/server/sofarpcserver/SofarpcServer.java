/*
 * Copyright (C) 2023-2023 Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.demo.server.sofarpcserver;

import com.huawei.demo.server.sofarpc.serviceimpl.HelloServiceSofaRpcImpl;

import com.huaweicloud.demo.lib.sofarpc.service.HelloService;

import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;

/**
 * sofarpc 服务端启动
 *
 * @author daizhenyu
 * @since 2023-09-28
 **/
public class SofarpcServer implements CommandLineRunner {
    @Value("${sofarpc.server.port}")
    private int sofaRpcPort;

    @Override
    public void run(String... args) {
        Thread sofaRpcThread = new Thread(() -> {
            ServerConfig serverConfig = new ServerConfig()
                    .setProtocol("bolt")
                    .setPort(sofaRpcPort)
                    .setDaemon(false);
            ProviderConfig<HelloService> providerConfig = new ProviderConfig<HelloService>()
                    .setInterfaceId(HelloService.class.getName())
                    .setRef(new HelloServiceSofaRpcImpl())
                    .setServer(serverConfig);
            providerConfig.export();
        });
        sofaRpcThread.start();
    }
}

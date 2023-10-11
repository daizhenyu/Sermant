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

import com.huaweicloud.demo.client.utils.GrpcUtils;
import com.huaweicloud.demo.lib.dubbo.service.GreetingInnerService;
import com.huaweicloud.demo.lib.grpc.service.EmptyRequest;
import com.huaweicloud.demo.lib.grpc.service.TagTransmissionTestGrpc;
import com.huaweicloud.demo.lib.grpc.service.TrafficTag;
import com.huaweicloud.demo.lib.servicecomb.service.ProviderService;
import com.huaweicloud.demo.lib.sofarpc.service.HelloService;
import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.servicecomb.provider.pojo.RpcReference;
import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 使用客户端调用同一进程的服务端
 *
 * @author daizhenyu
 * @since 2023-09-07
 **/
@RestSchema(schemaId = "ClientWithInnerServerController")
@RequestMapping(value = "inner")
public class ClientWithInnerServerController {
    private static final int SOFARPC_TIMEOUT = 10000;

    @Value("${innerServerUrl}")
    private String innerHttpServerUrl;

    @Value("${innerSofaRpcUrl}")
    private String innerSofaRpcUrl;

    @Value("${innerServicecombUrl}")
    private String innerServiceCombUrl;

    @Value("${inner.grpc.server.port}")
    private int innerGrpcServerPort;

    @Lazy
    @DubboReference(loadbalance = "random")
    private GreetingInnerService greetingInnerService;

    @RpcReference(schemaId = "InnerProviderController", microserviceName = "demo-consumer")
    private ProviderService providerService;

    /**
     * 验证httpclient3.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpClientV3", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testHttpClientV3() {
        return HttpClientUtils.doHttpClientV3Get(innerHttpServerUrl);
    }

    /**
     * 验证httpclient4.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpClientV4", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testHttpClientV4() {
        return HttpClientUtils.doHttpClientV4Get(innerHttpServerUrl);
    }

    /**
     * 验证okhttp透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "okHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testOkHttp() {
        return HttpClientUtils.doOkHttpGet(innerHttpServerUrl);
    }

    /**
     * 验证jdk http透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "jdkHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testJdkHttp() {
        return HttpClientUtils.doHttpUrlConnectionGet(innerHttpServerUrl);
    }

    /**
     * 验证同一进程的apache dubbo透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "dubbo", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String testInnerDubbo() {
        return greetingInnerService.sayHello();
    }

    /**
     * 验证同一进程的sofarpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "sofaRpc", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
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
    @RequestMapping(value = "serviceComb", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testInnerServiceCombRpc() {
        return providerService.sayHello();
    }

    /**
     * 验证同一进程的servicecomb透传流量标签，使用httpclient调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "serviceCombByHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testInnerServiceCombRpcByHttp() {
        return HttpClientUtils.doHttpClientV4Get(innerServiceCombUrl);
    }

    /**
     * 验证同一进程的grpc透传流量标签，使用stub方式调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "grpcStub", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testInnerGrpcByStub() {
        ManagedChannel originChannel = ManagedChannelBuilder.forAddress("localhost", innerGrpcServerPort)
                .usePlaintext()
                .build();
        TagTransmissionTestGrpc.TagTransmissionTestBlockingStub stub = TagTransmissionTestGrpc
                .newBlockingStub(originChannel);
        TrafficTag trafficTag = stub.testTag(EmptyRequest.newBuilder().build());
        originChannel.shutdown();
        return trafficTag.getTag();
    }

    /**
     * 验证同一进程的grpc透传流量标签，使用dynamic message方式调用服务端
     *
     * @return 流量标签值
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "grpcNoStub", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testInnerGrpcByDynamicMessage() throws ExecutionException, InterruptedException {
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress("localhost", innerGrpcServerPort).usePlaintext().build();

        Descriptors.MethodDescriptor originMethodDescriptor = GrpcUtils.generateProtobufMethodDescriptor();
        MethodDescriptor<DynamicMessage, DynamicMessage> methodDescriptor = GrpcUtils.generateGrpcMethodDescriptor(
                originMethodDescriptor);

        // 创建动态消息
        DynamicMessage request = DynamicMessage.newBuilder(originMethodDescriptor.getInputType()).build();

        // 使用 CompletableFuture 处理异步响应
        CallOptions callOptions = CallOptions.DEFAULT;
        CompletableFuture<DynamicMessage> responseFuture = new CompletableFuture<>();
        ClientCalls.asyncUnaryCall(channel.newCall(methodDescriptor, callOptions), request,
                new StreamObserver<DynamicMessage>() {
                    @Override
                    public void onNext(DynamicMessage value) {
                        responseFuture.complete(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });

        // 等待异步响应完成
        DynamicMessage response = responseFuture.get();
        channel.shutdown();
        return (String) response.getField(originMethodDescriptor.getOutputType().findFieldByName("tag"));
    }
}
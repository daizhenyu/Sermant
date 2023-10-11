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

import com.huaweicloud.demo.client.utils.GrpcUtils;
import com.huaweicloud.demo.lib.dubbo.service.GreetingOuterService;
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
 * 使用http客户端调用不同进程的服务端
 *
 * @author daizhenyu
 * @since 2023-09-07
 **/
@RequestMapping(value = "outer")
@RestSchema(schemaId = "ClientWithOuterServerController")
public class ClientWithOuterServerController {
    private static final int SOFARPC_TIMEOUT = 10000;

    @Value("${outerServerUrl}")
    private String outerHttpServerUrl;

    @Value("${jettyServerUrl}")
    private String jettyServerUrl;

    @Value("${outerSofaRpcUrl}")
    private String outerSofaRpcUrl;

    @Value("${outerServicecombUrl}")
    private String outerServiceCombUrl;

    @Value("${outer.grpc.server.port}")
    private int outerGrpcServerPort;

    @Lazy
    @DubboReference(loadbalance = "random")
    private GreetingOuterService greetingOuterService;

    @RpcReference(schemaId = "OuterProviderController", microserviceName = "demo-server")
    private ProviderService providerService;

    /**
     * 验证httpclient3.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpClientV3", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testHttpClientV3() {
        return HttpClientUtils.doHttpClientV3Get(outerHttpServerUrl);
    }

    /**
     * 验证httpclient4.x透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "httpClientV4", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testHttpClientV4() {
        return HttpClientUtils.doHttpClientV4Get(outerHttpServerUrl);
    }

    /**
     * 验证okhttp透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "okHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testOkHttp() {
        return HttpClientUtils.doOkHttpGet(outerHttpServerUrl);
    }

    /**
     * 验证jdk http透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "jdkHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testJdkHttp() {
        return HttpClientUtils.doHttpUrlConnectionGet(outerHttpServerUrl);
    }

    /**
     * 验证不同进程的apache dubbo透传流量标签
     *
     * @return string 流量标签值
     */
    @RequestMapping(value = "dubbo", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String testOuterDubbo() {
        return greetingOuterService.sayHello();
    }

    /**
     * 验证不同进程的sofarpc透传流量标签
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "sofaRpc", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
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
    @RequestMapping(value = "serviceComb", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testOuterServiceCombRpc() {
        return providerService.sayHello();
    }

    /**
     * 验证不同进程的servicecomb透传流量标签，使用httpclient调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "serviceCombByHttp", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testInnerServiceCombRpcByHttp() {
        return HttpClientUtils.doHttpClientV4Get(outerServiceCombUrl);
    }

    /**
     * 验证不同进程的grpc透传流量标签，使用stub方式调用服务端
     *
     * @return 流量标签值
     */
    @RequestMapping(value = "grpcStub", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public String testOuterGrpcByStub() {
        ManagedChannel originChannel = ManagedChannelBuilder.forAddress("localhost", outerGrpcServerPort)
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
                ManagedChannelBuilder.forAddress("localhost", outerGrpcServerPort).usePlaintext().build();

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

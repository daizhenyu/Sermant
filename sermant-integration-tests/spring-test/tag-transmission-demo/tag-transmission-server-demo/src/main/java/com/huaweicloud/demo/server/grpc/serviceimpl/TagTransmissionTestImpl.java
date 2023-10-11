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

package com.huaweicloud.demo.server.grpc.serviceimpl;

import com.huaweicloud.demo.lib.grpc.service.EmptyRequest;
import com.huaweicloud.demo.lib.grpc.service.TagTransmissionTestGrpc;
import com.huaweicloud.demo.lib.grpc.service.TrafficTag;
import com.huaweicloud.demo.lib.utils.HttpClientUtils;

import io.grpc.stub.StreamObserver;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * grpc服务的实现类
 *
 * @author daizhenyu
 * @since 2023-10-08
 **/
@Component
public class TagTransmissionTestImpl extends TagTransmissionTestGrpc.TagTransmissionTestImplBase {
    @Value("${commonServerUrl}")
    private String commonServerUrl;

    @Override
    public void testTag(EmptyRequest request, StreamObserver<TrafficTag> responseObserver) {
        responseObserver.onNext(TrafficTag.newBuilder()
                .setTag(HttpClientUtils.doHttpClientV4Get(commonServerUrl))
                .build());
        responseObserver.onCompleted();
    }
}

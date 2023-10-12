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

package com.huaweicloud.demo.client.utils;

import com.huaweicloud.demo.lib.grpc.service.TagTestProto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;

/**
 * grpc的工具类
 *
 * @author daizhenyu
 * @since 2023-10-08
 **/
public class GrpcUtils {
    private GrpcUtils() {
    }

    /**
     * 生成grpc的方法描述符
     *
     * @param originMethodDescriptor
     * @return 方法描述符
     */
    public static MethodDescriptor<DynamicMessage, DynamicMessage>
            generateGrpcMethodDescriptor(Descriptors.MethodDescriptor originMethodDescriptor) {
        // 生成方法全名
        String fullMethodName = MethodDescriptor
                .generateFullMethodName(originMethodDescriptor.getService().getFullName(),
                        originMethodDescriptor.getName());

        // 请求和响应类型
        MethodDescriptor.Marshaller<DynamicMessage> inputTypeMarshaller = ProtoUtils
                .marshaller(DynamicMessage.newBuilder(originMethodDescriptor.getInputType())
                .buildPartial());
        MethodDescriptor.Marshaller<DynamicMessage> outputTypeMarshaller = ProtoUtils
                .marshaller(DynamicMessage.newBuilder(originMethodDescriptor.getOutputType())
                .buildPartial());

        // 生成方法描述
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setFullMethodName(fullMethodName)
                .setRequestMarshaller(inputTypeMarshaller)
                .setResponseMarshaller(outputTypeMarshaller)
                // 使用 UNKNOWN，自动修改
                .setType(MethodDescriptor.MethodType.UNKNOWN)
                .build();
    }

    /**
     * 获取protobuf的方法描述符
     *
     * @return 方法描述符
     */
    public static Descriptors.MethodDescriptor generateProtobufMethodDescriptor() {
        // 构建服务存根
        Descriptors.FileDescriptor serviceFileDescriptor = TagTestProto.getDescriptor().getFile();
        Descriptors.ServiceDescriptor serviceDescriptor = serviceFileDescriptor
                .findServiceByName("TagTransmissionTest");
        return serviceDescriptor.getMethods().get(0);
    }
}

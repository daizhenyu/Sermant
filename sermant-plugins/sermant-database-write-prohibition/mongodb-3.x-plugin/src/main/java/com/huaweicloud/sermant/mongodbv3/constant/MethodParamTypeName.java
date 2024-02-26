/*
 *  Copyright (C) 2024-2024 Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huaweicloud.sermant.mongodbv3.constant;

/**
 * 方法参数类型的全限定名常量类
 *
 * @author daizhenyu
 * @since 2024-01-27
 **/
public class MethodParamTypeName {
    /**
     * String类全限定名
     */
    public static final String STRING_CLASS_NAME = "java.lang.String";

    /**
     * WriteBinding类全限定名
     */
    public static final String WRITE_BINDING_CLASS_NAME = "com.mongodb.binding.WriteBinding";

    /**
     * ReadPreference类全限定名
     */
    public static final String READ_PREFERENCE_CLASS_NAME = "com.mongodb.ReadPreference";

    /**
     * FieldNameValidator类全限定名
     */
    public static final String FIELD_NAME_VALIDATOR_CLASS_NAME = "org.bson.FieldNameValidator";

    /**
     * Decoder类全限定名
     */
    public static final String DECODER_CLASS_NAME = "org.bson.codecs.Decoder";

    /**
     * CommandCreator类全限定名
     */
    public static final String COMMAND_CREATOR_CLASS_NAME =
            "com.mongodb.operation.CommandOperationHelper.CommandCreator";

    /**
     * CommandWriteTransformer接口全限定名
     */
    public static final String COMMAND_WRITE_TRANSFORMER_CLASS_NAME =
            "com.mongodb.operation.CommandOperationHelper.CommandWriteTransformer";

    /**
     * CommandTransformer接口全限定名
     */
    public static final String COMMAND_TRANSFORMER_CLASS_NAME =
            "com.mongodb.operation.CommandOperationHelper.CommandTransformer";

    /**
     * Function类全限定名
     */
    public static final String FUNCTION_CLASS_NAME = "com.mongodb.Function";

    /**
     * BsonDocument类全限定名
     */
    public static final String BSON_DOCUMENT_CLASS_NAME = "org.bson.BsonDocument";

    /**
     * Connection类全限定名
     */
    public static final String CONNECTION_CLASS_NAME = "com.mongodb.connection.Connection";

    private MethodParamTypeName() {
    }
}

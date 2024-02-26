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

package com.huaweicloud.sermant.mongodb.utils;

import com.huaweicloud.sermant.core.plugin.agent.declarer.InterceptDeclarer;
import com.huaweicloud.sermant.core.plugin.agent.matcher.ClassMatcher;
import com.huaweicloud.sermant.core.plugin.agent.matcher.MethodMatcher;
import com.huaweicloud.sermant.database.handler.DatabaseHandler;
import com.huaweicloud.sermant.mongodb.constant.MethodParamTypeName;
import com.huaweicloud.sermant.mongodb.interceptors.GeneralExecuteInterceptor;
import com.huaweicloud.sermant.mongodb.interceptors.MixedBulkWriteOperationInterceptor;

/**
 * mongodb拦截点辅助类
 *
 * @author daizhenyu
 * @since 2024-01-16
 **/
public class MongoDbEnhancementHelper {
    private static final String MIXED_BULK_WRITE_CLASS = "com.mongodb.internal.operation.MixedBulkWriteOperation";

    private static final String COMMAND_OPERATION_CLASS = "com.mongodb.internal.operation.CommandOperationHelper";

    private static final String SYNC_OPERATION_CLASS = "com.mongodb.internal.operation.SyncOperationHelper";

    private static final String EXECUTE_METHOD_NAME = "execute";

    private static final String EXECUTE_COMMAND_METHOD_NAME = "executeCommand";

    private static final String EXECUTE_RETRYABLE_COMMAND_METHOD_NAME = "executeRetryableCommand";

    private static final String EXECUTE_RETRYABLE_WRITE = "executeRetryableWrite";

    private static final int METHOD_PARAM_COUNT = 9;

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_FIRST = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.DECODER_CLASS_NAME,
            MethodParamTypeName.CONNECTION_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME
    };

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_SECOND = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.FIELD_NAME_VALIDATOR_CLASS_NAME,
            MethodParamTypeName.DECODER_CLASS_NAME,
            MethodParamTypeName.CONNECTION_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME
    };

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_THIRD = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.FIELD_NAME_VALIDATOR_CLASS_NAME,
            MethodParamTypeName.DECODER_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME
    };

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_FOURTH = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.CONNECTION_CLASS_NAME
    };

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_FIFTH = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.DECODER_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME
    };

    private static final String[] EXECUTE_COMMAND_PARAMS_TYPE_SIXTH = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.BSON_DOCUMENT_CLASS_NAME,
            MethodParamTypeName.CONNECTION_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME
    };

    private static final String[] EXECUTE_RETRY_COMMAND_PARAMS_TYPE = {
            MethodParamTypeName.WRITE_BINDING_CLASS_NAME,
            MethodParamTypeName.STRING_CLASS_NAME,
            MethodParamTypeName.READ_PREFERENCE_CLASS_NAME,
            MethodParamTypeName.FIELD_NAME_VALIDATOR_CLASS_NAME,
            MethodParamTypeName.DECODER_CLASS_NAME,
            MethodParamTypeName.COMMAND_CREATOR_CLASS_NAME,
            MethodParamTypeName.COMMAND_WRITE_TRANSFORMER_CLASS_NAME,
            MethodParamTypeName.FUNCTION_CLASS_NAME
    };

    private MongoDbEnhancementHelper() {
    }

    /**
     * 获取CommandOperationHelper类的ClassMatcher
     *
     * @return ClassMatcher 类匹配器
     */
    public static ClassMatcher getCommandOperationHelperClassMatcher() {
        return ClassMatcher.nameEquals(COMMAND_OPERATION_CLASS);
    }

    /**
     * 获取MixedBulkWriteOperation类的ClassMatcher
     *
     * @return ClassMatcher 类匹配器
     */
    public static ClassMatcher getMixedBulkWriteOperationClassMatcher() {
        return ClassMatcher.nameEquals(MIXED_BULK_WRITE_CLASS);
    }

    /**
     * 获取SyncOperationHelperDeclarer类的ClassMatcher
     *
     * @return ClassMatcher 类匹配器
     */
    public static ClassMatcher getSyncOperationHelperDeclarerClassMatcher() {
        return ClassMatcher.nameEquals(SYNC_OPERATION_CLASS);
    }

    /**
     * 获取MixedBulkWriteOperation execute方法无参拦截器
     *
     * @return InterceptDeclarer MixedBulkWriteOperation execute方法无参拦截器
     */
    public static InterceptDeclarer getExecuteInterceptDeclarer() {
        return InterceptDeclarer.build(getExecuteMethodMatcher(),
                new MixedBulkWriteOperationInterceptor());
    }

    /**
     * 获取MixedBulkWriteOperation execute方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer MixedBulkWriteOperation execute方法有参拦截器
     */
    public static InterceptDeclarer getExecuteInterceptDeclarer(DatabaseHandler handler) {
        return InterceptDeclarer.build(getExecuteMethodMatcher(),
                new MixedBulkWriteOperationInterceptor(handler));
    }

    /**
     * 获取SyncOperationHelper类 无参拦截器数组
     *
     * @return InterceptDeclarer[] SyncOperationHelper 无参拦截器数组
     */
    public static InterceptDeclarer[] getSyncOperationHelperInterceptDeclarers() {
        GeneralExecuteInterceptor generalExecuteInterceptor = new GeneralExecuteInterceptor();
        return new InterceptDeclarer[]{
                InterceptDeclarer.build(getExecuteCommandMethodMatcher(), generalExecuteInterceptor),
                InterceptDeclarer.build(getExecuteRetryableWriteMethodMatcher(), generalExecuteInterceptor)
        };
    }

    /**
     * 获取SyncOperationHelper executeCommand方法无参拦截器
     *
     * @return InterceptDeclarer SyncOperationHelper executeCommand方法无参拦截器
     */
    public static InterceptDeclarer getExecuteCommandInterceptDeclarer4Sync() {
        return InterceptDeclarer.build(getExecuteCommandMethodMatcher(),
                new GeneralExecuteInterceptor());
    }

    /**
     * 获取SyncOperationHelper executeCommand方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer SyncOperationHelper executeCommand方法有参拦截器
     */
    public static InterceptDeclarer getExecuteCommandInterceptDeclarer4Sync(DatabaseHandler handler) {
        return InterceptDeclarer.build(getExecuteCommandMethodMatcher(),
                new GeneralExecuteInterceptor(handler));
    }

    /**
     * 获取SyncOperationHelper executeRetryableWrite方法无参拦截器
     *
     * @return InterceptDeclarer SyncOperationHelper executeRetryableWrite方法无参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableWriteInterceptDeclarer4Sync() {
        return InterceptDeclarer.build(getExecuteRetryableWriteMethodMatcher(),
                new GeneralExecuteInterceptor());
    }

    /**
     * 获取SyncOperationHelper executeRetryableWrite方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer SyncOperationHelper executeRetryableWrite方法有参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableWriteInterceptDeclarer4Sync(DatabaseHandler handler) {
        return InterceptDeclarer.build(getExecuteCommandMethodMatcher(),
                new GeneralExecuteInterceptor(handler));
    }

    /**
     * 获取CommandOperationHelper写操作无参拦截器数组
     *
     * @return InterceptDeclarer[] CommandOperationHelper写操作无参拦截器数组
     */
    public static InterceptDeclarer[] getCommandOperationHelperInterceptDeclarers() {
        GeneralExecuteInterceptor generalExecuteInterceptor = new GeneralExecuteInterceptor();
        return new InterceptDeclarer[]{
                InterceptDeclarer.build(getExecuteCommandMethodMatcher4CommandOperation(),
                        generalExecuteInterceptor),
                InterceptDeclarer.build(getExecuteRetryableCommandMethodMatcher4CommandOperation(),
                        generalExecuteInterceptor),
                InterceptDeclarer.build(getExecuteRetryableWriteMethodMatcher(),
                        generalExecuteInterceptor)
        };
    }

    /**
     * 获取CommandOperationHelper executeCommand方法无参拦截器
     *
     * @return InterceptDeclarer CommandOperationHelper executeCommand方法无参拦截器
     */
    public static InterceptDeclarer getExecuteCommandInterceptDeclarer() {
        return InterceptDeclarer
                .build(getExecuteCommandMethodMatcher4CommandOperation(), new GeneralExecuteInterceptor());
    }

    /**
     * 获取CommandOperationHelper executeCommand方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer CommandOperationHelper executeCommand方法有参拦截器
     */
    public static InterceptDeclarer getExecuteCommandInterceptDeclarer(DatabaseHandler handler) {
        return InterceptDeclarer
                .build(getExecuteCommandMethodMatcher4CommandOperation(), new GeneralExecuteInterceptor(handler));
    }

    /**
     * 获取CommandOperationHelper executeRetryableCommand方法无参拦截器
     *
     * @return InterceptDeclarer CommandOperationHelper executeRetryableCommand方法无参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableCommandInterceptDeclarer() {
        return InterceptDeclarer
                .build(getExecuteRetryableCommandMethodMatcher4CommandOperation(),
                        new GeneralExecuteInterceptor());
    }

    /**
     * 获取CommandOperationHelper executeRetryableCommand方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer CommandOperationHelper executeRetryableCommand方法有参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableCommandInterceptDeclarer(
            DatabaseHandler handler) {
        return InterceptDeclarer
                .build(getExecuteRetryableCommandMethodMatcher4CommandOperation(),
                        new GeneralExecuteInterceptor(handler));
    }

    /**
     * 获取CommandOperationHelper executeRetryableWrite方法无参拦截器
     *
     * @return InterceptDeclarer CommandOperationHelper executeRetryableWrite方法无参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableWriteInterceptDeclarer() {
        return InterceptDeclarer
                .build(getExecuteRetryableWriteMethodMatcher(),
                        new GeneralExecuteInterceptor());
    }

    /**
     * 获取CommandOperationHelper executeRetryableWrite方法有参拦截器
     *
     * @param handler 数据库自定义处理器
     * @return InterceptDeclarer CommandOperationHelper executeRetryableWrite方法有参拦截器
     */
    public static InterceptDeclarer getExecuteRetryableWriteInterceptDeclarer(
            DatabaseHandler handler) {
        return InterceptDeclarer
                .build(getExecuteRetryableWriteMethodMatcher(),
                        new GeneralExecuteInterceptor(handler));
    }

    private static MethodMatcher getExecuteMethodMatcher() {
        return MethodMatcher.nameEquals(EXECUTE_METHOD_NAME);
    }

    private static MethodMatcher getExecuteCommandMethodMatcher4CommandOperation() {
        return (MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_FIRST)))
                .or(MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_SECOND)))
                .or(MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_THIRD)))
                .or(MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_FOURTH)))
                .or(MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_FIFTH)))
                .or(MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME)
                        .and(MethodMatcher.paramTypesEqual(EXECUTE_COMMAND_PARAMS_TYPE_SIXTH)));
    }

    private static MethodMatcher getExecuteRetryableCommandMethodMatcher4CommandOperation() {
        return MethodMatcher.nameEquals(EXECUTE_RETRYABLE_COMMAND_METHOD_NAME)
                .and(MethodMatcher.paramTypesEqual(EXECUTE_RETRY_COMMAND_PARAMS_TYPE));
    }

    private static MethodMatcher getExecuteCommandMethodMatcher() {
        return MethodMatcher.nameEquals(EXECUTE_COMMAND_METHOD_NAME);
    }

    private static MethodMatcher getExecuteRetryableWriteMethodMatcher() {
        return MethodMatcher.nameEquals(EXECUTE_RETRYABLE_WRITE);
    }
}

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

package com.huaweicloud.demo.utils;

import com.huaweicloud.demo.common.Constant;

import javax.servlet.http.HttpServletRequest;

/**
 * String 工具类
 *
 * @author daizhenyu
 * @since 2023-09-11
 **/
public class StringUtils {
    private static final int BOUND_LENGTH = 2;

    private StringUtils() {
    }

    /**
     * 将http header中的tag表示为json字符串形式
     *
     * @param request
     * @return string
     */
    public static String convertHeader2String(HttpServletRequest request) {
        return buildString(request);
    }

    private static String buildString(HttpServletRequest request) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (String key : Constant.TRAFFIC_TAG_KEY) {
            if (request.getHeader(key) == null) {
                continue;
            }
            builder.append(key);
            builder.append(":");
            builder.append(request.getHeader(key));
            builder.append(",");
        }
        if (builder.length() >= BOUND_LENGTH) {
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("}");
        return builder.toString();
    }
}

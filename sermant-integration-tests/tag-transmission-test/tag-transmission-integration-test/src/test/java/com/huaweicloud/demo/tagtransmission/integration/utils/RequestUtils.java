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

package com.huaweicloud.demo.tagtransmission.integration.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.Map;
import java.util.Optional;

/**
 * 请求工具类
 *
 * @author daizhenyu
 * @since 2023-10-16
 */
public class RequestUtils {
    private RequestUtils() {
    }

    /**
     * httpclient get方法
     *
     * @param url 请求地址
     * @param headers 需要添加的header
     * @return Optional<String> 包含response的optional
     */
    public static Optional<String> get(String url, Map<String, String> headers) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        for (String key : headers.keySet()) {
            httpGet.addHeader(key, headers.get(key));
        }
        String responseContext = null;
        try {
            CloseableHttpResponse response = httpClient.execute(httpGet);
            responseContext = EntityUtils.toString(response.getEntity());

            // 关闭响应和 HttpClient
            response.close();
            httpClient.close();
        } catch (Exception e) {
        }
        return Optional.ofNullable(responseContext);
    }
}

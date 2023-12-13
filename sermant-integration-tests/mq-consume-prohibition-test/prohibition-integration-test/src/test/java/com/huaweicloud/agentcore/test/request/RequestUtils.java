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

package com.huaweicloud.agentcore.test.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * http请求工具类
 *
 * @author tangle
 * @since 2023-09-26
 */
public class RequestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestUtils.class);

    private RequestUtils() {
    }

    public static List<String> doPostGetList(String url, Map<String, Object> params){
        return convertHttpEntityToList(doPost(url, params));
    }

    /**
     * http的get请求
     *
     * @param url http请求url
     * @return 响应体body
     */
    public static String doPost(String url, Map<String, Object> params) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            RequestConfig requestConfig = RequestConfig.custom()
                    .build();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(convertMaptoHttpEntity(params));
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    return EntityUtils.toString(response.getEntity());
                } else {
                    LOGGER.info("Request error, the message is: {}", EntityUtils.toString(response.getEntity()));
                    return "";
                }
            }
        } catch (IOException e) {
            LOGGER.info("Request exception, the message is: {}", e.getMessage());
            return "";
        }
    }

    private static StringEntity convertMaptoHttpEntity(Map<String, Object> params){
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(params);
        return new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON);
    }

    /**
     * JSON数据转换为List
     *
     * @param response JSON数据
     * @return map数据
     */
    private static List<String> convertHttpEntityToList(String response) {
        JSONArray jsonArray = JSON.parseArray(response);
        List<String> result = new ArrayList<>();
        for (Object jsonObject : jsonArray){
            result.add((String) jsonObject);
        }
        return result;
    }
}

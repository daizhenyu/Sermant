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

package com.huaweicloud.demo.lib.utils;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * http客户端工具类
 *
 * @author daizhenyu
 * @since 2023-09-07
 **/
public class HttpClientUtils {
    private HttpClientUtils() {
    }

    /**
     * httpclient3.x get方法工具类
     *
     * @param url
     * @return http请求的response
     */
    public static String doHttpClientV3Get(String url) {
        // 创建 HttpClient 实例
        HttpClient httpClient = new HttpClient();
        GetMethod getMethod = new GetMethod(url);
        String responseContext = null;
        try {
            // 执行 GET 请求
            httpClient.executeMethod(getMethod);
            responseContext = getMethod.getResponseBodyAsString();
        } catch (IOException e) {
            // ignore
        } finally {
            // 释放连接
            getMethod.releaseConnection();
        }
        return responseContext;
    }

    /**
     * httpclient4.x get方法工具类
     *
     * @param url
     * @return http请求的response
     */
    public static String doHttpClientV4Get(String url) {
        // 创建 CloseableHttpClient 实例
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        String responseContext = null;
        try {
            // 执行 GET 请求
            CloseableHttpResponse response = httpClient.execute(httpGet);
            responseContext = EntityUtils.toString(response.getEntity());
            response.close();
        } catch (IOException e) {
            // ignore
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // ignore
            }
        }
        return responseContext;
    }

    /**
     * okhttp get方法工具类
     *
     * @param url
     * @return http请求的response
     */
    public static String doOkHttpGet(String url) {
        OkHttpClient client = new OkHttpClient();

        // 创建 HTTP 请求
        Request request = new Request.Builder()
                .url(url)
                .build();
        String responseContext = null;
        try {
            // 执行请求
            Response response = client.newCall(request).execute();
            responseContext = response.body().string();
            response.body().close();
        } catch (IOException e) {
            // ignore
        }
        return responseContext;
    }

    /**
     * jdkhttp get方法工具类
     *
     * @param url
     * @return http请求的response
     */
    public static String doHttpUrlConnectionGet(String url) {
        String responseContext = null;
        try {
            URL serverUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) serverUrl.openConnection();
            connection.setRequestMethod("GET");

            // 获取响应码
            connection.getResponseCode();

            // 读取响应数据
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            responseContext = content.toString();
            in.close();

            // 关闭连接
            connection.disconnect();
        } catch (IOException e) {
            // ignore
        }
        return responseContext;
    }
}

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

package com.huaweicloud.prohibitionkafkaproducerdemo.controller;

import com.huaweicloud.prohibitionkafkaproducerdemo.kafkaproducer.KafkaProducerClient;
import com.huaweicloud.prohibitionkafkaproducerdemo.kafkaproducer.KafkaProducerService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 消息中间件消费者controller
 *
 * @author daizhenyu
 * @since 2023-09-28
 **/
@RestController
@RequestMapping("/kafkaProducer")
public class KafkaProducerController {
    @Autowired
    KafkaProducerService kafkaProducerService;

    @RequestMapping("/testProducer")
    public void testProducer() {
    }

    @RequestMapping("/newProducer")
    public String newConsumer(@RequestBody Map<String, Object> param) {
        System.out.println("address is true:" + param.get("address"));
        System.out.println("name is true:" + param.get("name"));
        System.out.println("list is true:" + param.get("topic"));
        try {
            kafkaProducerService.initKafkaProducer((String) param.get("address"), (String) param.get("name"),
                    (String) param.get("topic"));
            return "success";
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
            return "fail";
        }
    }

    @RequestMapping("/startProduce")
    public void startProduce(@RequestBody Map<String, Object> param) {
        kafkaProducerService.startProduce((String) param.get("name"));
    }

    @RequestMapping("/produce")
    @ResponseBody
    public boolean produce(@RequestBody Map<String, Object> param) {
        return kafkaProducerService.produce((String) param.get("msg"), (String) param.get("name"));
    }

    @RequestMapping("/close")
    @ResponseBody
    public void close(@RequestBody Map<String, Object> param) {
        kafkaProducerService.close((String) param.get("name"));
    }
}

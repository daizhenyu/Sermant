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

package com.huaweicloud.sermant.database.prohibition.integration.mariadb;

import com.huaweicloud.sermant.database.prohibition.integration.utils.DynamicConfigUtils;
import com.huaweicloud.sermant.database.prohibition.integration.utils.HttpRequestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * mariadb write prohibition integration test
 *
 * @author daizhenyu
 * @since 2024-03-12
 **/
public class MariaDbProhibitionTest {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String MARIADB_CONFIG_ON = "enableMySqlWriteProhibition: true" + LINE_SEPARATOR +
            "mySqlDatabases:" + LINE_SEPARATOR +
            " - test";

    private static final String MARIADB_CONFIG_OFF = "enableMySqlWriteProhibition: false" + LINE_SEPARATOR +
            "mySqlDatabases:" + LINE_SEPARATOR +
            " - test";

    @BeforeAll
    public static void before() throws Exception {
        DynamicConfigUtils.updateConfig(MARIADB_CONFIG_OFF);
        Thread.sleep(3000);

        // prepare test data
        HttpRequestUtils.doGet("http://127.0.0.1:9098/static/createTable?table=tableSelect");
        HttpRequestUtils.doGet("http://127.0.0.1:9098/static/insert?table=tableSelect");
        HttpRequestUtils.doGet("http://127.0.0.1:9098/static/createTable?table=tableDrop");
        HttpRequestUtils.doGet("http://127.0.0.1:9098/static/createTable?table=tableData");
        HttpRequestUtils.doGet("http://127.0.0.1:9098/static/insert?table=tableData");

        DynamicConfigUtils.updateConfig(MARIADB_CONFIG_ON);
        Thread.sleep(3000);
    }

    /**
     * select
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches = "MARIADB")
    public void testSelect() {
        Assertions.assertEquals("1", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/select?table=tableSelect"));
        Assertions.assertEquals("101", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/select?table=tableSelect"));
        Assertions.assertEquals("1", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/select?table=tableSelect"));
    }

    /**
     * create table
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testTable() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/createTable?table=tableCreate"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/createTable?table=tableCreate"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/createTable?table=tableCreate"));
    }

    /**
     * drop table
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testDropCollection() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/dropTable?table=tableDrop"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/dropTable?table=tableDrop"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/dropTable?table=tableDrop"));
    }

    /**
     * insert
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testInsert() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/insert?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/insert?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/insert?table=tableData"));
    }

    /**
     * update
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testUpdate() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/update?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/update?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/update?table=tableData"));
    }

    /**
     * delete
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testDelete() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/delete?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/delete?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/delete?table=tableData"));
    }

    /**
     * create index
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testCreateIndex() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/creatIndex?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/creatIndex?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/creatIndex?table=tableData"));
    }

    /**
     * delete index
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testDeleteIndex() throws Exception {
        DynamicConfigUtils.updateConfig(MARIADB_CONFIG_OFF);
        Thread.sleep(3000);

        Assertions.assertEquals("101", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/creatIndex?table=tableData"));
        Assertions.assertEquals("101", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/creatIndex?table=tableData"));
        Assertions.assertEquals("101", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/creatIndex?table=tableData"));

        DynamicConfigUtils.updateConfig(MARIADB_CONFIG_ON);
        Thread.sleep(3000);

        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/dropIndex?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/dropIndex?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/dropIndex?table=tableData"));
    }

    /**
     * alter table
     */
    @Test
    @EnabledIfSystemProperty(named = "database.write.prohibition.integration.test.type", matches =
            "MARIADB")
    public void testAlterTable() {
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/static/alterTable?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/batch/alterTable?table=tableData"));
        Assertions.assertEquals("100", HttpRequestUtils
                .doGet("http://127.0.0.1:9098/prepared/alterTable?table=tableData"));
    }
}

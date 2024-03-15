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

package com.huaweicloud.sermant.demo.mariadb.controller;

import com.huaweicloud.sermant.database.prohibition.common.constant.DatabaseConstant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * operate mysql by static sql
 *
 * @author daizhenyu
 * @since 2024-03-12
 **/
@RequestMapping("static")
@RestController
public class MariadbStaticSqlController {
    @Value("${mysql.address}")
    private String mysqlAddress;

    /**
     * check running status
     *
     * @return running status
     */
    @RequestMapping("checkStatus")
    public String ping() {
        return "ok";
    }

    /**
     * createTable
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("createTable")
    public int createTable(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String createTableQuery =
                    "CREATE TABLE " + table + " (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255),"
                            + " age INT)";
            statement.execute(createTableQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
            System.out.println(e.getMessage());
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * dropTable
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("dropTable")
    public int dropTable(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String dropTableQuery = "DROP TABLE IF EXISTS " + table;
            statement.executeUpdate(dropTableQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * createIndex
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("creatIndex")
    public int createIndex(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String createIndexQuery = "CREATE INDEX idx_name ON " + table + " (name)";
            statement.executeUpdate(createIndexQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * dropIndex
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("dropIndex")
    public int dropIndex(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String dropIndexQuery = "DROP INDEX idx_name ON " + table;
            statement.executeUpdate(dropIndexQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * alterTable
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("alterTable")
    public int alterTable(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String alterTableQuery = "ALTER TABLE " + table + " ADD COLUMN address VARCHAR(255)";
            statement.executeUpdate(alterTableQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * insert
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("insert")
    public int insert(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String insertQuery = "INSERT INTO " + table + " (name, age) VALUES ('John Doe', 25)";
            statement.executeUpdate(insertQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * update
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("update")
    public int update(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String updateQuery = "UPDATE " + table + " SET age = 26 WHERE id = 1";
            statement.executeUpdate(updateQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * delete
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("delete")
    public int delete(String table) {
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String deleteQuery = "DELETE FROM " + table + " WHERE id = 1";
            statement.executeUpdate(deleteQuery);
        } catch (SQLException e) {
            if (e.getMessage().contains(DatabaseConstant.SQL_EXCEPTION_MESSAGE_PREFIX)) {
                return DatabaseConstant.SUCCEED_PROHIBITION_CODE;
            }
        }
        return DatabaseConstant.FAILED_PROHIBITION_CODE;
    }

    /**
     * select
     *
     * @param table table name
     * @return int prohibition status code
     */
    @RequestMapping("select")
    public int select(String table) {
        int rowCount = 0;
        try (Connection connection = DriverManager.getConnection(mysqlAddress)) {
            Statement statement = connection.createStatement();
            String selectQuery = "SELECT * FROM " + table;
            ResultSet resultSet = statement.executeQuery(selectQuery);
            rowCount = countRows(resultSet);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return rowCount;
    }

    private int countRows(ResultSet resultSet) throws SQLException {
        int rowCount = 0;
        if (resultSet != null) {
            resultSet.last();
            rowCount = resultSet.getRow();
            resultSet.beforeFirst();
        }
        return rowCount;
    }
}
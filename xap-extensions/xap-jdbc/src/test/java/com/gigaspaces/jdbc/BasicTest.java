/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc;

import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.*;
import java.util.Properties;

public class BasicTest {

    @Test
    public void test() throws SQLException {
        createAndFillSpace();

        Properties properties = new Properties();
        properties.put("useNewDriver", true);

        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:url:jini://*/*/demo", properties)) {
            Statement statement = connection.createStatement();
            String sqlQuery = "select name from com.gigaspaces.jdbc.MyPojo";

//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo AS A";
//            String sqlQuery = "select name from (select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name)";
//            String sqlQuery = "select name AS name2 from (select name from com.gigaspaces.jdbc.MyPojo)";
//            String sqlQuery = "select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//            String sqlQuery = "explain select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
            ResultSet res = statement.executeQuery(sqlQuery);
            DumpUtils.dump(res);
        }
    }

    private void createAndFillSpace() {
        GigaSpace gigaSpace = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("demo")).gigaSpace();
        gigaSpace.write(new MyPojo("Adler", 20, "Israel"));
        gigaSpace.write(new MyPojo("Adam", 30, "Israel"));
        gigaSpace.write(new MyPojo("Eve", 35, "UK"));
    }
}

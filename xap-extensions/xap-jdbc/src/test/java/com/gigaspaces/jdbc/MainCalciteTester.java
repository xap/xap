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

import com.gigaspaces.jdbc.data.Customer;
import com.gigaspaces.jdbc.data.DataGenerator;
import com.gigaspaces.jdbc.data.Product;
import com.gigaspaces.jdbc.data.Purchase;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class MainCalciteTester {
    public static void main(String[] args) throws SQLException, ParseException {
        GigaSpace space = createAndFillSpace(true, true);

        Properties properties = new Properties();
        properties.put("com.gs.embeddedQP.enabled", "true");

        try {
            Class.forName("com.j_spaces.jdbc.driver.GDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:v3://localhost:4174/" + space.getSpaceName(), properties)) {
            Statement statement = connection.createStatement();

            //Simple select
            execute(statement, String.format("SELECT * FROM %s", "\"" + MyPojo.class.getName() + "\""));
            // select with one projection
            execute(statement, String.format("SELECT first_name FROM %s", "\"" + MyPojo.class.getName() + "\""));
            // select with one aliased projection
            execute(statement, String.format("SELECT first_name as first FROM %s", "\"" + MyPojo.class.getName() + "\""));
            // select all with one filter
            execute(statement, String.format("SELECT * FROM %s where last_name = 'Aa'", "\"" + MyPojo.class.getName() + "\""));
            // select all with one projection + one filter
            execute(statement, String.format("SELECT first_name FROM %s where last_name = 'Aa'", "\"" + MyPojo.class.getName() + "\""));
            // select all with filter AND filter
            execute(statement, String.format("SELECT * FROM %s where last_name = 'Aa' AND first_name = 'Adam'", "\"" + MyPojo.class.getName() + "\""));
            // select all with filter OR filter
            execute(statement, String.format("SELECT * FROM %s where last_name = 'Aa' OR first_name = 'Adam'", "\"" + MyPojo.class.getName() + "\""));
            // select all join two tables
            execute(statement, String.format("select * from %s as c " +
                    "inner join " +
                    "%s as purchase " +
                    "on c.id = purchase.customerId", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
            // select with projection join two tables
            execute(statement, String.format("select c.first_name,purchase.productId from %s as c " +
                    "inner join " +
                    "%s as purchase " +
                    "on c.id = purchase.customerId", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
            // select all join two tables + filter
            execute(statement, String.format("select * from %s as c " +
                    "inner join " +
                    "%s as purchase " +
                    "on c.id = purchase.customerId where c.last_name = 'Ericsson' and purchase.amount > 3", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
            // select join two tables + filter + projection
            execute(statement, String.format("select c.first_name,purchase.productId from %s as c " +
                    "inner join " +
                    "%s as purchase " +
                    "on c.id = purchase.customerId where c.last_name = 'Ericsson' and purchase.amount > 3", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
            // select all, three tables
            execute(statement, String.format("select * from %s as c " +
                    "inner join " +
                    "%s as purchase on c.id = purchase.customerId " +
                    "inner join " +
                    "%s as p on purchase.productId = p.id", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\"", "\"" + Product.class.getName() + "\""));
//            execute(statement, String.format("SELECT first_name as first, last_name as last FROM %s where last_name = 'Bb'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>40))", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where T.last_name = 'Bb' or T.first_name = 'Adam' or T.last_name = 'Cc' or T.email = 'Adler@msn.com' or T.age>=40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, "SELECT * FROM com.gigaspaces.jdbc.MyPojo as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>=40))");
//            execute(statement, String.format("SELECT * FROM %s as T where T.age <= 40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("EXPLAIN PLAN FOR SELECT * FROM %s ", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("select c.first_name,purchase.productId from %s as c " +
//                    "inner join " +
//                    "%s as purchase " +
//                    "on c.id = purchase.customerId where c.last_name = 'Ericsson' and purchase.amount > 3", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
//            execute(statement, String.format("select * from %s as c " +
//                    "inner join " +
//                    "%s as purchase " +
//                    "on c.id = purchase.customerId where c.last_name = 'Ericsson' and purchase.amount > 3", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
//            execute(statement, String.format("SELECT name, first_name, last_name, email, age, country, birthDate, birthTime, birthLong FROM %s where last_name = 'Bb'", "\"" + MyPojo.class.getName() + "\""));
        }
    }

    private static void execute(Statement statement, String sql) throws SQLException {
        System.out.println();
        System.out.println("Executing: " + sql);
        ResultSet res = statement.executeQuery(sql);
        DumpUtils.dump(res);

    }

    private static GigaSpace createAndFillSpace(boolean newDriver, boolean embedded) throws ParseException {
        String spaceName = "demo" + (newDriver ? "new" : "old");
        AbstractSpaceConfigurer configurer = embedded ? new EmbeddedSpaceConfigurer(spaceName)
                .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
                .addProperty("com.gs.jdbc.v3.driver", "calcite")
//                .tieredStorage(new TieredStorageConfigurer().addTable(new TieredStorageTableConfig().setName(MyPojo.class.getName()).setCriteria("age > 20")))
                : new SpaceProxyConfigurer(spaceName);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (embedded || gigaSpace.count(null) == 0) {
            java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00.231");
            java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00.250");
            java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00.100");
            java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00.300");
            gigaSpace.write(new MyPojo("Adler Aa", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
            gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
            gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
            gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
        }
        DataGenerator.writeProduct(gigaSpace);
        DataGenerator.writePurchase(gigaSpace);
        DataGenerator.writeCustomer(gigaSpace);
        DataGenerator.writeInventory(gigaSpace);
        return gigaSpace;
    }
}

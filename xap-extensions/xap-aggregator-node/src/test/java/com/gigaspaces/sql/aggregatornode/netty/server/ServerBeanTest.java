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
package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class ServerBeanTest extends AbstractServerTest{
    @BeforeAll
    static void setUp() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        Class.forName("org.postgresql.Driver");

        gigaSpace = new GigaSpaceConfigurer(
                new EmbeddedSpaceConfigurer(SPACE_NAME)
                        .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
        ).gigaSpace();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00.231");
        java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00.250");
        java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00.100");
        java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00.300");
        gigaSpace.write(new MyPojo("Adler Aa", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
        gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
        gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
        gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConnection(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(1000));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSet(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final Statement statement = conn.createStatement();
            assertEquals(1, statement.executeUpdate("SET DateStyle = 'ISO'"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShow(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final Statement statement = conn.createStatement();
            assertTrue(statement.execute("SHOW DateStyle"));
            ResultSet res = statement.getResultSet();
            assertNotNull(res);
            assertTrue(res.next());
            assertEquals("ISO, MDY", res.getString(1));
        }
    }

    // TODO return test over extended query protocol after parameters support by SqlValidator implemented
    @ParameterizedTest
    @ValueSource(booleans = {true/*, false */})
    void testParametrized(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("SELECT first_name, last_name, email, age FROM \"%s\" as T where T.last_name = ? OR T.first_name = ?", MyPojo.class.getName());
            final PreparedStatement statement = conn.prepareStatement(qry);
            statement.setString(1, "Aa");
            statement.setString(2, "Adam");

            assertTrue(statement.execute());

            // TODO since runtime doesn't support dynamic parameters at now there is no results checking
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDateTypes(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("SELECT birthDate, birthTime, \"timestamp\" FROM \"%s\"", MyPojo.class.getName());
            final PreparedStatement statement = conn.prepareStatement(qry);

            assertTrue(statement.execute());

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
            ArrayList<java.util.Date> dates = new ArrayList<>();
            dates.add(simpleDateFormat.parse("10/09/2001 05:20:00.231"));
            dates.add(simpleDateFormat.parse("11/09/2001 10:20:00.250"));
            dates.add(simpleDateFormat.parse("12/09/2001 15:20:00.100"));
            dates.add(simpleDateFormat.parse("13/09/2001 20:20:00.300"));

            ResultSet res = statement.getResultSet();
            for (int i = 0; i < 4; i++) {
                assertTrue(res.next());
                Date date = dates.get(i);
                assertEquals(res.getTimestamp(1), new Timestamp(date.getTime()));
                assertEquals(res.getTime(2).toString(), new Time(date.getTime()).toString());
                assertEquals(res.getTimestamp(3), new Timestamp(date.getTime()));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTimeZone(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");

            String qry = String.format("SELECT \"timestamp\" FROM \"%s\" where first_name = 'Adam'", MyPojo.class.getName());
            try (PreparedStatement statement = conn.prepareStatement(qry)) {
                assertTrue(statement.execute());
                Date date = simpleDateFormat.parse("11/09/2001 10:20:00.250");

                ResultSet res = statement.getResultSet();
                assertTrue(res.next());
                assertEquals(new Timestamp(date.getTime()), res.getTimestamp(1));
            }

            qry = "SET TimeZone='GMT-1'"; // PG uses posix timezones which are negated
            try (PreparedStatement statement = conn.prepareStatement(qry)) {
                assertEquals(1, statement.executeUpdate());
            }

            qry = String.format("SELECT \"timestamp\" FROM \"%s\" where first_name = 'Adam'", MyPojo.class.getName());
            try (PreparedStatement statement = conn.prepareStatement(qry)) {
                assertTrue(statement.execute());
                Date date = simpleDateFormat.parse("11/09/2001 11:20:00.250");

                ResultSet res = statement.getResultSet();
                assertTrue(res.next());
                assertEquals(new Timestamp(date.getTime()), res.getTimestamp(1));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmptyTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_am where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| oid | amname | amhandler | amtype |\n" +
"| --- | ------ | --------- | ------ |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTypeTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_type where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| oid  | typname           | typnamespace | typowner | typlen | typbyval | typtype | typisdefined | typdelim | typrelid | typelem | typinput | typoutput | typreceive | typsend | typanalyze | typalign | typstorage | typnotnull | typbasetype | typtypmod | typndims | typdefaultbin | typdefault |\n" +
"| ---- | ----------------- | ------------ | -------- | ------ | -------- | ------- | ------------ | -------- | -------- | ------- | -------- | --------- | ---------- | ------- | ---------- | -------- | ---------- | ---------- | ----------- | --------- | -------- | ------------- | ---------- |\n" +
"| 1028 | oid[]         | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 26      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 16   | bool              | -1000        | 0        | 1      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 17   | bytea             | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1042 | bpchar            | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 18   | char              | -1000        | 0        | 1      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 19   | name              | -1000        | 0        | 63     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1043 | varchar           | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 20   | int8              | -1000        | 0        | 8      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 21   | int2              | -1000        | 0        | 2      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 22   | int2vector        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 21      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 23   | int4              | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 24   | regproc           | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 2201 | refcursor[]   | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1790    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 25   | text              | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 26   | oid               | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1182 | date[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1082    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 30   | oidvector         | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 26      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1183 | time[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1083    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1184 | timestamptz       | -1000        | 0        | 8      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1185 | timestamptz[] | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1184    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1186 | interval          | -1000        | 0        | 16     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1187 | interval[]    | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1186    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1700 | numeric           | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1082 | date              | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1083 | time              | -1000        | 0        | 8      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 700  | float4            | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 701  | float8            | -1000        | 0        | 8      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 705  | unknown           | -1000        | 0        | -2     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 194  | pg_node_tree      | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1231 | numeric[]     | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1700    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1114 | timestamp         | -1000        | 0        | 8      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1115 | timestamp[]   | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1114    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 2276 | any               | -1000        | 0        | 4      | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1000 | bool[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 16      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1001 | bytea[]       | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 17      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1002 | char[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 18      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1003 | name[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 19      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1005 | int2[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 21      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1006 | int2vector[]  | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 22      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1007 | int4[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 23      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1008 | regproc[]     | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 24      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1009 | text[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 25      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1266 | timetz            | -1000        | 0        | 12     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1014 | bpchar[]      | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1042    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1270 | timetz[]      | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1266    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1015 | varchar[]     | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 1043    | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1016 | int8[]        | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 20      | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1021 | float4[]      | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 700     | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n" +
"| 1790 | refcursor         | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 0       | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 0        | null          | null       |\n" +
"| 1022 | float8[]      | -1000        | 0        | -1     | null     | b       | true         | ,        | 0        | 701     | 0        | 0         | 0          | 0       | 0          | c        | p          | false      | 0           | -1        | 1        | null          | null       |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAttributeTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_attribute where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| attrelid | attname    | atttypid | attstattarget | attlen | attnum | attndims | attcacheoff | atttypmod | attbyval | attstorage | attalign | attnotnull | atthasdef | attidentity | attisdropped | attislocal | attinhcount |\n" +
"| -------- | ---------- | -------- | ------------- | ------ | ------ | -------- | ----------- | --------- | -------- | ---------- | -------- | ---------- | --------- | ----------- | ------------ | ---------- | ----------- |\n" +
"| 2        | age        | 23       | 0             | 4      | 1      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | birthDate  | 1184     | 0             | 8      | 2      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | birthLong  | 20       | 0             | 8      | 3      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | birthTime  | 1083     | 0             | 8      | 4      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | country    | 1043     | 0             | -1     | 5      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | email      | 1043     | 0             | -1     | 6      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | first_name | 1043     | 0             | -1     | 7      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | id         | 1043     | 0             | -1     | 8      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | last_name  | 1043     | 0             | -1     | 9      | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | name       | 1043     | 0             | -1     | 10     | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n" +
"| 2        | timestamp  | 1114     | 0             | 8      | 11     | 0        | -1          | -1        | null     | p          | c        | false      | false     | ' '         | false        | false      | 0           |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNamespaceTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_namespace where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| oid   | nspname    | nspowner | nspacl |\n" +
"| ----- | ---------- | -------- | ------ |\n" +
"| 0     | PUBLIC     | 0        | null   |\n" +
"| -1000 | PG_CATALOG | 0        | null   |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClassTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_class where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| oid | relname                                               | relnamespace | reltype | relowner | relam | relfilenode | reltablespace | relpages | reltuples | reltoastrelid | relhasindex | relisshared | relkind | relnatts | relchecks | reltriggers | relhasrules | relhastriggers | relhassubclass | relacl | reloptions |\n" +
"| --- | ----------------------------------------------------- | ------------ | ------- | -------- | ----- | ----------- | ------------- | -------- | --------- | ------------- | ----------- | ----------- | ------- | -------- | --------- | ----------- | ----------- | -------------- | -------------- | ------ | ---------- |\n" +
"| 1   | java.lang.Object                                      | 0            | 0       | 0        | 0     | 0           | 0             | 0        | 100.0     | 0             | false       | false       | r       | 0        | 0         | 0           | false       | false          | false          | null   | null       |\n" +
"| 2   | com.gigaspaces.sql.aggregatornode.netty.server.MyPojo | 0            | 0       | 0        | 0     | 0           | 0             | 0        | 100.0     | 0             | false       | false       | r       | 11       | 0         | 0           | false       | false          | false          | null   | null       |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClassTableNoFqn(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_class where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| oid | relname                                               | relnamespace | reltype | relowner | relam | relfilenode | reltablespace | relpages | reltuples | reltoastrelid | relhasindex | relisshared | relkind | relnatts | relchecks | reltriggers | relhasrules | relhastriggers | relhassubclass | relacl | reloptions |\n" +
"| --- | ----------------------------------------------------- | ------------ | ------- | -------- | ----- | ----------- | ------------- | -------- | --------- | ------------- | ----------- | ----------- | ------- | -------- | --------- | ----------- | ----------- | -------------- | -------------- | ------ | ---------- |\n" +
"| 1   | java.lang.Object                                      | 0            | 0       | 0        | 0     | 0           | 0             | 0        | 100.0     | 0             | false       | false       | r       | 0        | 0         | 0           | false       | false          | false          | null   | null       |\n" +
"| 2   | com.gigaspaces.sql.aggregatornode.netty.server.MyPojo | 0            | 0       | 0        | 0     | 0           | 0             | 0        | 100.0     | 0             | false       | false       | r       | 11       | 0         | 0           | false       | false          | false          | null   | null       |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultiline(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                    "SELECT first_name, last_name, email, age FROM \"%s\" as T where T.last_name = 'Aa' OR T.first_name = 'Adam';" +
                    "SET DateStyle = 'ISO';" +
                    "SHOW transaction_isolation",
                    MyPojo.class.getName());
            final Statement statement = conn.createStatement();
            assertTrue(statement.execute(qry));
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| first_name | last_name | email         | age |\n" +
"| ---------- | --------- | ------------- | --- |\n" +
"| Adler      | Aa        | Adler@msn.com | 20  |\n" +
"| Adam       | Bb        | Adam@msn.com  | 30  |\n";
            DumpUtils.checkResult(res, expected);
            statement.getMoreResults();
            int updateCount = statement.getUpdateCount();
            assertEquals(1, updateCount);
            statement.getMoreResults();
            res = statement.getResultSet();
            expected = "" +
"| transaction_isolation |\n" +
"| --------------------- |\n" +
"| READ_COMMITTED        |\n";
            DumpUtils.checkResult(res, expected);
        }
    }
}
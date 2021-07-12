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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class OdbcMetadataQueryTest extends AbstractServerTest {
    private static final String SET_DATE_STYLE_ISO = "SET DateStyle = 'ISO';";
    private static final String SET_EXTRA_FLOAT_DIGITS_2 = "SET extra_float_digits = 2;";
    private static final String SHOW_TRANSACTION_ISOLATION = "show transaction_isolation;";
    private static final String SELECT_TYPE_WHERE_TYPNAME_LO = "select oid, typbasetype from pg_type where typname = 'lo';";
    private static final String SHOW_MAX_IDENTIFIER_LENGTH = "show max_identifier_length;";
    private static final String SELECT_NULL = "select NULL, NULL, NULL";
    private static final String SELECT_TABLES = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname";
    private static final String SELECT_ATTRIBUTES_1 = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, 0, attidentity, c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.oid = 16388) inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid) inner join pg_catalog.pg_type t on t.oid = a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum";
    private static final String SELECT_ATTRIBUTES_2 = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, 0, attidentity, c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like 'test\\_table' and n.nspname like 'public') inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid) inner join pg_catalog.pg_type t on t.oid = a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum";
    private static final String SELECT_INDEXES = "select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = 'test_table' AND n.nspname = 'public' AND tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid order by ia.attnum";
    private static final String SELECT_CONSTRAINTS = "select  'testdb'::name as \"PKTABLE_CAT\",\n" +
            "                n2.nspname as \"PKTABLE_SCHEM\",\n" +
            "                c2.relname as \"PKTABLE_NAME\",\n" +
            "                a2.attname as \"PKCOLUMN_NAME\",\n" +
            "                'testdb'::name as \"FKTABLE_CAT\",\n" +
            "                n1.nspname as \"FKTABLE_SCHEM\",\n" +
            "                c1.relname as \"FKTABLE_NAME\",\n" +
            "                a1.attname as \"FKCOLUMN_NAME\",\n" +
            "                i::int2 as \"KEY_SEQ\",\n" +
            "                case ref.confupdtype\n" +
            "                        when 'c' then 0::int2\n" +
            "                        when 'n' then 2::int2\n" +
            "                        when 'd' then 4::int2\n" +
            "                        when 'r' then 1::int2\n" +
            "                        else 3::int2\n" +
            "                end as \"UPDATE_RULE\",\n" +
            "                case ref.confdeltype\n" +
            "                        when 'c' then 0::int2\n" +
            "                        when 'n' then 2::int2\n" +
            "                        when 'd' then 4::int2\n" +
            "                        when 'r' then 1::int2\n" +
            "                        else 3::int2\n" +
            "                end as \"DELETE_RULE\",\n" +
            "                ref.conname as \"FK_NAME\",\n" +
            "                cn.conname as \"PK_NAME\",\n" +
            "                case\n" +
            "                        when ref.condeferrable then\n" +
            "                                case\n" +
            "                                when ref.condeferred then 5::int2\n" +
            "                                else 6::int2\n" +
            "                                end\n" +
            "                        else 7::int2\n" +
            "                end as \"DEFERRABILITY\"\n" +
            "         from\n" +
            "         ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,\n" +
            "                 generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,\n" +
            "                 confupdtype, confdeltype, conname,\n" +
            "                 condeferrable, condeferred\n" +
            "          from pg_catalog.pg_constraint cn,\n" +
            "                pg_catalog.pg_class c,\n" +
            "                pg_catalog.pg_namespace n\n" +
            "          where contype = 'f'\n" +
            "           and  conrelid = c.oid\n" +
            "           and  relname = 'test_table'\n" +
            "           and  n.oid = c.relnamespace\n" +
            "           and  n.nspname = 'public'\n" +
            "         ) ref\n" +
            "         inner join pg_catalog.pg_class c1\n" +
            "          on c1.oid = ref.conrelid)\n" +
            "         inner join pg_catalog.pg_namespace n1\n" +
            "          on  n1.oid = c1.relnamespace)\n" +
            "         inner join pg_catalog.pg_attribute a1\n" +
            "          on  a1.attrelid = c1.oid\n" +
            "          and  a1.attnum = conkey[i])\n" +
            "         inner join pg_catalog.pg_class c2\n" +
            "          on  c2.oid = ref.confrelid)\n" +
            "         inner join pg_catalog.pg_namespace n2\n" +
            "          on  n2.oid = c2.relnamespace)\n" +
            "         inner join pg_catalog.pg_attribute a2\n" +
            "          on  a2.attrelid = c2.oid\n" +
            "          and  a2.attnum = confkey[i])\n" +
            "         left outer join pg_catalog.pg_constraint cn\n" +
            "          on cn.conrelid = ref.confrelid\n" +
            "          and cn.contype = 'p')\n" +
            "          order by ref.oid, ref.i";

    @BeforeAll
    public static void setUp() throws Exception {
        Class.forName("org.postgresql.Driver");
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

    // Queries executed while creating a datasource with a user query.

    @Test
    public void testSetDateStyleIso() throws Exception {
        checkQuery(SET_DATE_STYLE_ISO);
    }

    @Test
    public void testSetExtraFloatDigits2() throws Exception {
        checkQuery(SET_EXTRA_FLOAT_DIGITS_2);
    }

    @Test
    public void testShowTransactionIsolation() throws Exception {
        checkQuery(SHOW_TRANSACTION_ISOLATION);
    }

    @Test
    public void testSelectTypeWhereTypnameLo() throws Exception {
        checkQuery(SELECT_TYPE_WHERE_TYPNAME_LO);
    }

    @Test
    public void testShowMaxIdentifierLength() throws Exception {
        checkQuery(SHOW_MAX_IDENTIFIER_LENGTH);
    }

    // Queries executed while creating a datasource using UI.

    @Disabled("No tables has been detected (backend cannot execute quries that doesn't reference tables")
    @Test
    public void testSelectNull() throws Exception {
        checkQuery(SELECT_NULL);
    }

    @Disabled("UnsupportedOperationException: Not supported yet! (TempTableContainer.getJoinedTable)")
    @Test
    public void testSelectTables() throws Exception {
        checkQuery(SELECT_TABLES);
    }

    @Disabled("UnsupportedOperationException: Not supported yet! (TempTableContainer.getJoinedTable)")
    @Test
    public void testSelectAttributes1() throws Exception {
        checkQuery(SELECT_ATTRIBUTES_1);
    }

    @Disabled("UnsupportedOperationException: Not supported yet! (TempTableContainer.getJoinedTable)")
    @Test
    public void testSelectAttributes2() throws Exception {
        checkQuery(SELECT_ATTRIBUTES_2);
    }

    @Disabled("Could not find column with name [indexrelid] (column resolution issue in JOIN handler)")
    @Test
    public void testSelectIndexes() throws Exception {
        checkQuery(SELECT_INDEXES);
    }

    @Disabled("Missing CASE support")
    @Test
    public void testSelectConstraints() throws Exception {
        checkQuery(SELECT_CONSTRAINTS);
    }

    private void checkQuery(String query) throws Exception {
        try (Connection connection = connect(true)) {
            Statement statement = connection.createStatement();
            statement.execute(query);
        }
    }
}

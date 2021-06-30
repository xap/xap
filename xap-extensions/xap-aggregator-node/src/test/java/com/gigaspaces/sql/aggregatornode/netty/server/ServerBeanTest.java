package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.*;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.*;

class ServerBeanTest {
    public static final String SPACE_NAME = "mySpace";

    private static GigaSpace gigaSpace;
    private static ServerBean server;

    @BeforeAll
    static void setUp() throws Exception {
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

        server = new ServerBean();
        server.init();
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
    void testEmptyTable(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = "SELECT * from pg_catalog.pg_am where 1 = 1";
            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
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
            DumpUtils.dump(res);
            statement.getMoreResults();
            int updateCount = statement.getUpdateCount();
            assertEquals(1, updateCount);
            statement.getMoreResults();
            res = statement.getResultSet();
            DumpUtils.dump(res);
        }
    }

    private Connection connect(boolean simple) throws Exception {
        String url = "jdbc:postgresql://localhost/"+SPACE_NAME+"?user=fred&password=secret";
        if (simple)
            url += "&preferQueryMode=simple";

        final Connection conn = DriverManager.getConnection(url);
        assertFalse(conn.isClosed());
        assertTrue(conn.isValid(1000));
        return conn;
    }
}
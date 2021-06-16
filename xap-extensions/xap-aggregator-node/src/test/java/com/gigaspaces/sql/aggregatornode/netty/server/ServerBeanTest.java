package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerBeanTest {
    public static final String SPACE_NAME = "mySpace";

    private GigaSpace gigaSpace;
    private ServerBean server;

    @BeforeEach
    void setUp() throws Exception {
        gigaSpace = new GigaSpaceConfigurer(
                new EmbeddedSpaceConfigurer(SPACE_NAME)
                        .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
        ).gigaSpace();

        fillSpace(gigaSpace);
        server = new ServerBean(SPACE_NAME);
        server.init();
    }

    private void fillSpace(GigaSpace gigaSpace) throws ParseException {
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

    @Test
    void testConnection() throws Exception {
        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost/?user=fred&password=secret";
        try (Connection conn = DriverManager.getConnection(url)) {
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(1000));

            Statement statement = conn.createStatement();

            execute(statement, String.format("SELECT replace(last_name, 'a', 'b'), last_name FROM %s", "\"" + MyPojo.class.getName() + "\""));
        }
    }

    private static void execute(Statement statement, String sql) throws SQLException {
        System.out.println();
        System.out.println("Executing: " + sql);
        ResultSet res = statement.executeQuery(sql);
        DumpUtils.dump(res);

    }

}
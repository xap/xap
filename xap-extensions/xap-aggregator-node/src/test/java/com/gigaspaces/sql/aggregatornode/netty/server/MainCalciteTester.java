package com.gigaspaces.sql.aggregatornode.netty.server;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class MainCalciteTester {
    public static void main(String[] args) throws SQLException, ParseException {
        GigaSpace space = createAndFillSpace(true, true);

        try(ServerBean server = new ServerBean(space.getSpaceName())) {
            server.init();

            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/test?user=user&password=secret")) {
                Statement statement = connection.createStatement();

//            execute(statement, String.format("SELECT * FROM %s where last_name = 'Bb' OR first_name = 'Eve'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT first_name as first, last_name as last FROM %s where last_name = 'Bb'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>=40))", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where T.last_name = 'Bb' or T.first_name = 'Adam' or T.last_name = 'Cc' or T.email = 'Adler@msn.com' or T.age>=40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, "SELECT * FROM com.gigaspaces.jdbc.MyPojo as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>=40))");
                execute(statement, String.format("SELECT first_name, last_name, email, age FROM %s as T where T.last_name = 'Aa' OR T.first_name = 'Adam'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where T.age <= 40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("EXPLAIN PLAN FOR SELECT * FROM %s ", "\"" + MyPojo.class.getName() + "\""));
            }

        } catch (Exception e) {
            e.printStackTrace();
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
        return gigaSpace;
    }
}

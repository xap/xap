package com.gigaspaces.jdbc;

import org.junit.Assert;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.rmi.RemoteException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class MainTester {
    public static void main(String[] args) throws SQLException, ParseException {
        boolean newDriver = Boolean.getBoolean("useNewDriver");
        GigaSpace space = createAndFillSpace(newDriver, true);

        Properties properties = new Properties();
        properties.put("com.gs.embeddedQP.enabled", "true");

        try {
            Class.forName("com.j_spaces.jdbc.driver.GDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(newDriver ? "jdbc:gigaspaces:v3://localhost:4174/" + space.getSpaceName() : "jdbc:gigaspaces:url:jini://*/*/" + space.getSpaceName(), properties)) {

            Statement statement = connection.createStatement();
            try {
                execute(statement, String.format("SELECT AVG(name) FROM %s",MyPojo.class.getName()));
                Assert.fail("should fail");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            try {
                execute(statement, String.format("SELECT AVG(name) FROM (SELECT * FROM %s)",MyPojo.class.getName()));
                Assert.fail("should fail");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            execute(statement, String.format("SELECT * FROM (SELECT count(*) FROM %s)",MyPojo.class.getName()));
            execute(statement, String.format("SELECT AVG(age) FROM (SELECT * FROM %s)",MyPojo.class.getName()));
            execute(statement, String.format("SELECT SUM(age) FROM (SELECT * FROM %s)",MyPojo.class.getName()));

            teardown(space, true);
        }
    }

    private static void teardown(GigaSpace gigaSpace, boolean isEmbedded) {
        if (isEmbedded) {
            try {
                for(Thread t : Thread.getAllStackTraces().keySet()){
                    if ("RMI Reaper".equals(t.getName())) { // Interrupt RMI Reaper thread.
                        t.interrupt();
                    }
                }
                gigaSpace.getSpace().getDirectProxy().shutdown();
            } catch (RemoteException e) {
                System.err.println("failed to shutdown Space" + e);
            }
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
            gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
            gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
            gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
        }
        return gigaSpace;
    }
}

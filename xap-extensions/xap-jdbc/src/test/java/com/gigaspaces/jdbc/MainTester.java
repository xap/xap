package com.gigaspaces.jdbc;

import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.config.TieredStorageConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.*;
import java.util.Properties;

public class MainTester {
    public static void main(String[] args) throws SQLException {
        boolean newDriver = Boolean.getBoolean("useNewDriver");
        GigaSpace space = createAndFillSpace(newDriver, true);

        Properties properties = new Properties();
//                try (Connection connection = GSConnection.getInstance(space.getSpace(), properties)) {
        properties.put("com.gs.embeddedQP.enabled", "true");

        try {
            Class.forName("com.j_spaces.jdbc.driver.GDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
//        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:url:jini://*/*/demo", properties)) {
        try (Connection connection = DriverManager.getConnection(newDriver ? "jdbc:gigaspaces:v3://localhost:4174/" + space.getSpaceName() : "jdbc:gigaspaces:url:jini://*/*/" + space.getSpaceName(), properties)) {
            Statement statement = connection.createStatement();
//            execute(statement, "SELECT * FROM com.gigaspaces.jdbc.MyPojo where name = 'Adler' OR name = 'Adam' AND age = 30");// WHERE rowNum <= 10");

//            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo");// WHERE rowNum <= 10");
//            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo WHERE country like '%a%'");
            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo WHERE age NOT BETWEEN 10 and 20");
//
//            execute(statement, "select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");
//
//            execute(statement, "explain select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");
//            execute(statement, "explain verbose select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");

//            String sqlQuery = "select name, name, id from com.gigaspaces.jdbc.MyPojo";
//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo";
//                String sqlQuery = "explain select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20";
//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo AS A";
//            String sqlQuery = "select name from (select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name)";
//            String sqlQuery = "select name AS name2 from (select name from com.gigaspaces.jdbc.MyPojo)";
//            String sqlQuery = "select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//            String sqlQuery = "explain select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//                ResultSet res = statement.executeQuery(sqlQuery);
//                DumpUtils.dump(res);

        }
    }

    private static void execute(Statement statement, String sql) throws SQLException {
        ResultSet res = statement.executeQuery(sql);
        System.out.println();
        System.out.println("Executing: " + sql);
        DumpUtils.dump(res);

    }

    private static GigaSpace createAndFillSpace(boolean newDriver, boolean embedded) {
        String spaceName = "demo" + (newDriver ? "new" : "old");
        AbstractSpaceConfigurer configurer = embedded ? new EmbeddedSpaceConfigurer(spaceName)
//                .tieredStorage(new TieredStorageConfigurer().addTable(new TieredStorageTableConfig().setName(MyPojo.class.getName()).setCriteria("age > 20")))
                : new SpaceProxyConfigurer(spaceName);

        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (embedded || gigaSpace.count(null) == 0) {
            gigaSpace.write(new MyPojo("Adler", 20, "Israel"));
            gigaSpace.write(new MyPojo("Adam", 30, "Israel"));
            gigaSpace.write(new MyPojo("Eve", 35, "UK"));
            gigaSpace.write(new MyPojo("NoCountry", 40, null));
        }
        return gigaSpace;
    }
}

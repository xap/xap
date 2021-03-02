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

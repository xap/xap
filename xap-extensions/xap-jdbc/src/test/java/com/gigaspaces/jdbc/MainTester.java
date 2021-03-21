package com.gigaspaces.jdbc;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.*;
import java.util.Properties;

public class MainTester {
    public static void main(String[] args) throws SQLException {
        GigaSpace space = createAndFillSpace();

        Properties properties = new Properties();
//                try (Connection connection = GSConnection.getInstance(space.getSpace(), properties)) {

        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:v3://localhost:4174/demo", properties)) {
            Statement statement = connection.createStatement();
//            String sqlQuery = "select name, name, id from com.gigaspaces.jdbc.MyPojo";
//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo";
            String sqlQuery = "explain select name from com.gigaspaces.jdbc.MyPojo";

//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo AS A";
//            String sqlQuery = "select name from (select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name)";
//            String sqlQuery = "select name AS name2 from (select name from com.gigaspaces.jdbc.MyPojo)";
//            String sqlQuery = "select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//            String sqlQuery = "explain select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
            ResultSet res = statement.executeQuery(sqlQuery);
            DumpUtils.dump(res);
        }
    }

    private static GigaSpace createAndFillSpace() {
        GigaSpace gigaSpace = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("demo")).gigaSpace();
        gigaSpace.write(new MyPojo("Adler", 20, "Israel"));
        gigaSpace.write(new MyPojo("Adam", 30, "Israel"));
        gigaSpace.write(new MyPojo("Eve", 35, "UK"));
        return gigaSpace;
    }
}

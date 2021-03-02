package com.gigaspaces.jdbc;

import com.j_spaces.jdbc.driver.GConnection;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    @org.junit.Test
    public void test() throws SQLException, ClassNotFoundException {
        Class.forName("com.j_spaces.jdbc.driver.GDriver");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("demo")).gigaSpace();

        gigaSpace.write(new MyPojo("yohana", 30));

        try (Connection connection = GConnection.getInstance(gigaSpace.getSpace())) {
//            ResultSet res = connection.createStatement().executeQuery("select name, name as koko from com.gigaspaces.jdbc.MyPojo");
//            DumpUtils.dump(res);
            ResultSet res = connection.createStatement().executeQuery("select name, name as koko from (Select name, name as koko from com.gigaspaces.jdbc.MyPojo)");
            DumpUtils.dump(res);

        }
    }
}

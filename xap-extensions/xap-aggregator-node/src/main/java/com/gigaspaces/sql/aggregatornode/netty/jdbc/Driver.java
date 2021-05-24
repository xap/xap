package com.gigaspaces.sql.aggregatornode.netty.jdbc;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver{
    private static boolean registered = false;

    static {
        registerDriver();
    }

    private static void registerDriver() {
        //Class.forName will call this, so this is where we register the driver
        if (!registered) {
            try {
                java.sql.DriverManager.registerDriver(new Driver());
                registered = true;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url))
            return new com.gigaspaces.sql.aggregatornode.netty.jdbc.Connection();
        else
            return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc::aggregator");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}

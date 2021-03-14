package com.gigaspaces.jdbc;

import com.gigaspaces.logger.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class Driver implements java.sql.Driver {
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private static boolean registered = false;

    static {
        registerDriver();
    }

    private static void registerDriver() {
        //Class.forName will call this, so this is where we register the driver
        if (!registered) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("QueryProcessor: Registering driver");
            }
            try {
                java.sql.DriverManager.registerDriver(new Driver());
                registered = true;

            } catch (SQLException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        if (acceptsURL(url))
            return new GSConnection(url, info).connect();

        return null;

    }

    @Override
    public boolean acceptsURL(String url) {
        return (url.matches(GSConnection.CONNECTION_STRING_REGEX));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 3;
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
    public java.util.logging.Logger getParentLogger() {
        return null;
    }
}

/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.j_spaces.jdbc.driver;

import com.gigaspaces.logger.Constants;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GigaSpaces JDBC driver implementation.
 *
 * @author Michael Mitrani
 */

public class GDriver implements Driver {

    public static final int MAJOR_VERSION = 4;
    public static final int MINOR_VERSION = 1;
    public static final String DRIVER_NAME = "GigaSpaces JDBC Driver";
    public static boolean registered = false;
    private static HashMap procTable = null;
    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    static {
        registerDriver();
    }

    public static void registerDriver() {
        //Class.forName will call this, so this is where we register the driver
        if (!registered) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("QueryProcessor: Registering driver");
            }
            try {
                java.sql.DriverManager.registerDriver(new GDriver());
                registered = true;

            } catch (SQLException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * @see java.sql.Driver#getMajorVersion()
     */
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * @see java.sql.Driver#getMinorVersion()
     */
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /**
     * This one is not fully compliant.
     *
     * @see java.sql.Driver#jdbcCompliant()
     */
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Only urls that start with jdbc:gigaspaces: are compliant
     *
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    public boolean acceptsURL(String url) throws SQLException {
        return (url.startsWith(GConnection.JDBC_GIGASPACES));
    }

    /**
     * @return Connection, the new GConnection or null in case of invalid url.
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    public Connection connect(String url, Properties info) throws SQLException {

        if (acceptsURL(url))
            return new GConnection(url, info);

        return null;

    }

    /**
     * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        return null;
    }


    public static HashMap getProcTable() {
        if (procTable == null) {
            procTable = new HashMap();
        }
        return procTable;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return java.util.logging.Logger.getLogger(Constants.LOGGER_QUERY);
    }
}

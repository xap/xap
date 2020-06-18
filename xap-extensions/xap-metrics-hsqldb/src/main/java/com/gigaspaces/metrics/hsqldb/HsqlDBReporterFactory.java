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

package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricReporterFactory;

import java.util.Properties;

/**
 * @author Evgeny
 * @since 15.0
 */
public class HsqlDBReporterFactory extends MetricReporterFactory<MetricReporter> {

    private static final String DEFAULT_DRIVER_CLASS_NAME = "org.hsqldb.jdbc.JDBCDriver";
    private static final String DEFAULT_PORT = String.valueOf(9101);
    public static final String PORT = GsEnv.property("com.gs.ui.metrics.db.port").get( DEFAULT_PORT );
    private static final String DEFAULT_DBTYPE_STRING = "VARCHAR(300)";

    private String dbName;
    private String username;
    private String password;
    private String host;
    private String port;
    private String driverClassName;
    private String dbTypeString;
    private SharedJdbcConnectionWrapper connectionWrapper;

    @Override
    public void load(Properties properties) {
        super.load(properties);

        setDbName(properties.getProperty("dbname"));
        setDriverClassName(properties.getProperty("driverClassName", DEFAULT_DRIVER_CLASS_NAME));
        setHost(properties.getProperty("host"));
        setPort( PORT );
        setUsername(properties.getProperty("username"));
        setPassword(properties.getProperty("password"));
        setDbTypeString(properties.getProperty("dbTypeString", DEFAULT_DBTYPE_STRING));
    }

    @Override
    public MetricReporter create() {
        synchronized (this) {
            if (connectionWrapper == null)
                connectionWrapper = new SharedJdbcConnectionWrapper(this);
            else
                connectionWrapper.reuse();
        }

        return new HsqlDbReporter(this, connectionWrapper);
    }

    public String getConnectionUrl() {
        return "jdbc:hsqldb:hsql://" + host + ":" + port + "/" + dbName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbTypeString() {
        return dbTypeString;
    }

    public void setDbTypeString(String dbTypeString) {
        this.dbTypeString = dbTypeString;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}

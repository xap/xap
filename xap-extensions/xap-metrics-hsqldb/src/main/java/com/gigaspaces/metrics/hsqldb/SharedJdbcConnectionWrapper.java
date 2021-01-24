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

import com.gigaspaces.logger.ActivityLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedJdbcConnectionWrapper implements Closeable {

    private final Logger logger;
    private final HsqlDBReporterFactory factory;
    private final String url;

    private volatile Connection connection;
    private final Object connectionLock = new Object();
    private final ActivityLogger connectionLogger;
    private final AtomicInteger referenceCounter = new AtomicInteger(1);
    private final int minLoginTimeoutInSec = Integer.getInteger("com.gigaspaces.metrics.hsqldb.loginTimeoutSec.min", 10);
    private final int maxLoginTimeoutInSec = Integer.getInteger("com.gigaspaces.metrics.hsqldb.loginTimeoutSec.max", 60);

    public SharedJdbcConnectionWrapper(HsqlDBReporterFactory factory) {
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.factory = factory;
        this.url = factory.getConnectionUrl();
        this.connectionLogger = new ActivityLogger.Builder("Jdbc Connection", logger)
                .concurrent()
                .initialSilentDuration(Duration.ofMinutes(1))
                .reduceConsecutiveFailuresLogging(10, 100)
                .build();

        if (getOrCreateConnection() == null) {
            if (!isSilent())
                logger.warn("Connection is not available yet - will try to reconnect on first report");
        }
    }

    SharedJdbcConnectionWrapper reuse() {
        referenceCounter.incrementAndGet();
        return this;
    }

    @Override
    public void close() {
        if (referenceCounter.decrementAndGet() == 0) {
            synchronized (connectionLock) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.warn("Failed to close connection", e);
                    }
                    connection = null;
                }
            }
        }
    }

    public Connection getOrCreateConnection() {
        if (connection == null) {
            synchronized (connectionLock) {
                if (connection == null) {
                    try {
                        final int loginTimeoutInSec = getLoginTimeout();
                        logger.debug("Connecting to [{}] with a maximum wait time of [{}s]", url, loginTimeoutInSec);
                        DriverManager.setLoginTimeout(loginTimeoutInSec);
                        connection = DriverManager.getConnection(url, factory.getUsername(), factory.getPassword());
                        connectionLogger.success();
                        logger.info("Connected to [{}]", url);
                    } catch (Throwable e) {
                        connectionLogger.fail(e, () -> "[url=" + url + "]");
                    }
                }
            }
        }
        return connection;
    }

    /**
     * LoginTimeout: The amount of time, in seconds, that the driver waits for a connection
     * to be established before timing out the connection request. zero means there is no limit.
     *
     * The timeout starts from minLoginTimeoutInSec and increases by minLoginTimeoutInSec until
     * greater than maxLoginTimeoutInSec, and then starts cycles back to minLoginTimeoutInSec.
     *
     * updates are guarded by connectionLock
     */
    private int getLoginTimeout() {
        //zero means there is no limit
        if (minLoginTimeoutInSec == 0
                || maxLoginTimeoutInSec == 0) {
            return 0;
        }

        int loginTimeout = DriverManager.getLoginTimeout();
        if (loginTimeout < maxLoginTimeoutInSec) {
            loginTimeout += minLoginTimeoutInSec;
        } else {
            loginTimeout = minLoginTimeoutInSec;
        }
        return loginTimeout;
    }

    public void resetConnection(Connection conn) {
        if (conn == connection) {
            synchronized (connectionLock) {
                if (conn == connection) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.warn("Failed closing connection", e);
                    }
                    connection = null;
                }
            }
        }
    }

    public boolean isSilent() {
        return connectionLogger.isInitialTimerActive();
    }
}

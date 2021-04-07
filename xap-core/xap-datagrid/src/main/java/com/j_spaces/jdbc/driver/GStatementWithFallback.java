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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class GStatementWithFallback extends GStatement {
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private final Statement fallbackStatement;

    public GStatementWithFallback(GConnection connection) throws SQLException {
        super(connection);
        fallbackStatement = connection.getFallbackConnection().createStatement();
    }
    public <T> T call(Callable<T> first, Callable<T> second) throws SQLException {
        try {
            _logger.debug("Will try and run the query with the GDriver");
            T call = first.call();
            _logger.debug("Query ran successfully with GDriver");
            return call;
        } catch (SQLException e) {
            _logger.debug("Query failed to run using GDriver, trying fallback driver", e);
            try {
                T call = second.call();
                _logger.debug("Query ran successfully with fallback driver");
                return call;
            } catch (SQLException ex) {
                _logger.debug("Query failed to run using fallback driver", ex);
                throw ex;
            } catch (Exception ex) {
                _logger.debug("Query failed to run using fallback driver, got generic exception", ex);
                throw new SQLException(ex);
            }
        } catch (Exception e) {
            _logger.debug("Query failed to run using GDriver, got generic exception", e);
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return call(() -> super.executeQuery(sql), () -> fallbackStatement.executeQuery(sql));
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return call(() -> super.execute(sql), () -> fallbackStatement.execute(sql));
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return call(super::executeBatch, fallbackStatement::executeBatch);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return call(() -> super.executeUpdate(sql), () -> fallbackStatement.executeUpdate(sql));
    }

}

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

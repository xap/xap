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

import com.j_spaces.jdbc.ResultEntry;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * The ResultSet implementation
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class GResultSet implements ResultSet {

    private final ResultEntry results;
    private final Statement statement;
    private boolean last_was_null = false;
    private int currentRow = 0; //first time after next it will be 1

    public GResultSet(Statement statement, ResultEntry results) {
        this.statement = statement;
        this.results = results;
    }

    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    /**
     * This is only used as a hint. no support here.
     */
    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getRow() throws SQLException {
        return currentRow;
    }

    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * TYPE_FORWARD_ONLY mode only
     */
    public void afterLast() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);

    }

    public void beforeFirst() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);

    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void clearWarnings() throws SQLException {
        //there is no warning support, so there is nothing to do.
    }

    public void close() throws SQLException {
        //can clear everything
        //or do nothing
    }

    public void deleteRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void insertRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void refreshRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateRow() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean first() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean isAfterLast() throws SQLException {
        return (currentRow > results.getRowNumber());
    }

    public boolean isBeforeFirst() throws SQLException {
        return (currentRow <= 0);
    }

    public boolean isFirst() throws SQLException {
        return (currentRow == 1);
    }

    public boolean isLast() throws SQLException {
        return (currentRow == results.getRowNumber());
    }

    public boolean last() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean next() throws SQLException {
        if (results == null || currentRow + 1 > results.getRowNumber())
            return false;
        currentRow++;
        return true;

    }

    public boolean previous() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean wasNull() throws SQLException {
        return last_was_null;
    }

    public byte getByte(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0;
        }
        if (ob instanceof Byte) {
            return ((Byte) ob).byteValue();
        }

        throw new SQLException("Cannot represent this value as a byte",
                "GSP", -148);
    }

    public double getDouble(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0D;
        }
        if (ob instanceof Number) {
            return ((Number) ob).doubleValue();
        }

        throw new SQLException("Cannot represent this value as a double",
                "GSP", -149);
    }

    public float getFloat(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0f;
        }
        if (ob instanceof Number) {
            return ((Number) ob).floatValue();
        }

        throw new SQLException("Cannot represent this value as a float",
                "GSP", -150);
    }

    public int getInt(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0;
        }
        if (ob instanceof Number) {
            return ((Number) ob).intValue();
        }

        throw new SQLException("Cannot represent this value as an int",
                "GSP", -151);
    }

    public long getLong(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0L;
        }
        if (ob instanceof Number) {
            return ((Number) ob).longValue();
        }

        throw new SQLException("Cannot represent this value as a long",
                "GSP", -152);
    }

    public short getShort(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return 0;
        }
        if (ob instanceof Number) {
            return ((Number) ob).shortValue();
        }

        throw new SQLException("Cannot represent this value as a short",
                "GSP", -153);
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /**
     * TYPE_FORWARD_ONLY mode only
     */
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return false;
        }
        if (ob instanceof Boolean)
            return ((Boolean) ob).booleanValue();
        if (ob instanceof Number) {
            return (((Number) ob).floatValue() != 0);
        }
        if (ob instanceof String) {
            return ((String) ob).equalsIgnoreCase("true");
        }
        throw new SQLException("Cannot represent this value as a boolean",
                "GSP", -154);
    }

    public boolean relative(int rows) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }

        if (ob instanceof byte[])
            return (byte[]) ob;

        if (ob instanceof String)
            return ((String) ob).getBytes();

        throw new SQLException(String.format("Cannot convert %s type to a byte array", ob.getClass().getName()));
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkRowNumber();
        Object result = results.getFieldValues(currentRow)[columnIndex - 1];

        if (result == null) {
            last_was_null = true;
            return null;
        }

        if (result instanceof Clob) {
            Clob clobResult = (Clob) result;
            return clobResult.getAsciiStream();
        }

        throw new SQLException(String.format("Cannot convert %s type to an ascii stream", result.getClass().getName()));
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkRowNumber();
        Object result = results.getFieldValues(currentRow)[columnIndex - 1];

        if (result == null) {
            last_was_null = true;
            return null;
        }

        if (result instanceof Blob) {
            Blob blobResult = (Blob) result;
            return new ByteArrayInputStream(blobResult.getBytes(1, (int) blobResult.length()));
        }

        throw new SQLException(String.format("Cannot convert %s type to a binary stream", result.getClass().getName()));
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkRowNumber();
        Object result = results.getFieldValues(currentRow)[columnIndex - 1];

        if (result == null) {
            last_was_null = true;
            return null;
        }

        if (result instanceof com.j_spaces.jdbc.driver.Clob) {
            com.j_spaces.jdbc.driver.Clob clobResult = (com.j_spaces.jdbc.driver.Clob) result;
            return new StringReader(clobResult.getClobData());
        }

        throw new SQLException(String.format("Cannot convert %s type to a characters stream", result.getClass().getName()));
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Object getObject(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null)
            last_was_null = true;

        return ob;
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public String getCursorName() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public String getString(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }
        return ob.toString();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    public int findColumn(String columnName) throws SQLException {
        if (results != null) {
            String[] columnLabels = results.getColumnLabels();
            for (int index = 0; index < columnLabels.length; index++) {
                if (columnLabels[index].equalsIgnoreCase(columnName)) {
                    return index + 1; // JDBC's start index is 1.
                }
            }
        }
        throw new SQLException("Column " + columnName + " was not found in result", "GSP", -155);
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    public void updateNull(String columnName) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return BigDecimal.ZERO;
        }
        if (ob instanceof Number) {
            return new BigDecimal(((Number) ob).doubleValue());
        }

        throw new SQLException("Cannot represent this value as a BigDecimal",
                "GSP", -149);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public URL getURL(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }

        if (ob instanceof URL)
            return (URL) ob;

        throw new SQLException(String.format("Cannot convert %s type to a URL type", ob.getClass().getName()));
    }

    public Array getArray(int i) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }
        if (ob instanceof Blob) {
            com.j_spaces.jdbc.driver.Blob blob = ((com.j_spaces.jdbc.driver.Blob) ob);
            blob.setConnection(this.getStatement().getConnection());
            return (blob);
        }

        throw new SQLException("Cannot represent this value as Blob",
                "GSP", -156);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }
        if (ob instanceof Clob) {
            com.j_spaces.jdbc.driver.Clob clob = ((com.j_spaces.jdbc.driver.Clob) ob);
            clob.setConnection(this.getStatement().getConnection());
            return (clob);
        }

        throw new SQLException("Cannot represent this value as Clob",
                "GSP", -157);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Date getDate(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }

        if (ob instanceof Date) {
            return ((Date) ob);
        }

        throw new SQLException("Cannot represent this value as Date",
                "GSP", -158);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }

        if (ob instanceof Ref)
            return (Ref) ob;

        throw new SQLException(String.format("Cannot convert %s type to a Ref type", ob.getClass().getName()));
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return new GResultSetMetaData(results);
    }

    public SQLWarning getWarnings() throws SQLException {
        return null; //no warnings are ever kept
    }

    public Statement getStatement() throws SQLException {
        return statement;
    }

    public Time getTime(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }
        if (ob instanceof Time) {
            return ((Time) ob);
        }

        throw new SQLException("Cannot represent this value as Time",
                "GSP", -159);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkRowNumber();
        Object ob = results.getFieldValues(currentRow)[columnIndex - 1];
        if (ob == null) {
            last_was_null = true;
            return null;
        }
        if (ob instanceof Timestamp) {
            return ((Timestamp) ob);
        }

        throw new SQLException("Cannot represent this value as Timestamp",
                "GSP", -160);
    }

    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateAsciiStream(String columnName, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateBinaryStream(String columnName, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateCharacterStream(String columnName, Reader reader,
                                      int length) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        //not supported, and not wanting to cause confusion by just calling
        //getObject(i), so an exception is thrown
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    public void updateString(String columnName, String x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public URL getURL(String columnName) throws SQLException {
        return getURL(findColumn(columnName));
    }

    public Array getArray(String colName) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Blob getBlob(String columnName) throws SQLException {
        return getBlob(findColumn(columnName));
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Clob getClob(String columnName) throws SQLException {
        return getClob(findColumn(columnName));
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    public Ref getRef(String colName) throws SQLException {
        return getRef(findColumn(colName));
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        //ignore the calendar
        return getTime(columnIndex);
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        return getTimestamp(columnIndex);
    }

    public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(colName), map);
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        //ignore the calendar
        return getTime(findColumn(columnName));
    }

    public Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    private void checkRowNumber() throws SQLException {
        //reset last_was_null
        last_was_null = false;
        if (currentRow <= 0)
            throw new SQLException("The next() method must be called at least once",
                    "GSP", -161);
        if (currentRow > results.getRowNumber())
            throw new SQLException("Exhausted ResultSet!", "GSP", -162);
    }

    public ResultEntry getResult() throws SQLException {
        return this.results;
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBinaryStream(String columnLabel, InputStream x,
                                   long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateBlob(String columnLabel, InputStream inputStream,
                           long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateCharacterStream(String columnLabel, Reader reader,
                                      long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNCharacterStream(String columnLabel, Reader reader,
                                       long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNString(int columnIndex, String nString)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void updateNString(String columnLabel, String nString)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException {
        throw new UnsupportedOperationException();
    }
}

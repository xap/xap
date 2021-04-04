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

import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.batching.BatchResponsePacket;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * This is the PreparedStatement implementation.
 *
 * @author Michael Mitrani, 2Train4
 */
@com.gigaspaces.api.InternalApi
public class GPreparedStatement extends GStatement implements PreparedStatement {

    private final String sql;
    private PreparedValuesCollection _preparedValuesCollection;

    public GPreparedStatement(GConnection con, String sql) throws SQLException {
        super(con);
        this.sql = sql;
        int numOfValues = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?')
                numOfValues++;
        }
        _preparedValuesCollection = new PreparedValuesCollection(numOfValues);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#executeBatch()
     */
    public int[] executeBatch() throws SQLException {
        if (_preparedValuesCollection.size() > 0) {
            BatchResponsePacket response = connection.sendPreparedStatementBatch(
                    sql, _preparedValuesCollection);
            _preparedValuesCollection.clear();
            return response.getResult();
        }
        return new int[0];
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#executeUpdate()
     */
    public int executeUpdate() throws SQLException {
        checkValues();
        if (sql.toUpperCase().startsWith("SELECT"))
            throw new SQLException("Cannot call SELECT with executeUpdate. Use executeQuery instead",
                    "GSP", -143);

        //otherwise we continue
        ResponsePacket response = connection.sendPreparedStatement(sql, _preparedValuesCollection.getCurrentValues());
        //after the statement was sent and checked, we can return the result
        updateCount = response.getIntResult();
        return updateCount;
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#addBatch()
     */
    public void addBatch() throws SQLException {
        checkValues();
        _preparedValuesCollection.addBatch();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#clearBatch()
     */
    public void clearBatch() throws SQLException {
        _preparedValuesCollection.clear();
    }


    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#clearParameters()
     */
    public void clearParameters() throws SQLException {
        _preparedValuesCollection.resetCurrentValues();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#execute()
     */
    public boolean execute() throws SQLException {
        checkValues();

        ResponsePacket response = connection.sendPreparedStatement(sql, _preparedValuesCollection.getCurrentValues());
        //after the statement was sent and checked, we can return the result
        if (response.getResultEntry() != null) {
            buildResultSet(response);
            return true;
        } else {
            updateCount = response.getIntResult();
            return false;
        }
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setByte(int, byte)
     */
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Byte.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setDouble(int, double)
     */
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Double.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setFloat(int, float)
     */
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Float.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setInt(int, int)
     */
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Integer.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNull(int, int)
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, null);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setLong(int, long)
     */
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Long.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setShort(int, short)
     */
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Short.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBoolean(int, boolean)
     */
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, Boolean.valueOf(x));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBytes(int, byte[])
     */
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        checkIndex(parameterIndex);

        if (x == null)
            throw new SQLException("Cannot set a null ascii stream", "GSP", -144);

        try {

            _preparedValuesCollection.setValue(parameterIndex - 1, SQLUtil.convertAsciiInputStreamToString(x, length));

        } catch (IOException e) {
            throw new SQLException("Error reading from input stream: " + e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        checkIndex(parameterIndex);

        if (x == null)
            throw new SQLException("Cannot set a null binary stream", "GSP", -144);

        try {

            _preparedValuesCollection.setValue(parameterIndex - 1, SQLUtil.convertInputStreamToByteArray(x, length));

        } catch (IOException e) {
            throw new SQLException("Error reading from input stream: " + e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
     */
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        checkIndex(parameterIndex);

        if (reader == null)
            throw new SQLException("Cannot set a null character stream", "GSP", -144);

        try {

            _preparedValuesCollection.setValue(parameterIndex - 1, SQLUtil.convertReaderToString(reader, length));

        } catch (IOException e) {
            throw new SQLException("Error reading from reader: " + e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
     */
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkIndex(parameterIndex);
        if (x == null)
            throw new SQLException("Cannot set a null object", "GSP", -144);

        _preparedValuesCollection.setValue(parameterIndex - 1, x);

    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType,
                          int scale) throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
     */
    public void setNull(int paramIndex, int sqlType, String typeName)
            throws SQLException {
        setNull(paramIndex, sqlType);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setString(int, java.lang.String)
     */
    public void setString(int parameterIndex, String x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
     */
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
     */
    public void setArray(int i, Array x) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
     */
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
     */
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
     */
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkIndex(parameterIndex);
        Calendar cal = Calendar.getInstance();
        cal.setTime(x);
        Calendar cal2 = Calendar.getInstance();
        cal2.clear();
        cal2.set(Calendar.DATE, cal.get(Calendar.DATE));
        cal2.set(Calendar.MONTH, cal.get(Calendar.MONTH));
        cal2.set(Calendar.YEAR, cal.get(Calendar.YEAR));
        _preparedValuesCollection.setValue(parameterIndex - 1, new Date(cal2.getTime().getTime()));
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#getParameterMetaData()
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
     */
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setObject(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#executeQuery()
     */
    public ResultSet executeQuery() throws SQLException {
        checkValues();
        if (!sql.toUpperCase().startsWith("SELECT") && !sql.toUpperCase().startsWith("CALL") && !sql.toUpperCase().startsWith("EXPLAIN")) {
            //	throw new SQLException(" Used executeQuery  instead executeUpdate",
            //	"GSP",-146);
            executeUpdate();
        } else {
            ResponsePacket response = connection.sendPreparedStatement(sql, _preparedValuesCollection.getCurrentValues());
            //	query was sent and checked
            buildResultSet(response); //build the ResultSet
        }
        return resultSet;
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#getMetaData()
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        //TODO
        return null;
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
     */
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
     */
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
        checkIndex(parameterIndex);
        _preparedValuesCollection.setValue(parameterIndex - 1, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
     */
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
        setDate(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
     */
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException {
        setTime(parameterIndex, x);
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
     */
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    private void checkIndex(int index) throws SQLException {
        if (index > _preparedValuesCollection.getCurrentValues().length)
            throw new SQLException("Cannot set value, invalid index " + index, "GSP", -147);
    }

    private void checkValues() throws SQLException {
        if (!_preparedValuesCollection.allValuesAreSet())
            throw new SQLException("All values must be set", "GSP", -126);
    }

    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        setAsciiStream(parameterIndex, x, Integer.MAX_VALUE);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        setBinaryStream(parameterIndex, x, Integer.MAX_VALUE);

    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException {
        setBinaryStream(parameterIndex, inputStream, Integer.MAX_VALUE);

    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        setCharacterStream(parameterIndex, reader, Integer.MAX_VALUE);

    }

    public void setCharacterStream(int parameterIndex, Reader reader,
                                   long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setNCharacterStream(int parameterIndex, Reader value,
                                    long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public void setNString(int parameterIndex, String value)
            throws SQLException {
        throw new UnsupportedOperationException();

    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * An implementation for holding the prepared values & prepared values batch.
     *
     * Note:	The values set status in not serialized since its only used for validation.
     *
     * @author idan
     * @since 8.0
     */
    public static class PreparedValuesCollection implements Externalizable {
        private static final long serialVersionUID = 1L;
        private Object[] _currentValues;
        private boolean[] _currentValuesSetStatus;
        private List<Object[]> _valuesBatch;

        public PreparedValuesCollection() {
        }

        public PreparedValuesCollection(int numberOfValues) {
            _currentValues = new Object[numberOfValues];
            _currentValuesSetStatus = new boolean[numberOfValues];
        }

        public List<Object[]> getBatchValues() {
            return _valuesBatch;
        }

        public boolean allValuesAreSet() {
            for (int i = 0; i < _currentValuesSetStatus.length; i++)
                if (!_currentValuesSetStatus[i])
                    return false;
            return true;
        }

        public void setValue(int index, Object value) {
            _currentValues[index] = value;
            _currentValuesSetStatus[index] = true;
        }

        public Object[] getCurrentValues() {
            return _currentValues;
        }

        public void addBatch() {
            if (_valuesBatch == null)
                _valuesBatch = new ArrayList<Object[]>();
            _valuesBatch.add(_currentValues.clone());
            resetCurrentValues();
        }

        public void resetCurrentValues() {
            for (int i = 0; i < _currentValues.length; i++) {
                _currentValues[i] = null;
                _currentValuesSetStatus[i] = false;
            }
        }

        public int size() {
            return (_valuesBatch != null) ? _valuesBatch.size() : 0;
        }

        public void clear() {
            resetCurrentValues();
            if (_valuesBatch != null)
                _valuesBatch.clear();
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            _currentValues = IOUtils.readObjectArray(in);
            int length = in.readInt();
            _valuesBatch = new ArrayList<Object[]>();
            for (int i = 0; i < length; i++) {
                _valuesBatch.add(IOUtils.readObjectArray(in));
            }
            _currentValuesSetStatus = new boolean[length];
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeObjectArray(out, _currentValues);
            int length = _valuesBatch.size();
            out.writeInt(length);
            for (Object[] valuesArray : _valuesBatch) {
                IOUtils.writeObjectArray(out, valuesArray);
            }
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException {
        throw new UnsupportedOperationException();
    }
}

package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.EntryHolder;
import com.gigaspaces.internal.server.storage.FlatEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.utils.concurrent.ReentrantSimpleLock;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.lease.Lease;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteException;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SqliteRDBMS implements InternalRDBMS {
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    private String PATH = "/tmp/sqlite_db";
    private String DB_URL = "jdbc:sqlite:" + PATH;
    private Connection connection;
    private ReentrantSimpleLock modifierLock = new ReentrantSimpleLock();
    private AtomicInteger readCount = new AtomicInteger(0);
    private AtomicInteger writeCount = new AtomicInteger(0);
    private SpaceTypeManager typeManager;

    @Override
    public void initialize(SpaceTypeManager typeManager) throws SAException {
        try {
            org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
            connection = connectToDB(JDBC_DRIVER, DB_URL, USER, PASS, config);
            this.typeManager = typeManager;
        } catch (ClassNotFoundException | SQLException e) {
            throw new SAException("failed to initialize internal sqlite RDBMS", e);
        }
    }

    @Override
    public void createTable(ITypeDesc typeDesc) throws SAException {
        String typeName = typeDesc.getTypeName();
        StringBuilder stringBuilder = new StringBuilder("CREATE TABLE '").append(typeName).append("' (");
        for (PropertyInfo property : typeDesc.getProperties()) {
            stringBuilder.append("'").append(property.getName()).append("' ").append(getPropertyType(property)).append(", ");
        }
        stringBuilder.append("PRIMARY KEY (").append(typeDesc.getIdPropertyName()).append(")");
        stringBuilder.append(");");
        String sqlQuery = stringBuilder.toString();
        System.out.println("Query = " + sqlQuery);
        try {
            executeUpdate(sqlQuery);

            for (Map.Entry<String, SpaceIndex> entry : typeDesc.getIndexes().entrySet()) {
                if (!entry.getKey().equals(typeDesc.getIdPropertyName())) {
                    stringBuilder = new StringBuilder("CREATE ");
                    SpaceIndex index = entry.getValue();
                    if (index.isUnique()) {
                        stringBuilder.append("UNIQUE ");
                    }
                    stringBuilder.append("INDEX '").append(index.getName()).append("'");
                    stringBuilder.append(" ON '").append(typeName).append("' ('").append(index.getName()).append("');");
                    sqlQuery = stringBuilder.toString();
                    System.out.println(sqlQuery);
                    executeUpdate(sqlQuery);
                }
            }
        } catch (SQLException e){
            throw new SAException(e);
        }
    }

    @Override
    public void insertEntry(IEntryHolder entryHolder) throws SAException {
        ITypeDesc typeDesc = entryHolder.getEntryData().getSpaceTypeDescriptor();
        String typeName = typeDesc.getTypeName();
        StringBuilder stringBuilder = new StringBuilder("INSERT INTO '").append(typeName).append("' (");
        for (PropertyInfo property : typeDesc.getProperties()) {
            stringBuilder.append("'").append(property.getName()).append("' ").append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(") ");
        stringBuilder.append(" VALUES (");
        for (PropertyInfo property : typeDesc.getProperties()) {
            Object propertyValue = entryHolder.getEntryData().getFixedPropertyValue(property.getOriginalIndex());
            stringBuilder.append(getValueString(property, propertyValue)).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(");");
        String sqlQuery = stringBuilder.toString();
        System.out.println(sqlQuery);
        try {
            executeUpdate(sqlQuery);
        } catch (SQLException e) {
            //TODO - check error code and throw EntryAllReadyInSpace when needed
            if(e instanceof SQLiteException && ((SQLiteException) e).getResultCode().code == 1555){
                throw new EntryAlreadyInSpaceException(entryHolder.getUID(), typeName);
            }
        }
        writeCount.incrementAndGet();
    }

    private String getValueString(SpacePropertyDescriptor property, Object propertyValue) {
        if (property.getType().equals(String.class)) {
            return "\"" + propertyValue + "\"";
        } else {
            return propertyValue.toString();
        }
    }


    @Override
    public void updateEntry(IEntryHolder updatedEntry) throws SAException {
        ITypeDesc typeDesc = updatedEntry.getEntryData().getSpaceTypeDescriptor();
        String typeName = typeDesc.getTypeName();
        StringBuilder stringBuilder = new StringBuilder("UPDATE '").append(typeName).append("' SET ");
        for (PropertyInfo property : typeDesc.getProperties()) {
            Object propertyValue = updatedEntry.getEntryData().getFixedPropertyValue(property.getOriginalIndex());
            stringBuilder.append("'").append(property.getName()).append("' = ").append(getValueString(property, propertyValue)).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(";");
        String sqlQuery = stringBuilder.toString();
        System.out.println(sqlQuery);
        try {
            executeUpdate(sqlQuery);
        } catch (SQLException e) {
           throw new SAException(e);
        }
    }

    @Override
    public void removeEntry(IEntryHolder entryPacket) throws SAException {

    }

    @Override
    public IEntryHolder getEntry(String typeName, ITemplateHolder templateHolder) throws SAException {
        return null;
    }

    @Override
    public IEntryHolder getEntry(String typeName, Object id) throws SAException {
        ITypeDesc typeDesc = typeManager.getTypeDesc(typeName);
        String idPropertyName = typeDesc.getIdPropertyName();
        String sqlQuery = "Select * from '" + typeDesc.getTypeName() + "' " +
                "WHERE " + idPropertyName + " = " + getValueString(typeDesc.getFixedProperty(idPropertyName), id) + ";";
        System.out.println(sqlQuery);
        ResultSet resultSet = executeQuery(sqlQuery);
        try {
            if (resultSet.next()) {
                PropertyInfo[] properties = typeDesc.getProperties();
                Object[] values = new Object[properties.length];
                for (int i = 0; i < properties.length; i++) {
                    values[i] = getPropertyValue(resultSet, properties[i]);
                }
                FlatEntryData data = new FlatEntryData(values, null, typeDesc.getEntryTypeDesc(EntryType.DOCUMENT_JAVA), 0, Lease.FOREVER, null);
                readCount.incrementAndGet();
                return new EntryHolder(typeManager.getServerTypeDesc(typeName), null, 0, false, data);
            } else {
                return null;
            }

        } catch (SQLException e) {
            throw new RuntimeException("could not get entry of type " + typeName + " with id " + id.toString(), e);
        }
    }


    @Override
    public ISAdapterIterator<IEntryCacheInfo> makeEntriesIter(String typeName, ITemplateHolder templateHolder) throws
            SAException {
        return null;
    }

    @Override
    public void shutDown() {
        FileUtils.deleteFileOrDirectoryIfExists(new File(PATH));
    }

    @Override
    public int getWriteCount() {
        return writeCount.get();
    }

    @Override
    public int getReadCount() {
        return readCount.get();
    }

    private Connection connectToDB(String jdbcDriver, String dbUrl, String user, String password, SQLiteConfig config) throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Class.forName(jdbcDriver);
        System.out.println("Connecting to database...");
        config.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, "wal");
        config.setPragma(SQLiteConfig.Pragma.CACHE_SIZE, "5000");
        Properties properties = config.toProperties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        conn = DriverManager.getConnection(dbUrl, properties);
        System.out.println("Creating table in given database...");
        return conn;
    }

    private void executeUpdate(String sqlQuery) throws SQLException {
        modifierLock.lock();
        connection.createStatement().executeUpdate(sqlQuery);
        modifierLock.unlock();
    }

    private ResultSet executeQuery(String sqlQuery) throws SAException {
        try {
            return connection.createStatement().executeQuery(sqlQuery);
        } catch (SQLException e) {
            throw new SAException("failed to create table in internal sqlite RDBMS", e);
        }
    }

    private String getPropertyType(PropertyInfo property) {
        Class<?> propertyType = property.getType();
        if (propertyType.equals(String.class)) {
            return "VARCHAR";
        } else if (propertyType.equals(boolean.class) || propertyType.equals(Boolean.class)) {
            return "BIT";
        } else if (propertyType.equals(byte.class) || propertyType.equals(Byte.class)) {
            return "TINYINT";
        } else if (propertyType.equals(short.class) || propertyType.equals(Short.class)) {
            return "SMALLINT";
        } else if (propertyType.equals(int.class) || propertyType.equals(Integer.class)) {
            return "INTEGER";
        } else if (propertyType.equals(long.class) || propertyType.equals(Long.class)) {
            return "BIGINT";
        } else if (propertyType.equals(BigInteger.class)) {
            return "BIGINT";
        } else if (propertyType.equals(BigDecimal.class)) {
            return "DECIMAL";
        } else if (propertyType.equals(float.class) || propertyType.equals(Float.class)) {
            return "REAL";
        } else if (propertyType.equals(double.class) || propertyType.equals(Double.class)) {
            return "float";
        } else if (propertyType.equals(byte[].class) || propertyType.equals(Byte[].class)) {
            return "BINARY";
        } else if (propertyType.equals(Timestamp.class)) {
            return "DATETIME";
        } else if (propertyType.equals(Time.class)) {
            return "BIGTIME";
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }

    private Object getPropertyValue(ResultSet resultSet, PropertyInfo property) throws SQLException {
        Class<?> propertyType = property.getType();
        int propertyIndex = property.getOriginalIndex() + 1;
        if (propertyType.equals(String.class)) {
            return resultSet.getString(propertyIndex);
        } else if (propertyType.equals(boolean.class) || propertyType.equals(Boolean.class)) {
            return resultSet.getBoolean(propertyIndex);
        } else if (propertyType.equals(byte.class) || propertyType.equals(Byte.class)) {
            return resultSet.getByte(propertyIndex);
        } else if (propertyType.equals(short.class) || propertyType.equals(Short.class)) {
            return resultSet.getShort(propertyIndex);
        } else if (propertyType.equals(int.class) || propertyType.equals(Integer.class)) {
            return resultSet.getInt(propertyIndex);
        } else if (propertyType.equals(long.class) || propertyType.equals(Long.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(BigInteger.class)) {
            return resultSet.getLong(propertyIndex);
        } else if (propertyType.equals(BigDecimal.class)) {
            return resultSet.getBigDecimal(propertyIndex);
        } else if (propertyType.equals(float.class) || propertyType.equals(Float.class)) {
            return resultSet.getFloat(propertyIndex);
        } else if (propertyType.equals(double.class) || propertyType.equals(Double.class)) {
            return resultSet.getDouble(propertyIndex);
        } else if (propertyType.equals(byte[].class) || propertyType.equals(Byte[].class)) {
            return resultSet.getBytes(propertyIndex);
        } else if (propertyType.equals(Timestamp.class)) {
            return resultSet.getTimestamp(propertyIndex);
        } else if (propertyType.equals(Time.class)) {
            return resultSet.getTime(propertyIndex);
        }
        throw new IllegalArgumentException("cannot map non trivial type " + propertyType.getName());
    }
}
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.utils.concurrent.ReentrantSimpleLock;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.lease.Lease;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteException;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gigaspaces.internal.server.space.tiered_storage.SqliteUtils.*;

public class SqliteRDBMS implements InternalRDBMS {
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    private static final String DB_NAME = "sqlite_db";
    private static final String PATH = "/tmp";
    private static final String DB_URL = "jdbc:sqlite:" + PATH +"/"+ DB_NAME;;
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
        } catch (SQLException e) {
            throw new SAException(e);
        }
    }

    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder) throws SAException {
        ITypeDesc typeDesc = entryHolder.getEntryData().getSpaceTypeDescriptor();
        String typeName = typeDesc.getTypeName();
        StringBuilder stringBuilder = new StringBuilder("INSERT INTO '").append(typeName).append("' (");
        for (PropertyInfo property : typeDesc.getProperties()) {
            stringBuilder.append("'").append(property.getName()).append("' ").append(",");
        }
        if (stringBuilder.charAt(stringBuilder.length() - 1) == ',') {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        stringBuilder.append(") ");
        stringBuilder.append(" VALUES (");
        for (PropertyInfo property : typeDesc.getProperties()) {
            Object propertyValue = entryHolder.getEntryData().getFixedPropertyValue(property.getOriginalIndex());
            stringBuilder.append(getValueString(property, propertyValue)).append(",");
        }
        if (stringBuilder.charAt(stringBuilder.length() - 1) == ',') {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        stringBuilder.append(");");
        String sqlQuery = stringBuilder.toString();
        System.out.println(sqlQuery);
        try {
            executeUpdate(sqlQuery);
        } catch (SQLException e) {
            //TODO - check error code and throw EntryAllReadyInSpace when needed
            if (e instanceof SQLiteException && ((SQLiteException) e).getResultCode().code == 1555) {
                throw new EntryAlreadyInSpaceException(entryHolder.getUID(), typeName);
            }
        }
        writeCount.incrementAndGet();
    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException {
        ITypeDesc typeDesc = updatedEntry.getEntryData().getSpaceTypeDescriptor();
        String typeName = typeDesc.getTypeName();
        StringBuilder stringBuilder = new StringBuilder("UPDATE '").append(typeName).append("' SET ");
        PropertyInfo idProperty = null;
        for (PropertyInfo property : typeDesc.getProperties()) {
            if (property.getName().equalsIgnoreCase(typeDesc.getIdPropertyName())) {
                idProperty = property;
            }
            Object propertyValue = updatedEntry.getEntryData().getFixedPropertyValue(property.getOriginalIndex());
            stringBuilder.append("'").append(property.getName()).append("' = ").append(getValueString(property, propertyValue)).append(",");
        }

        if (idProperty == null) {
            throw new SAException("could not find id property (" + typeDesc.getIdPropertyName() + ") in " + Arrays.toString(typeDesc.getProperties()));
        }
        if (stringBuilder.charAt(stringBuilder.length() - 1) == ',') {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        stringBuilder.append(" WHERE ").append(typeDesc.getIdPropertyName()).append(" = ")
                .append(getValueString(idProperty, updatedEntry.getEntryData().getFixedPropertyValue(idProperty.getOriginalIndex())));
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
    public boolean removeEntry(Context context, IEntryHolder entryHolder) throws SAException {
        ITypeDesc typeDesc = entryHolder.getServerTypeDesc().getTypeDesc();
        String idPropertyName = typeDesc.getIdPropertyName();
        String sqlQuery = "DELETE FROM '" + typeDesc.getTypeName() + "' " +
                "WHERE " + idPropertyName + " = " + getValueString(typeDesc.getFixedProperty(idPropertyName), entryHolder.getEntryId()) + ";";
        System.out.println(sqlQuery);
        try {
            int changedRows = executeUpdate(sqlQuery);
            return changedRows == 1;
        } catch (SQLException e) {
            throw new SAException("Failed to delete entry - type = "+typeDesc.getTypeName()+" , id = "+entryHolder.getEntryId(), e);
        }
    }

    @Override
    public IEntryHolder getEntry(Context context, String typeName, Object id) throws SAException {
        ITypeDesc typeDesc = typeManager.getTypeDesc(typeName);
        String idPropertyName = typeDesc.getIdPropertyName();
        String sqlQuery = "SELECT * FROM '" + typeDesc.getTypeName() + "' " +
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
                Object idFromEntry = data.getFixedPropertyValue(((PropertyInfo) typeDesc.getFixedProperty(typeDesc.getIdPropertyName())).getOriginalIndex());
                String uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc, idFromEntry);
                readCount.incrementAndGet();
                return new EntryHolder(typeManager.getServerTypeDesc(typeName), uid, 0, false, data);
            } else {
                return null;
            }

        } catch (SQLException e) {
            throw new RuntimeException("could not get entry of type " + typeName + " with id " + id.toString(), e);
        }
    }

    @Override
    public ISAdapterIterator<IEntryCacheInfo> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder, TieredState tieredState) throws SAException {
        if (templateHolder.getCustomQuery() != null) {
            ICustomQuery customQuery = templateHolder.getCustomQuery();
            //TODO - convert customQuery to sql
            return null;
        } else {
            ITypeDesc typeDesc = templateHolder.getServerTypeDesc().getTypeDesc();
            ;
            TemplateEntryData entryData = templateHolder.getTemplateEntryData();
            if (templateHolder.getExtendedMatchCodes() == null) {//by template
                StringBuilder stringBuilder = new StringBuilder("SELECT * FROM '").append(typeDesc.getTypeName()).append("' WHERE ");
                for (PropertyInfo property : typeDesc.getProperties()) {
                    Object value = entryData.getFixedPropertyValue(property.getOriginalIndex());
                    if (value != null) {
                        stringBuilder.append(property.getName()).append(" = ").append(getValueString(property, value)).append(" AND ");
                    }
                }
                int lastIndexOf = stringBuilder.lastIndexOf(" AND ");
                if (lastIndexOf != -1) {
                    stringBuilder.delete(lastIndexOf, stringBuilder.length() - 1);
                } else {
                    stringBuilder.delete(stringBuilder.lastIndexOf(" WHERE"), stringBuilder.length() - 1);
                }
                stringBuilder.append(";");
                String sqlQuery = stringBuilder.toString();
                System.out.println(sqlQuery);
                ResultSet resultSet = executeQuery(sqlQuery);
                return new RDBMSIterator(resultSet, typeDesc, typeManager);
            } else if (templateHolder.getExtendedMatchCodes() != null) {
                short[] matchCodes = templateHolder.getExtendedMatchCodes();
                StringBuilder stringBuilder = new StringBuilder("SELECT * FROM '").append(typeDesc.getTypeName()).append("' WHERE ");
                for (PropertyInfo property : typeDesc.getProperties()) {
                    int originalIndex = property.getOriginalIndex();
                    Object value = entryData.getFixedPropertyValue(originalIndex);
                    if (value != null) {
                        stringBuilder.append(property.getName()).append(getMatchCodeString(matchCodes[originalIndex])).append(getValueString(property, value)).append(" AND ");
                    }
                }
                int lastIndexOf = stringBuilder.lastIndexOf(" AND ");
                if (lastIndexOf != -1) {
                    stringBuilder.delete(lastIndexOf, stringBuilder.length() - 1);
                } else {
                    stringBuilder.delete(stringBuilder.lastIndexOf(" WHERE"), stringBuilder.length() - 1);
                }
                stringBuilder.append(";");
                String sqlQuery = stringBuilder.toString();
                System.out.println(sqlQuery);
                ResultSet resultSet = executeQuery(sqlQuery);
                return new RDBMSIterator(resultSet, typeDesc, typeManager);
            }
        }
        return null;
    }

    @Override
    public void shutDown() {
        File folder = new File(PATH);
        final File[] files = folder.listFiles((dir, name) -> name.matches(DB_NAME+".*"));
        for (final File file : Objects.requireNonNull(files)) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
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

    private int executeUpdate(String sqlQuery) throws SQLException {
        try {
            modifierLock.lock();
            return connection.createStatement().executeUpdate(sqlQuery);
        } finally {
            modifierLock.unlock();
        }
    }

    private ResultSet executeQuery(String sqlQuery) throws SAException {
        try {
            return connection.createStatement().executeQuery(sqlQuery);
        } catch (SQLException e) {
            throw new SAException("failed to create table in internal sqlite RDBMS", e);
        }
    }
}
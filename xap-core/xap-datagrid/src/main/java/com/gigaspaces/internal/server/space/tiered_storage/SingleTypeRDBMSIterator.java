package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.EntryHolder;
import com.gigaspaces.internal.server.storage.FlatEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.lease.Lease;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.gigaspaces.internal.server.space.tiered_storage.SqliteUtils.getPropertyValue;

public class SingleTypeRDBMSIterator implements ISAdapterIterator<IEntryHolder> {

    private final ResultSet resultSet;
    private final String typeName;
    private final SpaceTypeManager typeManager;
    private final ITypeDesc typeDesc;

    public SingleTypeRDBMSIterator(ResultSet resultSet, ITypeDesc typeDesc, SpaceTypeManager typeManager) {
        this.resultSet = resultSet;
        this.typeManager = typeManager;
        this.typeDesc = typeDesc;
        this.typeName = typeDesc.getTypeName();
    }

    @Override
    public IEntryHolder next() throws SAException {
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
                return new EntryHolder(typeManager.getServerTypeDesc(typeName), uid, 0, false, data);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new SAException("failed to read row from result set for type " + typeName, e);
        }
    }

    @Override
    public void close() throws SAException {
        try {
            resultSet.close();
        } catch (SQLException e) {
            throw new SAException("failed to close result set for type " + typeName, e);
        }
    }
}

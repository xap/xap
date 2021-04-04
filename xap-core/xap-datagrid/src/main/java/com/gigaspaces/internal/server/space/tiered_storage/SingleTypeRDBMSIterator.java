package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.sql.ResultSet;
import java.sql.SQLException;

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
                return TieredStorageUtils.getEntryHolderFromRow(typeManager.getServerTypeDesc(typeName), resultSet);
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

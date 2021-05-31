package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;
import java.sql.SQLException;

public class SingleTypeRDBMSIterator implements ISAdapterIterator<IEntryHolder> {

    private final RDBMSResult result;
    private final String typeName;
    private final SpaceTypeManager typeManager;

    public SingleTypeRDBMSIterator(RDBMSResult result, ITypeDesc typeDesc, SpaceTypeManager typeManager) {
        this.result = result;
        this.typeManager = typeManager;
        this.typeName = typeDesc.getTypeName();
    }

    @Override
    public IEntryHolder next() throws SAException {
        try {
            if (result.next()) {
                return TieredStorageUtils.getEntryHolderFromRow(typeManager.getServerTypeDesc(typeName), result.getResultSet());
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
            result.close();
        } catch (IOException e) {
            throw new SAException("failed to close result set for type " + typeName, e);
        }
    }

    public RDBMSResult getResult() {
        return result;
    }

    public String getTypeName() {
        return typeName;
    }

    public SpaceTypeManager getTypeManager() {
        return typeManager;
    }
}

package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.sadapter.SAException;

import java.sql.SQLException;

public class TimeBasedTypeRDBMSInitialLoadIterator extends SingleTypeRDBMSIterator {

    private final TimePredicate predicate;
    private final LeaseManager leaseManager;

    public TimeBasedTypeRDBMSInitialLoadIterator(RDBMSResult result, ITypeDesc typeDesc, SpaceTypeManager typeManager, TimePredicate predicate, LeaseManager leaseManager) {
        super(result, typeDesc, typeManager);
        this.predicate = predicate;
        this.leaseManager = leaseManager;
    }

    @Override
    public IEntryHolder next() throws SAException {
        try {
            if (getResult().next()) {
                return TieredStorageUtils.getEntryHolderFromRow(getTypeManager().getServerTypeDesc(getTypeName()), getResult().getResultSet(), predicate, leaseManager);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new SAException("failed to read row from result set for type " + getTypeName(), e);
        }
    }
}

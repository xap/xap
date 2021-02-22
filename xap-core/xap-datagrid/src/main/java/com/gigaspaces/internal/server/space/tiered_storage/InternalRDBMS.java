package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

public interface InternalRDBMS {

    void initialize(SpaceTypeManager typeManager) throws SAException;

    void createTable(ITypeDesc typeDesc) throws SAException;

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     */
    void insertEntry(Context context,  IEntryHolder entryHolder) throws SAException;

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException;

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryPacket entry to remove
     */
    void removeEntry(Context context, IEntryHolder entryPacket) throws SAException;


    /**
     * Gets an entry object from internalDiskStorage.
     *
     * @param typeName      class of the entry to get
     * @param templateHolder selection template,may be null, currently used by cacheload/cachestore in
     *                       order to pass primary key fields when GS uid is not saved in an external DB
     * @return IEntryPacket
     */
    IEntryHolder getEntry(Context context, String typeName, ITemplateHolder templateHolder) throws SAException;

    IEntryHolder getEntry(Context context, String typeName, Object id) throws SAException;

    ISAdapterIterator<IEntryCacheInfo> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder) throws SAException;

    void shutDown();


    //Temporary for tests
    int getWriteCount();

    int getReadCount();
}


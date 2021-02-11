package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

public interface InternalRDBMS {

    void initialize() throws SAException;

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     */
    void insertEntry(IEntryHolder entryHolder) throws SAException;

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    void updateEntry(IEntryHolder updatedEntry) throws SAException;

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryPacket entry to remove
     */
    void removeEntry(IEntryHolder entryPacket) throws SAException;


    /**
     * Gets an entry object from internalDiskStorage.
     *
     * @param className      class of the entry to get
     * @param templateHolder selection template,may be null, currently used by cacheload/cachestore in
     *                       order to pass primary key fields when GS uid is not saved in an external DB
     * @return IEntryPacket
     */
    IEntryHolder getEntry(String className, ITemplateHolder templateHolder) throws SAException;

    IEntryHolder getEntry(String className, Object id) throws SAException;

    ISAdapterIterator<IEntryCacheInfo> makeEntriesIter(String typeName, ITemplateHolder templateHolder) throws SAException;

    void shutDown() throws SAException;

}


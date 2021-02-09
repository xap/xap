package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.util.Map;

public interface InternalRDBMS {

    void initialize() throws SAException;

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryPacket entry to insert
     */
    void insertEntry(IEntryPacket entryPacket) throws SAException;

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    void updateEntry(IEntryPacket updatedEntry) throws SAException;

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryPacket entry to remove
     */
    void removeEntry(IEntryPacket entryPacket) throws SAException;


    /**
     * Gets an entry object from internalDiskStorage.
     *
     * @param className class of the entry to get
     * @param template  selection template,may be null, currently used by cacheload/cachestore in
     *                  order to pass primary key fields when GS uid is not saved in an external DB
     * @return IEntryPacket
     */
    IEntryPacket getEntry(String className, ITemplatePacket template) throws SAException;


    ISAdapterIterator initialLoad(Map<String, CachePredicate> cacheRules) throws SAException;

    Map<String, IEntryPacket> getEntries(String typeName, ITemplatePacket template) throws SAException;

    ISAdapterIterator<IEntryPacket> makeEntriesIter(String typeName, ITemplatePacket template) throws SAException;

    void shutDown() throws SAException;

}


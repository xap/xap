package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockRDBMS implements InternalRDBMS {
    private Map<Object, IEntryPacket> data;
    private AtomicInteger readCount = new AtomicInteger(0);
    private AtomicInteger writeCount = new AtomicInteger(0);

    @Override
    public void initialize() throws SAException {
        data = new HashMap<>();
    }

    @Override
    public void insertEntry(IEntryPacket entryPacket) throws SAException {
        writeCount.incrementAndGet();
        data.put(entryPacket.getID(), entryPacket);
    }

    @Override
    public void updateEntry(IEntryPacket updatedEntry) throws SAException {

    }

    @Override
    public void removeEntry(IEntryPacket entryPacket) throws SAException {

    }

    @Override
    public IEntryPacket getEntry(String className, ITemplatePacket template) throws SAException {
        readCount.incrementAndGet();
        if (template.isIdQuery()) {
            return data.get(template.getID());
        }
        return null;
    }

    @Override
    public ISAdapterIterator initialLoad(Map<String, CachePredicate> cacheRules) throws SAException {
        return null;
    }

    @Override
    public Map<String, IEntryPacket> getEntries(String typeName, ITemplatePacket template) throws SAException {
        return null;
    }

    @Override
    public ISAdapterIterator<IEntryPacket> makeEntriesIter(String typeName, ITemplatePacket template) throws SAException {
        return null;
    }

    @Override
    public void shutDown() throws SAException {

    }

    public int getReadCount() {
        return readCount.get();
    }

    public int getWriteCount() {
        return writeCount.get();
    }

    public Map<Object, IEntryPacket> getData() {
        return data;
    }
}

package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockRDBMS implements InternalRDBMS {
    private Map<Object, IEntryHolder> data;
    private AtomicInteger readCount = new AtomicInteger(0);
    private AtomicInteger writeCount = new AtomicInteger(0);

    @Override
    public void initialize() throws SAException {
        data = new HashMap<>();
    }

    @Override
    public void insertEntry(IEntryHolder entryHolder) throws SAException {
        writeCount.incrementAndGet();
        data.put(entryHolder.getEntryId(), entryHolder);
    }

    @Override
    public void updateEntry(IEntryHolder updatedEntry) throws SAException {

    }

    @Override
    public void removeEntry(IEntryHolder entryPacket) throws SAException {

    }

    @Override
    public IEntryHolder getEntry(String className, ITemplateHolder templateHolder) throws SAException {
        return null;
    }

    @Override
    public IEntryHolder getEntry(String className, Object id) throws SAException {
        readCount.incrementAndGet();
        return data.get(id);
    }

    @Override
    public ISAdapterIterator<IEntryCacheInfo> makeEntriesIter(String typeName, ITemplateHolder templateHolder) throws SAException {
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

    public Map<Object, IEntryHolder> getData() {
        return data;
    }
}

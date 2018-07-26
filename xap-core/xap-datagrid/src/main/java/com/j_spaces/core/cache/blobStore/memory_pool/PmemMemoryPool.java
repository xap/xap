package com.j_spaces.core.cache.blobStore.memory_pool;

import com.gigaspaces.pmem.TempPmemDriverJNI;
import com.gigaspaces.pmem.TempPmemException;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class PmemMemoryPool extends AbstractMemoryPool {

    private final boolean verbose;
    private String fileName;
    private Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private TempPmemDriverJNI pmemDriver = new TempPmemDriverJNI();

    public PmemMemoryPool(long threshold, String fileName, boolean verbose) {
        super(threshold);
        this.fileName = fileName;
        this.verbose = verbose;
    }

    public void initPool(String spaceName){
        try {
            fileName += "_"+spaceName;
            pmemDriver.init(fileName, threshold, verbose);
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to init pmem pool", e);
            throw new RuntimeException("Failed to init pmem pool", e);
        }
    }

    @Override
    public void write(IBlobStoreOffHeapInfo info, byte[] buf) {
        long offset = BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY;
        try {
            offset = pmemDriver.add(buf);
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to add object to pmem pool " + fileName, e);
            throw new RuntimeException("Failed to add object to pmem pool " + fileName, e);
        } finally {
            info.setOffHeapAddress(offset);
        }
        incrementMetrics(buf.length, info.getTypeName());
    }

    @Override
    public byte[] get(IBlobStoreOffHeapInfo info) {
        try {
            return pmemDriver.get(info.getOffHeapAddress());
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to retrieve object at offset " + info.getOffHeapAddress() + " to pmem pool " + fileName, e);
            throw new RuntimeException("Failed to retrieve object at offset " + info.getOffHeapAddress() + " to pmem pool " + fileName, e);
        }
    }

    @Override
    public void update(IBlobStoreOffHeapInfo info, byte[] buf) {
        try {
            //TODO: optimize - move metric counters to c and add getters in JNI
            decrementMetrics(pmemDriver.get(info.getOffHeapAddress()).length, info.getTypeName());
            info.setOffHeapAddress(pmemDriver.replace(buf, info.getOffHeapAddress()));
            incrementMetrics(info.getOffHeapAddress(), info.getTypeName());
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to update object at offset " + info.getOffHeapAddress() + " in pmem pool " + fileName, e);
            throw new RuntimeException("Failed to update object at offset " + info.getOffHeapAddress() + " in pmem pool " + fileName, e);
        }
    }

    @Override
    public void delete(IBlobStoreOffHeapInfo info) {
        try {
            decrementMetrics(pmemDriver.get(info.getOffHeapAddress()).length, info.getTypeName());
            pmemDriver.delete(info.getOffHeapAddress());
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to delete object at offset " + info.getOffHeapAddress() + " from pmem pool " + fileName, e);
            throw new RuntimeException("Failed to delete object at offset " + info.getOffHeapAddress() + " from pmem pool " + fileName, e);
        }
    }

    public void executeBulk(byte[] operations, long[] offsets, Object[] operationsData) {
        try {
            pmemDriver.executeBulk(operations, offsets, operationsData);
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to execute bulk operation in pmem pool " + fileName, e);
            throw new RuntimeException("Failed to execute bulk operation in pmem pool " + fileName, e);
        }
    }

    public String getFileName() {
        return fileName;
    }

    public void close() {
        try {
            pmemDriver.close(fileName);
        } catch (TempPmemException e) {
            logger.log(Level.SEVERE, "Failed to close pmem pool " + fileName, e);
            throw new RuntimeException("Failed to close pmem pool " + fileName, e);
        }
    }
}

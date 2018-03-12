/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//

package com.j_spaces.core.cache.blobStore.offheap;

import com.gigaspaces.internal.utils.concurrent.UnsafeHolder;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yael Nahon
 * @since 12.2
 */
public class OffHeapMemoryPool {

    private volatile static Unsafe _unsafe;
    private static Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final int MINIMAL_BUFFER_DIFF_TO_ALLOCATE = 50;

    private final long threshold;
    private final LongCounter totalCounter = new LongCounter();
    private final Map<String, LongCounter> typesCounters = new ConcurrentHashMap<String, LongCounter>();
    private MetricRegistrator metricRegistrator;

    public OffHeapMemoryPool(long threshold) {
        this.threshold = threshold;
        if(!UnsafeHolder.isAvailable()){
            throw new RuntimeException(" unsafe instance could not be obtained");
        }
    }

    public long getThreshold() {
        return threshold;
    }

    public void initMetrics(MetricRegistrator metricRegistrator) {
        this.metricRegistrator = metricRegistrator.extend("off-heap");
        this.metricRegistrator.register(metricsPath("total"), totalCounter);
    }

    private String metricsPath(String typeName) {
        return metricRegistrator.toPath("used-bytes", typeName);
    }

    public void register(String typeName) {
        LongCounter counter = new LongCounter();
        typesCounters.put(typeName, counter);
        metricRegistrator.register(metricsPath(typeName), counter);
    }

    public void unregister(String typeName) {
        typesCounters.remove(typeName);
        metricRegistrator.unregisterByPrefix(metricsPath(typeName));
    }

    public void allocateAndWrite(IBlobStoreOffHeapInfo info, byte[] buf, boolean fromUpdate) {
        long newAddress;

        if(info.getOffHeapAddress() == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY){
            fromUpdate = false;
        }

        if (info.getOffHeapAddress() != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY && !fromUpdate) {
            throw new IllegalStateException("trying to allocateAndWrite when already allocated in off heap");
        }

        int headerSize = calculateHeaderSize(buf.length);
        try {
            if(fromUpdate){
                newAddress = UnsafeHolder.reallocateMemory(info.getOffHeapAddress(), headerSize + buf.length);
            } else {
                newAddress = UnsafeHolder.allocateMemory(headerSize + buf.length);
            }

        } catch (Error e) {
            logger.log(Level.SEVERE, "failed to allocateAndWrite offheap space", e);
            throw e;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to allocateAndWrite offheap space");
            throw new RuntimeException("failed to allocateAndWrite offheap space", e);
        }
        if (newAddress == 0) {
            logger.log(Level.SEVERE, "failed to allocateAndWrite offheap space");
            throw new RuntimeException("failed to allocateAndWrite offheap space");
        }

        putHeaderToUnsafe(newAddress, buf.length);
        writeBytes(newAddress + headerSize, buf);
        info.setOffHeapAddress(newAddress);
        incrementMetrics(headerSize + buf.length, info.getTypeName());
    }

    public byte[] get(long address) {
        if (address == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to read from off heap but no address found");
        }
        int headerSize = getHeaderSizeFromUnsafe(address);
        int numOfBytes = getHeaderFromUnsafe(address, headerSize);
        byte[] bytes = readBytes(address + (long) (headerSize), numOfBytes);
        return bytes;
    }

    public void update(IBlobStoreOffHeapInfo info, byte[] buf) {
        if (info.getOffHeapAddress() == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to update when no off heap memory is allocated");
        }
        int oldHeaderSize = getHeaderSizeFromUnsafe(info.getOffHeapAddress());
        int oldEntryLength = getHeaderFromUnsafe(info.getOffHeapAddress(), oldHeaderSize);
        if (oldEntryLength < buf.length || (oldEntryLength - buf.length >= MINIMAL_BUFFER_DIFF_TO_ALLOCATE)) {
            delete(info, true);
            allocateAndWrite(info, buf, true);
        } else {
            writeBytes(info.getOffHeapAddress() + (long) (oldHeaderSize), buf);
        }
    }

    public void delete(IBlobStoreOffHeapInfo info, boolean fromUpdate) {
        long valuesAddress = info.getOffHeapAddress();
        if (valuesAddress != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            int headerSize = getHeaderSizeFromUnsafe(info.getOffHeapAddress());
            int numOfBytes = getHeaderFromUnsafe(valuesAddress,headerSize);
            if(!fromUpdate){
                UnsafeHolder.freeFromMemory(valuesAddress);
                info.setOffHeapAddress(BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY);
            }
            decrementMetrics(headerSize + numOfBytes, info.getTypeName());
        }
    }

    public long getUsedBytes() {
        return totalCounter.getCount();
    }

    private void incrementMetrics(long n, String typeName) {
        totalCounter.inc(n);
        LongCounter typeCounter = typesCounters.get(typeName);
        if (typeCounter != null)
            typeCounter.inc(n);
    }

    private void decrementMetrics(long n, String typeName) {
        totalCounter.dec(n);
        LongCounter typeCounter = typesCounters.get(typeName);
        if (typeCounter != null)
            typeCounter.dec(n);

    }

    private static void writeBytes(long address, byte[] bytes) {
        UnsafeHolder.copyByteArrayToMemory(bytes, address, bytes.length);
    }

    private static byte[] readBytes(long address, int numOfBytes) {
        byte[] res = new byte[numOfBytes];
        UnsafeHolder.copyByteArrayFromMemory(res, address, numOfBytes);
        return res;
    }

    private static int calculateHeaderSize(int bufferLen) {
        if (bufferLen > Integer.MAX_VALUE / 2)
            throw new RuntimeException("Illegal buffer length =" + bufferLen);
        for (int left = 3; left >= 0; left--) {
            int next = (bufferLen >>> (left * 8)) & 0xFF;
            if (next != 0) {
                if ((next & 0x0C0) != 0)
                    return (left + 2);
                else
                    return left + 1;
            }
        }
        throw new RuntimeException("Illegal buffer length =" + bufferLen);
    }

    private static int putHeaderToUnsafe(long address, int bufferLen) {
        int headerSize = 0;
        boolean started = false;
        for (int left = 3; left >= 0; left--) {
            int next = (bufferLen >>> (left * 8)) & 0xFF;
            if (!started) {
                if (next == 0)
                    continue;
                if ((next & 0x0C0) != 0) {
                    headerSize = left + 1;
                    UnsafeHolder.putByte(address, (byte) (headerSize << 6));
                    address++;
                } else {
                    headerSize = left;
                    next |= ((headerSize << 6));
                }
                headerSize++;
                started = true;
            }
            UnsafeHolder.putByte(address, (byte) (next));
            address++;
        }
        return headerSize;
    }

    private static int getHeaderSizeFromUnsafe(long address) {
        return (((UnsafeHolder.getByte(address)) & 0xC0) >> 6) + 1;
    }

    private static int getHeaderFromUnsafe(long address, int headerSize) {
        int len = 0;
        for (int i = 0; i < headerSize; i++) {
            int intByte = UnsafeHolder.getByte(address);
            if (i == 0)
                intByte &= 0x3F;
            else
                intByte &= 0xFF;
            len |= (intByte << ((headerSize - 1 - i) * 8));
            address++;
        }
        return len;
    }
}

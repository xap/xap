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

package com.j_spaces.core.cache.blobStore.optimizations;

import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yael Nahon
 * @since 12.2
 */
public class OffHeapIndexesValuesHandler {

    private volatile static Unsafe _unsafe;
    private static Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final int MINIMAL_BUFFER_DIFF_TO_ALLOCATE = 50;

    private static Unsafe getUnsafe() {
        if (_unsafe == null) {
            Constructor<Unsafe> unsafeConstructor = null;
            try {
                unsafeConstructor = Unsafe.class.getDeclaredConstructor();
                unsafeConstructor.setAccessible(true);
                _unsafe = unsafeConstructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("could not get unsafe instance");
            }
        }
        return _unsafe;
    }

    public static long allocate(byte[] buf, long address, LongCounter offHeapByteCounter, LongCounter offHeapTypeCounter) {
        long newAddress;

        if (address != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to allocate when already allocated in off heap");
        }
        int headerSize = calculateHeaderSize(buf.length);
        try {
            newAddress = getUnsafe().allocateMemory(headerSize + buf.length);
        } catch (Error e) {
            logger.log(Level.SEVERE, "failed to allocate offheap space", e);
            throw e;
        } catch (Exception e){
            logger.log(Level.SEVERE, "failed to allocate offheap space");
            throw new RuntimeException("failed to allocate offheap space");
        }
        if (newAddress == 0) {
            logger.log(Level.SEVERE, "failed to allocate offheap space");
            throw new RuntimeException("failed to allocate offheap space");
        }

        putHeaderToUnsafe(newAddress, buf.length);
        writeBytes(newAddress + headerSize, buf);
        offHeapByteCounter.inc(headerSize + buf.length);
        offHeapTypeCounter.inc(headerSize + buf.length);
        return newAddress;

    }

    public static byte[] get(long address) {
        if (address == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to read from off heap but no address found");
        }
        int headerSize = getHeaderSizeFromUnsafe(address);
        int numOfBytes = getHeaderFromUnsafe(address,headerSize);
        byte[] bytes = readBytes(address + (long)(headerSize), numOfBytes);
        return bytes;
    }

    public static void update(IBlobStoreOffHeapInfo info, byte[] buf, LongCounter offHeapByteCounter, LongCounter offHeapTypeCounter) {
        if (info.getOffHeapAddress() == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to update when no off heap memory is allocated");
        }
        int oldHeaderSize = getHeaderSizeFromUnsafe(info.getOffHeapAddress());
        int oldEntryLength = getHeaderFromUnsafe(info.getOffHeapAddress(),oldHeaderSize);
        if (oldEntryLength < buf.length || (oldEntryLength - buf.length >= MINIMAL_BUFFER_DIFF_TO_ALLOCATE)) {
            delete(info, offHeapByteCounter, offHeapTypeCounter);
            info.setOffHeapAddress(allocate(buf, info.getOffHeapAddress(), offHeapByteCounter, offHeapTypeCounter));
        }
        else {
            writeBytes(info.getOffHeapAddress() + (long)(oldHeaderSize), buf);
        }
    }

    public static void delete(IBlobStoreOffHeapInfo info, LongCounter offHeapByteCounter, LongCounter offHeapTypeCounter) {
        long valuesAddress = info.getOffHeapAddress();
        if (valuesAddress != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            int headerSize = getHeaderSizeFromUnsafe(info.getOffHeapAddress());
            int numOfBytes = getHeaderFromUnsafe(valuesAddress,headerSize);
            getUnsafe().freeMemory(valuesAddress);
            info.setOffHeapAddress(BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY);
            offHeapByteCounter.dec(headerSize + numOfBytes);
            offHeapTypeCounter.dec(headerSize + numOfBytes);
        }
    }

    private static void writeBytes(long address, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            getUnsafe().putByte(address, bytes[i]);
            address++;
        }
    }

    private static byte[] readBytes(long address, int numOfBytes) {
        byte[] res = new byte[numOfBytes];
        for (int i = 0; i < numOfBytes; i++) {
            res[i] = getUnsafe().getByte(address);
            address++;
        }
        return res;
    }

    private static int calculateHeaderSize(int bufferLen) {
        if (bufferLen > Integer.MAX_VALUE /2 )
            throw new RuntimeException("illigal buffer length =" + bufferLen);
        for (int left =3; left >= 0; left-- )
        {
            int next = (bufferLen >>> (left*8)) & 0xFF;
            if (next != 0)
            {
                if ((next & 0x0C0) !=0)
                    return (left + 2);
                else
                    return left +1;
            }
        }
        throw new RuntimeException("illigal buffer length =" + bufferLen);
    }

    private static int putHeaderToUnsafe(long address, int bufferLen) {
        int headerSize = 0;
        boolean started = false;
        for (int left =3; left >= 0; left-- ) {
            int next = (bufferLen >>> (left * 8)) & 0xFF;
            if (!started) {
                if (next == 0)
                    continue;
                if ((next & 0x0C0) != 0) {
                    headerSize = left + 1;
                    getUnsafe().putByte(address, (byte) (headerSize << 6));
                    address++;
                } else {
                    headerSize = left;
                    next |= ((headerSize << 6));
                }
                headerSize++;
                started = true;
            }
            getUnsafe().putByte(address, (byte) (next));
            address++;
        }
        return headerSize;
    }

    private static int getHeaderSizeFromUnsafe(long address) {
        return (((getUnsafe().getByte(address)) & 0xC0) >>6) + 1;
    }

    private static int getHeaderFromUnsafe(long address, int headerSize) {
        int len =0;
        for (int i=0; i<headerSize; i++)
        {
            int intByte = getUnsafe().getByte(address);
            if (i==0)
                intByte &= 0x3F;
            else
                intByte &= 0xFF;
            len |= (intByte << ((headerSize -1 -i) * 8));
            address++;
        }
        return len;
    }


}

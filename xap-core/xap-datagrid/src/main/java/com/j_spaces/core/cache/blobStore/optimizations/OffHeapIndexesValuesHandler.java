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
    private static int CONSTANT_PREFIX_SIZE = 4;

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

    public static long allocate(byte[] buf, long address) {
        long newAddress;

        if (address != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to allocate when already allocated in off heap");
        }
        try {
            newAddress = getUnsafe().allocateMemory(CONSTANT_PREFIX_SIZE + buf.length);
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
        getUnsafe().putInt(newAddress, buf.length);
        writeBytes(newAddress + CONSTANT_PREFIX_SIZE, buf);
        return newAddress;

    }

    public static byte[] get(long address) {
        if (address == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to read from off heap but no address found");
        }
        int numOfBytes = getUnsafe().getInt(address);
        byte[] bytes = readBytes(address + CONSTANT_PREFIX_SIZE, numOfBytes);
        return bytes;
    }

    public static void update(IBlobStoreOffHeapInfo info, byte[] buf) {
        if (info.getOffHeapAddress() == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            throw new IllegalStateException("trying to update when no off heap memory is allocated");
        }
        int oldEntryLength = getUnsafe().getInt(info.getOffHeapAddress());
        if (oldEntryLength < buf.length) {
            delete(info);
            info.setOffHeapAddress(allocate(buf, info.getOffHeapAddress()));
        }
        else {
            getUnsafe().putInt(info.getOffHeapAddress(), buf.length);
            writeBytes(info.getOffHeapAddress() + CONSTANT_PREFIX_SIZE, buf);
        }
    }

    public static void delete(IBlobStoreOffHeapInfo info) {
        long valuesAddress = info.getOffHeapAddress();
        if (valuesAddress != BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) {
            unsafeDelete(valuesAddress);
            info.setOffHeapAddress(BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY);
        }
    }

    private static void unsafeDelete(long address) {
        getUnsafe().freeMemory(address);
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

}

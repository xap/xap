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

package com.j_spaces.core.cache.offHeap.optimizations;

import com.j_spaces.core.cache.offHeap.OffHeapRefEntryCacheInfo;
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
    private static int numOfBytes = 40;
    private static Logger logger = Logger.getLogger("MyLogger");

    private static Unsafe getUnsafe() {
        if (_unsafe == null) {
            logger.log(Level.INFO,"***** created unsafe instance *****");
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

    public static long allocate(){
        long address;
        try {
            address = getUnsafe().allocateMemory(numOfBytes);
//            logger.log(Level.INFO,"***** allocated off heap space at address "+address+"*****");
        }catch (Error e){
            logger.log(Level.SEVERE, "failed to allocate offheap space", e);
            throw e;
        }
        if(address == 0){
            logger.log(Level.SEVERE, "failed to allocate offheap space");
            throw new RuntimeException("failed to allocate offheap space");
        }
        getUnsafe().setMemory(address, numOfBytes, (byte) 0);
        return address;
    }

    public static byte[] get(long address){
        if(address==OffHeapRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY) return null;
//        logger.log(Level.INFO,"***** reading off heap space from address "+address+"*****");
        byte[] res = new byte[numOfBytes];
        for (int i = 0; i < numOfBytes; i++) {
            res[i] = getUnsafe().getByte(address);
            address++;
        }
        return res;
    }


    public static void delete(OffHeapRefEntryCacheInfo entryCacheInfo){
        long valuesAddress = entryCacheInfo.getOffHeapIndexValuesAddress();
        if(valuesAddress != OffHeapRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY ){
            unsafeDelete(valuesAddress);
            entryCacheInfo.setOffHeapIndexValuesAddress(OffHeapRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY);
        }else {
            logger.log(Level.INFO,"***** free off heap space at address "+valuesAddress+" could not be performed*****");
        }
    }

    public static void unsafeDelete(long address){
//        logger.log(Level.INFO,"***** freeing off heap space at address "+address+"*****");
        getUnsafe().freeMemory(address);
    }

    public static void update(long address){
        if(address == OffHeapRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY){
            throw new IllegalStateException("trying to update when no off heap memory is allocated");
        }
//        logger.log(Level.INFO,"***** updated off heap space at address "+address+"*****");
        getUnsafe().setMemory(address, numOfBytes, (byte) 1);
    }

}

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

package com.gigaspaces.internal.utils.concurrent;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class UnsafeHolder {

    private static final Logger logger = Logger.getLogger("com.gigaspaces.core.unsafe");

    private static final Unsafe _unsafe = initUnsafe();

    public static final long BYTE_ARR_OFF = _unsafe != null ? _unsafe.arrayBaseOffset(byte[].class) : 0;

    private static Unsafe initUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Throwable e) {
            logger.log(Level.FINE, "Fail to initialize Unsafe.", e);
            return null;
        }
    }

    public static boolean isAvailable() {
        return _unsafe != null;
    }

    public static long objectFieldOffset(Field field) {
        return _unsafe.objectFieldOffset(field);
    }

    public static Object getObjectVolatile(Object instance, long offset) {
        return _unsafe.getObjectVolatile(instance, offset);
    }

    public static void putObjectVolatile(Object instance, long offset, Object newValue) {
        _unsafe.putObjectVolatile(instance, offset, newValue);
    }

    public static boolean compareAndSwapObject(Object instance, long offset, Object expected, Object newValue) {
        return _unsafe.compareAndSwapObject(instance, offset, expected, newValue);
    }

    public static int getIntVolatile(Object instance, long offset) {
        return _unsafe.getIntVolatile(instance, offset);
    }

    public static void putIntVolatile(Object instance, long offset, int newValue) {
        _unsafe.putIntVolatile(instance, offset, newValue);
    }

    public static boolean compareAndSwapInt(Object instance, long offset, int expected, int newValue) {
        return _unsafe.compareAndSwapInt(instance, offset, expected, newValue);
    }

    public static boolean compareAndSwapLong(Object instance, long offset, long expected, long newValue) {
        return _unsafe.compareAndSwapLong(instance, offset, expected, newValue);
    }

    public static long allocateMemory(long size) {
        return _unsafe.allocateMemory(size);
    }

    public static long reallocateMemory(long oldAddress, long size) {
        return _unsafe.reallocateMemory(oldAddress, size);
    }

    public static void copyByteArrayToMemory(byte[] data, long address, int length) {
        _unsafe.copyMemory(data, BYTE_ARR_OFF, null, address, length);
    }

    public static void copyByteArrayFromMemory(byte[] destination, long address, int length) {
        _unsafe.copyMemory(null, address, destination, BYTE_ARR_OFF, length);
    }

    public static void freeFromMemory(long address) {
        _unsafe.freeMemory(address);
    }

    public static void putByte(long address, byte b) {
        _unsafe.putByte(address, b);
    }

    public static int getByte(long address) {
        return _unsafe.getByte(address);
    }
}

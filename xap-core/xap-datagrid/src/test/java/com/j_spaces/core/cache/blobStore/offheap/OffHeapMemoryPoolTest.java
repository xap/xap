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
package com.j_spaces.core.cache.blobStore.offheap;

import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.memory_pool.OffHeapMemoryPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class OffHeapMemoryPoolTest {

    private OffHeapMemoryPool offHeapMemoryPool;

    public OffHeapMemoryPoolTest() {
        this.offHeapMemoryPool = new OffHeapMemoryPool(1000);
    }

    //Write + Read
    @Test
    public void writeAndReadFromOffHeapTest() {
        System.out.println("Asserting writing and reading buffers from off-heap");
        testWriteAndReadShortSentance();
        testWriteEmptyStringThrowsException();
        testWriteToAlreadyAllocatedAddressThrowsException();
    }

    //Update
    @Test
    public void updateTest() {
        testBasicUpdate();
        testExceptionOnUpdateOnUnAllocated();
        testUpdateOnShorterBuffer();
    }

    //Delete
    @Test
    public void deleteTest() {
        testBasicDelete();
    }

    private void testWriteAndReadShortSentance() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        offHeapMemoryPool.write(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);
    }

    private void testWriteEmptyStringThrowsException() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "".getBytes();

        try {
            offHeapMemoryPool.write(infoMock, buffer);
            Assert.fail("allocateAndWrite should have thrown exception but didn't");
        } catch (RuntimeException e) {
            Assert.assertTrue("allocateAndWrite should have thrown exception with another msg\n",
                    e.getMessage().contains("Illegal buffer length"));
        }
    }

    private void testWriteToAlreadyAllocatedAddressThrowsException() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        infoMock.setOffHeapAddress(123456); // arbitrary address
        byte[] buffer = "Test Buffer".getBytes();

        try {
            offHeapMemoryPool.write(infoMock, buffer);
            Assert.fail("allocateAndWrite should have thrown exception but didn't");
        } catch (IllegalStateException e) {
            Assert.assertTrue("allocateAndWrite should have throun exception with another msg\n",
                    e.getMessage().equals("trying to allocateAndWrite when already allocated in off heap"));
        }
    }

    private void assertBufferWrittenToOffheap(BlobStoreOffHeapInfoMock infoMock, byte[] buffer) {
        byte[] readedFromHeap = offHeapMemoryPool.get(infoMock);
        Assert.assertTrue("object read from offheap different then the written one\n", Arrays.equals(readedFromHeap, buffer));
    }

    private void testBasicUpdate() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        offHeapMemoryPool.write(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);
        buffer = "another text".getBytes();
        offHeapMemoryPool.update(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);
    }

    private void testExceptionOnUpdateOnUnAllocated() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "another text".getBytes();

        try {
            offHeapMemoryPool.update(infoMock, buffer);
            Assert.fail("update should have thrown exception but didn't");
        } catch (IllegalStateException e) {
            Assert.assertEquals("allocateAndWrite should have thrown exception with another msg\n", e.getMessage(),
                    "trying to update when no off heap memory is allocated");
        }

    }

    private void testUpdateOnShorterBuffer() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        offHeapMemoryPool.write(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);
        buffer = "short".getBytes();
        offHeapMemoryPool.update(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);
    }

    private void testBasicDelete() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        offHeapMemoryPool.write(infoMock, buffer);
        assertBufferWrittenToOffheap(infoMock, buffer);

        offHeapMemoryPool.delete(infoMock);
        Assert.assertEquals("Infomock address not changed to UNALLOCATED_OFFHEAP_MEMORY after delete\n", infoMock.getOffHeapAddress(), BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY);
    }

}
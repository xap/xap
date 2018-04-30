package com.j_spaces.core.cache.blobStore.offheap;

import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
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
        assertWriteAndReadShortSentance();
        assertWriteAndReadEmptyString();
        assertWriteAndReadToAllocatedAddress();
    }

    private void assertWriteAndReadShortSentance() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        assertWriteAndReadBuffer(infoMock, buffer);
    }

    private void assertWriteAndReadEmptyString() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "".getBytes();
        boolean exceptionThrowned = false;

        try {
            offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        } catch (RuntimeException e) {
            exceptionThrowned = e.getMessage().contains("Illegal buffer length");
        }

        Assert.assertTrue(exceptionThrowned);
    }

    private void assertWriteAndReadToAllocatedAddress() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        infoMock.setOffHeapAddress(123456); // arbitrary address
        byte[] buffer = "Test Buffer".getBytes();
        boolean exceptionThrowned = false;

        try {
            offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        } catch (IllegalStateException e) {
            exceptionThrowned = true;
        }

        Assert.assertTrue(exceptionThrowned);
    }

    private void assertWriteAndReadBuffer(BlobStoreOffHeapInfoMock infoMock, byte[] buffer) {
        offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        assertBufferWritedToOffheap(infoMock, buffer);
    }

    private void assertBufferWritedToOffheap(BlobStoreOffHeapInfoMock infoMock, byte[] buffer) {
        byte[] readedFromHeap = offHeapMemoryPool.get(infoMock);
        Assert.assertTrue("object readed from offheap different then the writed one", Arrays.equals(readedFromHeap, buffer));
    }

    //Update
    @Test
    public void updateTest() {
        assertUpdate();
        assertExceptionOnUpdateOnUnAllocated();
        assertUpdateOnShorterBuffer();
    }

    private void assertUpdate() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();
        offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        assertBufferWritedToOffheap(infoMock, buffer);
        buffer = "another text".getBytes();
        offHeapMemoryPool.update(infoMock, buffer);
        assertBufferWritedToOffheap(infoMock, buffer);
    }

    private void assertExceptionOnUpdateOnUnAllocated() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();
        buffer = "another text".getBytes();
        boolean exceptionThrown = false;

        try {
            offHeapMemoryPool.update(infoMock, buffer);
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    private void assertUpdateOnShorterBuffer() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();

        offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        assertBufferWritedToOffheap(infoMock, buffer);
        buffer = "short".getBytes();
        offHeapMemoryPool.update(infoMock, buffer);
        assertBufferWritedToOffheap(infoMock, buffer);
    }

    //Delete
    @Test
    public void deleteTest() {
        assertDelete();
        assertDeleteUnAllocated();
    }

    private void assertDeleteUnAllocated() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        boolean unAllocated, exceptionThrown = false;

        try {
            offHeapMemoryPool.delete(infoMock, false);
        } catch (Exception e) {
            exceptionThrown = true;
        }

        unAllocated = infoMock.getOffHeapAddress() == BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY;
        Assert.assertTrue(!exceptionThrown && unAllocated);
    }

    private void assertDelete() {
        BlobStoreOffHeapInfoMock infoMock = new BlobStoreOffHeapInfoMock();
        byte[] buffer = "Test Buffer".getBytes();
        offHeapMemoryPool.allocateAndWrite(infoMock, buffer, false);
        assertBufferWritedToOffheap(infoMock, buffer);
        boolean exceptionThrown = false;

        offHeapMemoryPool.delete(infoMock, false);
        try {
            offHeapMemoryPool.get(infoMock);
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }
}
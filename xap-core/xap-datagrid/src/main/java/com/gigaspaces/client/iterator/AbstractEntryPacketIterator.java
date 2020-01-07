package com.gigaspaces.client.iterator;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public abstract class AbstractEntryPacketIterator implements IEntryPacketIterator {
    abstract Iterator<IEntryPacket> getNextBatch() throws TimeoutException;
}

package com.gigaspaces.client.iterator;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface IEntryPacketIterator extends Iterator<IEntryPacket>{
    void close();
    Object nextEntry();
}

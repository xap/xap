package com.gigaspaces.lrmi.classloading;

import com.gigaspaces.lrmi.nio.*;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;

import java.io.IOException;
import java.rmi.RemoteException;
/**
 * A simple, non LRMI, implementation of IClassProvider.
 * Class definitions are loaded by a simple request/response to the remote ClassProvider
 *
 * @author alon shoham
 * @since 15.0
 */
public class SimpleClassProvider implements IClassProvider{
    private final ChannelEntry _channel;

    public SimpleClassProvider(ChannelEntry channelEntry) {
        this._channel = channelEntry;
    }

    @Override
    public byte[] getClassDefinition(long id, String className) throws ClassNotFoundException {
        try {
            _channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, className, ClassDefinitionRequest.FileType.CLASS), null));
            RequestPacket response = _channel._reader.readRequest(true);
            return ((ClassDefinitionResponse) response.getRequestObject()).getClassBytes();
        } catch (Exception e) {
            throw new ClassNotFoundException("Failed to retrieve remote class definition of " + className + " from the specified class loader [" + id + "]", e);
        }
    }

    public byte[] getResource(long id, String resourceName) throws ClassNotFoundException {
        try {
            _channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, resourceName, ClassDefinitionRequest.FileType.RESOURCE), null));
            RequestPacket response = _channel._reader.readRequest(true);
            return ((ClassDefinitionResponse) response.getRequestObject()).getClassBytes();
        } catch (Exception e) {
            throw new ClassNotFoundException("Failed to retrieve remote resource definition of " + resourceName + " from the specified class loader [" + id + "]", e);
        }
    }

    @Override
    public long putClassLoader(ClassLoader classLoader) throws RemoteException {
        return 0;
    }
}

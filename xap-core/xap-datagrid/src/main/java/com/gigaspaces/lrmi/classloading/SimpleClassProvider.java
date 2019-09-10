package com.gigaspaces.lrmi.classloading;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.gigaspaces.lrmi.nio.*;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple, non LRMI, implementation of IClassProvider.
 * Class definitions are loaded by a simple request/response to the remote ClassProvider
 *
 * @author alon shoham
 * @since 15.0
 */
public class SimpleClassProvider implements IClassProvider{
    private final static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);
    private final ChannelEntry _channel;

    public SimpleClassProvider(ChannelEntry channelEntry) {
        this._channel = channelEntry;
    }

    @Override
    public byte[] getClassDefinition(long id, String className) throws ClassNotFoundException {
        try {
            _channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, className, ClassDefinitionRequest.FileType.CLASS), null));
            RequestPacket response = _channel._reader.readRequest(true);
            ClassDefinitionResponse classDefinitionResponse = (ClassDefinitionResponse) response.getRequestObject();
            if(classDefinitionResponse.getException() != null){
                throw new ClassNotFoundException("failed to get class definition remotely", classDefinitionResponse.getException());
            }
            return classDefinitionResponse.getClassBytes();
        } catch (Exception e) {
            throw new ClassNotFoundException("Failed to retrieve remote class definition of " + className + " from the specified class loader [" + id + "]", e);
        }
    }

    public byte[] getResource(long id, String resourceName) throws ClassNotFoundException {
        try {
            _channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, resourceName, ClassDefinitionRequest.FileType.RESOURCE), null));
            RequestPacket response = _channel._reader.readRequest(true);
            ClassDefinitionResponse classDefinitionResponse = (ClassDefinitionResponse) response.getRequestObject();
            if(classDefinitionResponse.getException() != null){
                throw new ClassNotFoundException("failed to get class definition remotely", classDefinitionResponse.getException());
            }
            return classDefinitionResponse.getClassBytes();
        } catch (Exception e) {
            throw new ClassNotFoundException("Failed to retrieve remote resource definition of " + resourceName + " from the specified class loader [" + id + "]", e);
        }
    }

    @Override
    public long putClassLoader(ClassLoader classLoader) throws RemoteException {
        return 0;
    }
}

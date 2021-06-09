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
package com.gigaspaces.lrmi.classloading;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.gigaspaces.lrmi.nio.*;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.rmi.RemoteException;

/**
 * A simple, non LRMI, implementation of IClassProvider.
 * Class definitions are loaded by a simple request/response to the remote ClassProvider
 *
 * @author alon shoham
 * @since 15.0
 */
public class SimpleClassProvider implements IClassProvider{
    private final static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);
    private final WeakReference<ChannelEntry> _channel;

    public SimpleClassProvider(ChannelEntry channelEntry) {
        this._channel = new WeakReference<>(channelEntry);
    }

    @Override
    public byte[] getClassDefinition(long id, String className) throws ClassNotFoundException {
        try {
            byte[] result = LocalClassProvider.getClassDefinitionLocally(id, className, getClass());
            if (result != null) {
                return result;
            }
        }  catch(ClassNotFoundException e){
            //do nothing here
        }
        if (_logger.isDebugEnabled())
            _logger.debug("SimpleRemoteClassProvider failed to find class [" + className + "] definition locally from class loader id " + id + ". Trying remotely");
        ChannelEntry channel = _channel.get();
        if(channel == null){
            throw new ClassNotFoundException("Failed to retrieve remote class definition of " + className + " from the specified class loader [" + id + "], channel is closed");
        }
        try{
            //Method is called from space side. Space sends a class definition request wrapped in a ReplyPacket
            channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, className, FileType.CLASS), null));
            //The class definition response is returned wrapped in a RequestPacket
            RequestPacket response = channel._reader.readRequest(true);
            ClassDefinitionResponse classDefinitionResponse = (ClassDefinitionResponse) response.getRequestObject();
            if(classDefinitionResponse.getException() != null){
                _channel.clear();
                throw new ClassNotFoundException("Failed to retrieve remote class definition of " + className + " from the specified class loader [" + id + "]", classDefinitionResponse.getException());
            }
            return classDefinitionResponse.getClassBytes();
        } catch (Exception e) {
            _channel.clear();
            throw new ClassNotFoundException("Failed to retrieve remote class definition of " + className + " from the specified class loader [" + id + "]", e);
        }
    }

    public byte[] getResource(long id, String resourceName) throws ClassNotFoundException {
        try {
            byte[] result = LocalClassProvider.getResourceLocally(id, resourceName, getClass());
            if (result != null) {
                return result;
            }
        }  catch(ClassNotFoundException e){
            //do nothing here
        }
        if (_logger.isDebugEnabled())
            _logger.debug("SimpleRemoteClassProvider failed to find resource [" + resourceName + "] locally from class loader id " + id + ". Trying remotely");
        ChannelEntry channel = _channel.get();
        if(channel == null){
            throw new ClassNotFoundException("Failed to retrieve remote resource definition of " + resourceName + " from the specified class loader [" + id + "], because channel is closed");
        }
        try{
            //Method is called from space side. Space sends a resource definition request wrapped in a ReplyPacket
            channel._writer.writeReply(new ReplyPacket(new ClassDefinitionRequest(id, resourceName, FileType.RESOURCE), null));
            //The resource definition response is returned wrapped in a RequestPacket
            RequestPacket response = channel._reader.readRequest(true);
            ClassDefinitionResponse classDefinitionResponse = (ClassDefinitionResponse) response.getRequestObject();
            if(classDefinitionResponse.getException() != null){
                _channel.clear();
                throw new ClassNotFoundException("Failed to retrieve remote resource definition of " + resourceName + " from the specified class loader [" + id + "]", classDefinitionResponse.getException());
            }
            return classDefinitionResponse.getClassBytes();
        } catch (Exception e) {
            _channel.clear();
            throw new ClassNotFoundException("Failed to retrieve remote resource definition of " + resourceName + " from the specified class loader [" + id + "]", e);
        }
    }

    @Override
    public long putClassLoader(ClassLoader classLoader) throws RemoteException {
        return LocalClassProvider.putClassLoader(classLoader);
    }
}

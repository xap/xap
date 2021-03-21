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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.internal.io.AnnotatedObjectInputStream;
import com.gigaspaces.internal.io.AnnotatedObjectOutputStream;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.lrmi.classloading.LRMIRemoteClassLoaderIdentifier;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;

import java.io.IOException;
import java.rmi.UnmarshalException;

/**
 * A Reply Packet is constructed by the NIO server and sent back to the client peer.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class ReplyPacket<T> implements IPacket {
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE + 1;

    private T result;
    private Exception exception;   // if not null - an exception occurred

    public ReplyPacket() {
    }

    public ReplyPacket(T result, Exception exception) {
        this.result = result;
        this.exception = exception;
    }


    /**
     * @return the result
     */
    public T getResult() {
        return result;
    }

    /**
     * @param exception the exception to set
     */
    public void setException(Exception exception) {
        this.exception = exception;
    }

    /**
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    public void clear() {
        result = null;
        exception = null;
    }

    /*
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(AnnotatedObjectInputStream in) throws IOException, ClassNotFoundException {
        if (in.readByte() != SERIAL_VERSION)
            throw new UnmarshalException("Requested version does not match local version. Please make sure you are using the same version on both ends.");

        LRMIRemoteClassLoaderIdentifier remoteClassLoaderId = RemoteClassLoaderContext.get();
        LRMIRemoteClassLoaderIdentifier previousIdentifier = null;
        if (remoteClassLoaderId != null)
            previousIdentifier = LRMIConnection.setRemoteClassLoaderIdentifier(remoteClassLoaderId);

        result = IOUtils.readObject(in);
        exception = IOUtils.readObject(in);
        // NIV_WARN: backwards change
        //result = (T) in.readUnshared();
        //exception = (Exception) in.readUnshared();

        if (remoteClassLoaderId != null)
            LRMIConnection.setRemoteClassLoaderIdentifier(previousIdentifier);
    }

    /*
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
    public void writeExternal(AnnotatedObjectOutputStream out) throws IOException {
        //Writes serial version
        out.writeByte(SERIAL_VERSION);
        IOUtils.writeObject(out, result);
        IOUtils.writeObject(out, exception);
        // NIV_WARN: backwards change
        //out.writeUnshared(result);
        //out.writeUnshared(exception);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("[ReplyPacket: ");
        if (result != null) {
            builder.append("result = ").append(result);
        } else if (exception != null) {
            builder.append("exception = ").append(exception);
        }

        builder.append("]");

        return builder.toString();
    }


}
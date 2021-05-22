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

import com.gigaspaces.internal.io.*;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.LRMIMethod;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.ObjectRegistry;
import com.gigaspaces.lrmi.OperationPriority;
import com.gigaspaces.lrmi.classloading.LRMIRemoteClassLoaderIdentifier;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.InvalidClassException;
import java.rmi.UnmarshalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Request Packet is constructed by the NIO client peer and sent to the server.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class RequestPacket implements IPacket {
    @SuppressWarnings("unused")
    // DO NOT CHANGE. use SERIAL_VERSION instead.
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE + 2;

    private final static Logger _contextLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_CONTEXT);

    private long lrmiId;
    private long objectId;
    private long remoteClassLoaderId;
    private int methodOrderId;
    private Object[] args;

    transient private LRMIMethod invokeMethod;
    transient private LRMIRemoteClassLoaderIdentifier previousIdentifier;
    transient private boolean shouldRestore;

    public LRMIMethod getInvokeMethod() {
        return invokeMethod;
    }

    private Object _requestObj;

    /**
     * Exception during deserialization of RequestPacket of during method invocation
     */
    public Exception exception;

    /**
     * if <code>true</code> don't wait for reply
     */
    private transient boolean isOneWay;
    public transient boolean isCallBack;
    public transient OperationPriority operationPriority;
    private transient PlatformLogicalVersion targetVersion;

    public RequestPacket() {
        operationPriority = OperationPriority.REGULAR;
    }

    public RequestPacket(Object requestObj) {
        this._requestObj = requestObj;
        operationPriority = OperationPriority.REGULAR;
    }

    public RequestPacket(long objectId, int methodOrderId, Object[] args, boolean isOneWay,
                         boolean isCallBack, LRMIMethod invokeMethod, long contextClassLoaderId, OperationPriority priority, PlatformLogicalVersion targetVersion) {
        set(objectId, methodOrderId, args, isOneWay, isCallBack, invokeMethod, contextClassLoaderId, priority, targetVersion);
    }

    final public void set(long objectId, int methodOrderId, Object[] args, boolean isOneWay,
                          boolean isCallBack, LRMIMethod lrmiMethod, long contextClassLoaderId, OperationPriority priority, PlatformLogicalVersion targetVersion) {
        this.lrmiId = LRMIRuntime.getRuntime().getID();
        this.objectId = objectId;
        this.methodOrderId = methodOrderId;
        this.args = args;
        this.isOneWay = isOneWay;
        this.isCallBack = isCallBack;
        this.invokeMethod = lrmiMethod;
        this.remoteClassLoaderId = contextClassLoaderId;
        this.operationPriority = priority;
        this.targetVersion = targetVersion;
    }

    public Object getRequestObject() {
        return _requestObj;
    }

    public static OperationPriority decodePriority(byte flags) {
        // Test DIRECT first, because it's a combination of liveness and monitoring
        if ((flags & BitMap.IS_DIRECT_PRIORITY) >= BitMap.IS_DIRECT_PRIORITY)
            return OperationPriority.DIRECT;
        if ((flags & BitMap.IS_LIVENESS_PRIORITY) != 0)
            return OperationPriority.LIVENESS;
        if ((flags & BitMap.IS_MONITORING_PRIORITY) != 0)
            return OperationPriority.MONITORING;
        if ((flags & BitMap.IS_CUSTOM_PRIORITY) != 0)
            return OperationPriority.CUSTOM;

        return OperationPriority.REGULAR;
    }

    public static byte encodePriority(OperationPriority priority, byte flags) {
        switch (priority) {
            case LIVENESS:
                flags |= BitMap.IS_LIVENESS_PRIORITY;
                break;
            case MONITORING:
                flags |= BitMap.IS_MONITORING_PRIORITY;
                break;
            case CUSTOM:
                flags = (byte) (flags | BitMap.IS_CUSTOM_PRIORITY);
                break;
            case DIRECT:
                flags |= BitMap.IS_DIRECT_PRIORITY;
                break;
            case REGULAR:
                break;
        }
        return flags;
    }

    /**
     * @param in request the input stream, MarshalInputStream is passed to allow changing the
     *           default classloader.
     */
    public void readExternal(MarshalInputStream in) throws IOException,
            ClassNotFoundException {
        byte version = in.readByte();
        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + version + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same version on both ends, service version is " + PlatformVersion.getOfficialVersion());

        final byte flags = in.readByte();

        if ((flags & BitMap.REQUEST_OBJECT) != 0) // UID not a null
        {
            _requestObj = in.readObject();
        } else {
            //TODO OPT: we can derive lrmiId and objectId from the channel this invocation came from
            //instead of serializing it over the wire every time (save 16 bytes)
            /* read classStubId to unmarshal methods values */
            lrmiId = in.readLong();

            objectId = in.readLong();
            methodOrderId = in.readInt();
            remoteClassLoaderId = in.readLong();
            operationPriority = decodePriority(flags);
            if (operationPriority == OperationPriority.CUSTOM) {
                //Place holder for having named dedicated thread pools
                IOUtils.readRepetitiveString(in);
            }

            //TODO OPT: have some cache instead of creating this identifier every time.
            previousIdentifier = LRMIConnection.setRemoteClassLoaderIdentifier(new LRMIRemoteClassLoaderIdentifier(lrmiId, remoteClassLoaderId));
            shouldRestore = true;
            isOneWay = (flags & BitMap.IS_ONEWAY) != 0;
            isCallBack = (flags & BitMap.IS_CALLBACK) != 0;

            if (objectId != -1) {
                LRMIRuntime lrmiRuntime = LRMIRuntime.getRuntime();

                ObjectRegistry.Entry entry = lrmiRuntime.getRegistryObject(objectId);
                if (entry == null)
                    throw new java.rmi.NoSuchObjectException("Object " + objectId + " not found in object registry");
                if (entry.getExportedThreadClassLoader() == null)
                    throw new java.rmi.NoSuchObjectException(entry + " has no ExportedThreadClassLoader");
                
    			/* get reflected methods by specific remote stub classId */
                LRMIMethod[] methodList = entry.getMethodList();

    			/* protect unexpected behavior when client has not the same stub class version with the server */
                if (methodOrderId >= methodList.length) {
                    String invClaz = methodList[0].realMethod.getDeclaringClass().getName();
                    throw new InvalidClassException(invClaz + "; local class incompatible. Check that you run the same class version. " +
                            "[objectId: " + objectId + ", methodId: " + methodOrderId + "]");
                }

    			/* retrieve the invocation remote method by methodOrderId */
                invokeMethod = methodList[methodOrderId];

                //Update context for debug logging purpose

                boolean logContext = _contextLogger.isDebugEnabled();
                if (logContext) {
                    LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
                    LRMIInvocationTrace trace = currentContext.getTrace();
                    if (trace != null) {
                        trace = trace.setMethod(invokeMethod.realMethodString);
                        currentContext.setTrace(trace);
                    }
                }


                // in case of oneway call using the socket to callback the client is forbidden!
                if (isOneWay)
                    LRMIConnection.clearConnection();

                /* unmarsh method values with ClassLoader of target invocation object */
                ClassLoader unmarshallClassLoader = entry.getExportedThreadClassLoader();

                if (invokeMethod.getNumOfArguments() != 0) {
                    ClassLoader orgThreadCL = Thread.currentThread().getContextClassLoader();
                    final boolean changeCL = orgThreadCL != unmarshallClassLoader;
                    if (changeCL)
                        ClassLoaderHelper.setContextClassLoader(unmarshallClassLoader, true /*ignore security*/);

                    try {
                        args = invokeMethod.readRequest(in);
                        //Update context for debug logging purpose
                        if (logContext) {
                            LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
                            LRMIInvocationTrace trace = currentContext.getTrace();
                            if (trace != null) {
                                trace = trace.setArgs(args);
                                currentContext.setTrace(trace);
                            }
                        }
                    } finally {
                        if (changeCL)
                            ClassLoaderHelper.setContextClassLoader(orgThreadCL, true /*ignore security*/);
                    }
                }
            }
        }
    }

    /*
      * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
      */
    public void writeExternal(MarshalOutputStream out) throws IOException {
        out.writeByte(SERIAL_VERSION);
        out.writeByte(buildFlags());
        if (_requestObj != null) {
            out.writeObject(_requestObj);
        } else {
            /** write stubClassId, so that the ServerPeer will marsh/unmarsh the method arguments more faster */
            out.writeLong(lrmiId);
            out.writeLong(objectId);
            out.writeInt(methodOrderId);
            out.writeLong(remoteClassLoaderId);

            if (operationPriority == OperationPriority.CUSTOM && targetVersion.greaterOrEquals(PlatformLogicalVersion.v9_7_0)) {
                //Place holder for dedicated thread pools
                IOUtils.writeRepetitiveString(out, "");
            }

            invokeMethod.writeRequest(out, args);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("[RequestPacket: ");
        if (_requestObj != null) {
            builder.append("RequestObj = ").append(_requestObj);
        }

        if (invokeMethod != null && invokeMethod.realMethod != null) {
            builder.append(invokeMethod.realMethod.getDeclaringClass()).append('.');
            builder.append(invokeMethod.realMethod.getName()).append('(');
            Class<?>[] types = invokeMethod.methodTypes;
            for (int i = 0; i < types.length; i++) {
                builder.append(types[i] == null ? "null type" : types[i].getCanonicalName());
                builder.append(' ');
                builder.append(args == null ? "null args" : args[i]);
                if (i < types.length - 1) {
                    builder.append(", ");
                }
            }
            builder.append(')');
        }

        builder.append(", isOneWay = ").append(isOneWay);
        builder.append(", isCallBack = ").append(isCallBack);
        builder.append(", Priority = ").append(operationPriority);
        builder.append(']');
        return builder.toString();
    }

    /**
     * Bit map for serialization
     */
    private interface BitMap {
        byte REQUEST_OBJECT = 1;
        byte IS_ONEWAY = 1 << 1;
        byte IS_CALLBACK = 1 << 2;
        byte IS_LIVENESS_PRIORITY = 1 << 3;
        byte IS_MONITORING_PRIORITY = 1 << 4;
        byte IS_DIRECT_PRIORITY = IS_LIVENESS_PRIORITY | IS_MONITORING_PRIORITY;
        byte IS_CUSTOM_PRIORITY = 1 << 5;
    }

    private byte buildFlags() {
        byte flags = 0;
        if (_requestObj != null) {
            flags |= BitMap.REQUEST_OBJECT;
        }
        if (isOneWay) {
            flags |= BitMap.IS_ONEWAY;
        }
        if (isCallBack) {
            flags |= BitMap.IS_CALLBACK;
        }
        return encodePriority(operationPriority, flags);
    }

    /**
     * Clear the RequestPacket internal state.
     */
    public void clear() {
        args = null;
    }

    public Object[] getArgs() {
        return args;
    }

    public long getObjectId() {
        return objectId;
    }

    public boolean isOneWay() {
        return isOneWay;
    }

    public void restorePreviousLRMIRemoteClassLoaderState() {
        if (shouldRestore)
            LRMIConnection.setRemoteClassLoaderIdentifier(previousIdentifier);
    }


    /**
     * Assumes internal knowledge of how the LRMI incoming invocation bytes should appear
     *
     * @param bytes the bytes
     * @return operation priority
     * @since 9.0
     */
    public static OperationPriority getOperationPriorityFromBytes(byte[] bytes) {
        if (bytes.length < 4)
            throw new IllegalStateException("Incoming invocation request is not of known format, byte array length is too small - " + bytes.length);
        return decodePriority(bytes[3]);
    }
}

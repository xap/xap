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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.lrmi.LRMIProxyMonitoringDetailsImpl;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ProxyInvocationHandler;
import com.gigaspaces.internal.stubcache.MissingCachedStubException;
import com.gigaspaces.internal.stubcache.StubId;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext.InvocationStage;
import com.gigaspaces.lrmi.LRMIInvocationContext.ProxyWriteType;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link ILRMIInvocationHandler} as a {@link InvocationHandler} with an internam {@link
 * RemoteMethodCache} for fast transformation between regular method to LRMI method.
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class MethodCachedInvocationHandler
        implements ProxyInvocationHandler {
    final private static Logger _contextLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_CONTEXT);
    final static boolean DISABLE_STUB_CACHE = Boolean.getBoolean("com.gs.transport_protocol.lrmi.disable-stub-cache");

    private final RemoteMethodCache _cache;
    private final ClientPeerInvocationHandler _invocationHandler;
    private final String _serverPlatformVersion;
    private final PlatformLogicalVersion _serverPlatformLogicalVersion;
    private final String _connectionURL;
    private int _referencesCount = 1;

    public MethodCachedInvocationHandler(RemoteMethodCache cache, ClientPeerInvocationHandler invocationHandler, String serverPlatformVersion, PlatformLogicalVersion serverPlatformLogicalVersion, String connectionURL) {
        _cache = cache;
        _invocationHandler = invocationHandler;
        _serverPlatformVersion = serverPlatformVersion;
        _serverPlatformLogicalVersion = serverPlatformLogicalVersion;
        _connectionURL = connectionURL;
    }

    public Object invoke(Object proxy, IMethod method, Object[] args)
            throws Throwable {
        LRMIMethod lrmiMethod = _cache.getLRMIMethod(method);

        if (!lrmiMethod.isSupported())
            throw new ServiceUnsupportedOperationException("current method [" + method.toString() + "] is not supported by the remote stub," +
                    " possibly due to older version of the stub. local version [OfficialVersion=" + PlatformVersion.getOfficialVersion() + " " + PlatformLogicalVersion.getLogicalVersion() + "] remote stub version [OfficialVersion=" + _serverPlatformVersion + " " + _serverPlatformLogicalVersion + "] connectionURL=" + _connectionURL, PlatformVersion.getOfficialVersion(), _serverPlatformVersion, PlatformLogicalVersion.getLogicalVersion(), _serverPlatformLogicalVersion);

        try {
            setLRMIInvocationContext(method, args, lrmiMethod);

            try {
                return _invocationHandler.invoke(proxy, lrmiMethod, args);
            } catch (MissingCachedStubException ex) {
                //On missing cache switch to ProxyWriteType.CACHED_FULL mode, no new snapshot is needed
                LRMIInvocationContext.updateContext(null, ProxyWriteType.CACHED_FULL, InvocationStage.CLIENT_SEND_REQUEST, null, null, false, null, null);

                return _invocationHandler.invoke(proxy, lrmiMethod, args);
            }
        } catch (IOException ex) {
            // check if the exception cause is that the thread was interrupted
            // and clear interrupted state
            if (Thread.interrupted()) {
                String exMessage = "Thread was interrupted while waiting on the network";
                InterruptedException ie = new InterruptedException(exMessage);
                ie.initCause(ex);
                //check if the actual exception can be thrown
                if (throwsException(lrmiMethod.realMethod, InterruptedException.class))
                    throw ie;
                //throw runtime exception, so it is propagated to the user
                throw new InterruptedSpaceException(exMessage, ie);
            }

            throw ex;
        } finally {
            //Restore previous context when done
            LRMIInvocationContext.restoreContext();
        }
    }

    private void setLRMIInvocationContext(IMethod method, Object[] args,
                                          LRMIMethod lrmiMethod) {
        LRMIInvocationTrace trace = _contextLogger.isDebugEnabled() ? new LRMIInvocationTrace(LRMIUtilities.getMethodDisplayString(method), args, null, true) : null;
        ProxyWriteType proxyWriteType = DISABLE_STUB_CACHE ? ProxyWriteType.UNCACHED : ProxyWriteType.CACHED_LIGHT;
        //We must create a snapshot and update a new context because we do not control the thread that called this method
        //which might contain other invocation context in its stack (For instance incoming destructive operation which is being replicated to
        //a different space using the same thread).
        Boolean useStubCache = lrmiMethod.useStubCache ? Boolean.TRUE : Boolean.FALSE;
        LRMIInvocationContext.updateContext(trace, proxyWriteType, InvocationStage.CLIENT_SEND_REQUEST, null, _serverPlatformLogicalVersion, true, useStubCache, null);
    }

    /**
     * Checks if the given method throws the given exception
     *
     * @return true if the method throws the exception
     */
    private static boolean throwsException(IMethod method, Class<?> exceptionClass) {
        Class<?>[] exceptionClasses = method.getExceptionTypes();

        for (Class<?> c : exceptionClasses) {
            if (exceptionClass.isAssignableFrom(c)) {
                return true;

            }
        }
        return false;
    }

    //This should never be called
    public StubId getStubId() {
        throw new UnsupportedOperationException();
    }

    public ClientPeerInvocationHandler getInvocationHandler() {
        return _invocationHandler;
    }

    public void disable() {
        _invocationHandler.disable();
    }

    public void enable() {
        _invocationHandler.enable();
    }

    //Assumes this is always called under external lock that protects concurrent access to remove and to add 
    public boolean decrementReference() {
        boolean lastReferent = --_referencesCount == 0;
        if (lastReferent)
            _invocationHandler.close();
        return lastReferent;
    }

    //Assumes this is always called under external lock 
    public void incrementReference() {
        _referencesCount++;
    }

    public LRMIProxyMonitoringDetailsImpl getMonitoringDetails() {
        return _invocationHandler.getMonitoringDetails();
    }

    public String getConnectionURL() {
        return _connectionURL;
    }
}

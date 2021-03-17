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

import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.net.InetSocketAddress;

/**
 * Represents thread local lrmi invocation context that is used for debug logging levels
 *
 * @author eitany
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIInvocationContext {
    private static class LRMIInvocationContextHolder {
        private LRMIInvocationContext context;

        public void setContext(LRMIInvocationContext context) {
            this.context = context;
        }

        public LRMIInvocationContext getContext() {
            return context;
        }
    }

    public enum ProxyWriteType {
        UNCACHED,
        CACHED_FULL,
        CACHED_LIGHT
    }

    /**
     * Represents the stage of the lrmi invocation
     *
     * @author eitany
     * @since 7.5
     */
    public enum InvocationStage {
        CLIENT_SEND_REQUEST,
        CLIENT_RECEIVE_REPLY,
        INVOCATION_HANDLING,
        SERVER_MARSHAL_REPLY,
        SERVER_UNMARSHAL_REQUEST
    }

    private final static ThreadLocal<LRMIInvocationContextHolder> invocationContexts = new ThreadLocal<LRMIInvocationContextHolder>() {
        protected LRMIInvocationContextHolder initialValue() {
            LRMIInvocationContextHolder result = new LRMIInvocationContextHolder();
            //Keep two context hierarchy per thread, if heirarchy gets larger, a new context will be generated on the fly
            LRMIInvocationContext context = new LRMIInvocationContext();
            context._nextContext = new LRMIInvocationContext();
            result.setContext(context);
            return result;
        }
    };

    private LRMIInvocationTrace _trace;
    private PlatformLogicalVersion _sourceLogicalVersion;
    private PlatformLogicalVersion _targetLogicalVersion;
    private ProxyWriteType _proxyWriteType = ProxyWriteType.UNCACHED;
    private InvocationStage _invocationStage;
    private LRMIInvocationContext _nextContext;
    private LRMIInvocationContext _previousContext;
    private boolean _useStubCache;
    private boolean _callbackMethod;
    private boolean _livenessPriorityEnabled;
    private boolean _customPriorityEnabled;
    private boolean _directPriorityEnabled;

    private boolean _callbackMethodInNextInvocation;
    private boolean _livenessPriorityEnabledInNextInvocation;
    private boolean _customPriorityEnabledInNextInvocation;
    private boolean _directPriorityEnabledInNextInvocation;
    private InetSocketAddress _clientEndPointAddress;

    public void setTrace(LRMIInvocationTrace trace) {
        _trace = trace;
    }

    public LRMIInvocationTrace getTrace() {
        return _trace;
    }

    private void setSourceLogicalVersion(PlatformLogicalVersion sourceLogicalVersion) {
        this._sourceLogicalVersion = sourceLogicalVersion;
    }

    public PlatformLogicalVersion getSourceLogicalVersion() {
        return _sourceLogicalVersion;
    }

    private void setTargetLogicalVersion(PlatformLogicalVersion targetLogicalVersion) {
        this._targetLogicalVersion = targetLogicalVersion;
    }

    public PlatformLogicalVersion getTargetLogicalVersion() {
        return _targetLogicalVersion;
    }

    private void setProxyWriteType(ProxyWriteType proxyWriteType) {
        this._proxyWriteType = proxyWriteType;
    }

    public ProxyWriteType getProxyWriteType() {
        return _proxyWriteType;
    }

    private void setInvocationStage(InvocationStage stage) {
        _invocationStage = stage;
    }

    public InvocationStage getInvocationStage() {
        return _invocationStage;
    }

    public boolean isEmpty() {
        return _invocationStage == null;
    }

    public void setClientEndPointAddress(InetSocketAddress clientEndPointAddress) {
        _clientEndPointAddress = clientEndPointAddress;
    }

    public InetSocketAddress getClientEndPointAddress() {
        return _clientEndPointAddress;
    }

    /**
     * @return invocation stage related end point logical version, if this is the invocation server
     * then the source logical version is returned, if it is the client then the target logical
     * version is returned. if there is no lrmi invocation context then the current jar logical
     * version is returned.
     */
    public static PlatformLogicalVersion getEndpointLogicalVersion() {
        LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
        if (currentContext == null || currentContext.isEmpty())
            return PlatformLogicalVersion.getLogicalVersion();
        InvocationStage invocationStage = currentContext._invocationStage;
        PlatformLogicalVersion result;
        switch (invocationStage) {
            case CLIENT_RECEIVE_REPLY:
            case CLIENT_SEND_REQUEST:
                result = currentContext._targetLogicalVersion;
                break;
            case SERVER_MARSHAL_REPLY:
            case SERVER_UNMARSHAL_REQUEST:
            case INVOCATION_HANDLING:
                result = currentContext._sourceLogicalVersion;
                break;
            default:
                throw new IllegalArgumentException("Unhandled invocation stage " + currentContext._invocationStage);
        }
        return result;
    }

    /**
     * Snapshot current state and returns a new context which can be restored to the previous
     * snapshot by calling restore
     *
     * @see LRMIInvocationContext#restore()
     */
    private LRMIInvocationContext snapshot() {
        LRMIInvocationContext newContext = _nextContext == null ? new LRMIInvocationContext() : _nextContext;
        newContext._previousContext = this;
        return newContext;
    }

    /**
     * @return the previous snapshot which created this new context
     * @see LRMIInvocationContext#snapshot()
     */
    private LRMIInvocationContext restore() {
        reset();
        _previousContext.resetNextInvocationState();
        return _previousContext;
    }

    private void resetNextInvocationState() {
        _livenessPriorityEnabledInNextInvocation = false;
        _customPriorityEnabledInNextInvocation = false;
        _directPriorityEnabledInNextInvocation = false;
        _callbackMethodInNextInvocation = false;
    }

    private void reset() {
        _sourceLogicalVersion = null;
        _targetLogicalVersion = null;
        _trace = null;
        _invocationStage = null;
        _proxyWriteType = ProxyWriteType.UNCACHED;
        _useStubCache = false;
        _callbackMethod = false;
        _livenessPriorityEnabled = false;
        _customPriorityEnabled = false;
        _directPriorityEnabled = false;
        _clientEndPointAddress = null;
        resetNextInvocationState();
    }

    public static String getContextMethodShortDisplayString() {
        LRMIInvocationContext context = getCurrentContext();
        if (context == null || context.isEmpty())
            return "no context";
        LRMIInvocationTrace trace = context.getTrace();
        if (trace == null)
            return "no trace context";
        return trace.getTraceShortDisplayString();
    }

    public static String getContextMethodLongDisplayString() {
        LRMIInvocationContext context = getCurrentContext();
        if (context == null || context.isEmpty())
            return "no context";
        LRMIInvocationTrace trace = context.getTrace();
        if (trace == null)
            return "no trace context";
        return trace.getTraceLongDisplayString();
    }

    public static LRMIInvocationContext getCurrentContext() {
        return invocationContexts.get().getContext();
    }

    /**
     * Updates this thread lrmi invocation context
     *
     * @param invocationTrace       trace
     * @param proxyWriteType        write type
     * @param stage                 invocation stage
     * @param sourceLogicalVersion  source logical version
     * @param targetLogicalVersion  target logical version
     * @param createSnapshot        if true, the current context is kept and a new context is
     *                              created
     * @param useStubCache          true if stub cache should be used for this invocation which can
     *                              later be restored to the current context using the {@link
     *                              LRMIInvocationContext#restoreContext()} method. if false the
     *                              current context is overriden with all the non null values
     *                              parameters.
     * @param clientEndPointAddress - The source ip/port
     */
    public static void updateContext(LRMIInvocationTrace invocationTrace, ProxyWriteType proxyWriteType, InvocationStage stage,
                                     PlatformLogicalVersion sourceLogicalVersion,
                                     PlatformLogicalVersion targetLogicalVersion, boolean createSnapshot, Boolean useStubCache, InetSocketAddress clientEndPointAddress) {


        LRMIInvocationContextHolder holder = invocationContexts.get();
        LRMIInvocationContext invocationContext = holder.getContext();
        LRMIInvocationContext actualContext = createSnapshot ? invocationContext.snapshot() : invocationContext;

        if (invocationTrace != null || createSnapshot)
            actualContext.setTrace(invocationTrace);
        if (proxyWriteType != null || createSnapshot)
            actualContext.setProxyWriteType(proxyWriteType);
        if (stage != null || createSnapshot)
            actualContext.setInvocationStage(stage);
        if (sourceLogicalVersion != null || createSnapshot)
            actualContext.setSourceLogicalVersion(sourceLogicalVersion);
        if (targetLogicalVersion != null || createSnapshot)
            actualContext.setTargetLogicalVersion(targetLogicalVersion);
        if (useStubCache != null)
            actualContext.setUseStubCache(useStubCache.booleanValue());
        if (clientEndPointAddress != null || createSnapshot) {
            actualContext.setClientEndPointAddress(clientEndPointAddress);
        }
        if (createSnapshot) {
            if (invocationContext._livenessPriorityEnabledInNextInvocation)
                actualContext.setLivenessPriorityEnabled(true);
            if (invocationContext._customPriorityEnabledInNextInvocation)
                actualContext.setCustomPriorityEnabled(true);
            if (invocationContext._directPriorityEnabledInNextInvocation)
                actualContext.setDirectPriorityEnabled(true);
            if (invocationContext._callbackMethodInNextInvocation)
                actualContext.setCallbackMethod(true);
        }

        holder.setContext(actualContext);
    }

    /**
     * Restores the current context to its previous {@link LRMIInvocationContext#snapshot()} stage.
     */
    public static void restoreContext() {
        LRMIInvocationContextHolder contextHolder = invocationContexts.get();
        contextHolder.setContext(contextHolder.getContext().restore());
    }

    /**
     * Resets the current context
     */
    public static void resetContext() {
        invocationContexts.get().getContext().reset();
    }

    public static void enableLivenessPriorityForNextInvocation() {
        getCurrentContext()._livenessPriorityEnabledInNextInvocation = true;
    }

    public static void enableCustomPriorityForNextInvocation() {
        getCurrentContext()._customPriorityEnabledInNextInvocation = true;
    }

    public static void enableDirectPriorityForNextInvocation() {
        getCurrentContext()._directPriorityEnabledInNextInvocation = true;
    }

    public static void enableCallbackModeForNextInvocation() {
        getCurrentContext()._callbackMethodInNextInvocation = true;
    }


    public void setUseStubCache(boolean useStubCache) {
        _useStubCache = useStubCache;
    }

    public boolean isUseStubCache() {
        return _useStubCache;
    }

    private void setCallbackMethod(boolean callbackMethod) {
        _callbackMethod = callbackMethod;
    }

    public boolean isCallbackMethod() {
        return _callbackMethod;
    }

    private void setLivenessPriorityEnabled(boolean enabled) {
        _livenessPriorityEnabled = enabled;
    }

    public boolean isLivenessPriorityEnabled() {
        return _livenessPriorityEnabled;
    }

    public void setCustomPriorityEnabled(boolean customPriorityEnabled) {
        _customPriorityEnabled = customPriorityEnabled;
    }

    public boolean isCustomPriorityEnabled() {
        return _customPriorityEnabled;
    }

    public void setDirectPriorityEnabled(boolean directPriorityEnabled) {
        _directPriorityEnabled = directPriorityEnabled;
    }

    public boolean isDirectPriorityEnabled() {
        return _directPriorityEnabled;
    }

    public static InetSocketAddress getEndpointAddress() {
        LRMIInvocationContext currentContext = getCurrentContext();
        if (currentContext == null || currentContext.isEmpty())
            return null;

        return currentContext.getClientEndPointAddress();
    }

}

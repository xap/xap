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

package com.gigaspaces.internal.remoting.routing;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.logger.Constants;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class AbstractRemoteOperationRouter implements RemoteOperationRouter {
    protected final Logger _logger;

    public AbstractRemoteOperationRouter(String name) {
        this._logger = LoggerFactory.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER + '.' + name);
    }

    protected void logBeforeExecute(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, boolean oneway) {
        if (oneway)
            logBeforeExecuteOneway(proxy, request);
        else if (_logger.isTraceEnabled())
            _logger.trace("Starting execution of " + proxy.toLogMessage(request));
    }

    protected void logBeforeExecuteAsync(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isTraceEnabled())
            _logger.trace("Starting async execution of " + proxy.toLogMessage(request));
    }

    private void logBeforeExecuteOneway(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isTraceEnabled())
            _logger.trace("Starting oneway execution of " + proxy.toLogMessage(request));
    }


    protected void logAfterExecute(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, RemoteOperationResult result, boolean oneway) {
        if (oneway)
            logAfterExecuteOneway(proxy, request);
        else if (_logger.isTraceEnabled())
            _logger.trace("Execution result: " + result + ", request=" + proxy.toLogMessage(request));
    }

    protected void logAfterExecuteAsync(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, RemoteOperationResult result) {
        if (_logger.isTraceEnabled())
            _logger.trace("Async execution result: " + result + ", request=" + proxy.toLogMessage(request));
    }

    private void logAfterExecuteOneway(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isTraceEnabled())
            _logger.trace("Oneway execution completed, request=" + proxy.toLogMessage(request));
    }

    protected void logExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception, boolean oneway) {
        if (oneway)
            logOnewayExecutionFailure(proxy, request, exception);
        else if (_logger.isWarnEnabled())
            _logger.warn("Execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logAsyncExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isWarnEnabled())
            _logger.warn("Async execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    private void logOnewayExecutionFailure(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isWarnEnabled())
            _logger.warn("Oneway execution failed: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logInterruptedExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception, boolean oneway) {
        if (oneway)
            logInterruptedOnewayExecution(proxy, request, exception);
        else if (_logger.isDebugEnabled())
            _logger.debug("Execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logInterruptedAsyncExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isDebugEnabled())
            _logger.debug("Async execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    protected void logUnexpectedAsyncExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isWarnEnabled())
            _logger.warn("Async execution failed unexpectedly: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    private void logInterruptedOnewayExecution(RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request, Exception exception) {
        if (_logger.isDebugEnabled())
            _logger.debug("Oneway execution interrupted: " + exception + ", request=" + proxy.toLogMessage(request));
    }

    @Override
    public RemoteOperationsExecutorProxy getCachedMember() {
        throw new UnsupportedOperationException();
    }
}

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

package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.admin.quiesce.QuiesceException;
import com.gigaspaces.admin.quiesce.QuiesceState;
import com.gigaspaces.admin.quiesce.QuiesceStateChangedEvent;
import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.admin.quiesce.QuiesceTokenFactory;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.SystemProperties;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Quiesce core functionality
 *
 * @author Yechiel
 * @version 10.1
 */
@com.gigaspaces.api.InternalApi
public class QuiesceHandler {
    private static final boolean QUIESCE_DISABLED = Boolean.getBoolean(SystemProperties.DISABLE_QUIESCE_MODE);
    private final Logger _logger;
    private final SpaceImpl _spaceImpl;
    private final boolean _supported;
    private volatile Guard _guard;

    public QuiesceHandler(SpaceImpl spaceImpl, QuiesceStateChangedEvent quiesceStateChangedEvent) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_QUIESCE + '.' + spaceImpl.getNodeName());
        _supported = !QUIESCE_DISABLED && !_spaceImpl.isLocalCache();
        _guard = null;
        if (quiesceStateChangedEvent != null && quiesceStateChangedEvent.getQuiesceState() == QuiesceState.QUIESCED)
            setQuiesceMode(quiesceStateChangedEvent);
    }

    public boolean isQuiesced() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return currGuard != null;
    }

    public synchronized void setQuiesceMode(QuiesceStateChangedEvent newQuiesceInfo) {
        if (_supported) {
            QuiesceState currState = _guard != null ? QuiesceState.QUIESCED : QuiesceState.UNQUIESCED;
            if (currState != newQuiesceInfo.getQuiesceState()) {
                if (newQuiesceInfo.getQuiesceState() == QuiesceState.QUIESCED) {
                    _guard = new Guard(newQuiesceInfo.getToken(), newQuiesceInfo.getDescription(), _spaceImpl.getServiceName());
                    //throw exception on all pending op templates
                    if (_spaceImpl.getEngine() != null)
                        _spaceImpl.getEngine().getCacheManager().getTemplateExpirationManager().returnWithExceptionFromAllPendingTemplates(_guard.exception);
                } else {
                    _guard = null;
                }
                if (_logger.isLoggable(Level.INFO))
                    _logger.log(Level.INFO, "Quiesce state changed to " + newQuiesceInfo.getQuiesceState());
            }
        } else {
            if (QUIESCE_DISABLED)
                _logger.log(Level.SEVERE, "Quiesce is not supported because the '" + SystemProperties.DISABLE_QUIESCE_MODE + "' was set");
            if (_spaceImpl.isLocalCache())
                _logger.log(Level.SEVERE, "Quiesce is not supported for local-cache/local-view");
        }
    }

    //disable any non-admin op if q mode on
    public void checkAllowedOp(QuiesceToken operationToken) {
        if (_supported) {
            // Concurrency: snapshot volatile _guard into local variable
            final Guard currGuard = _guard;
            if (currGuard != null) {
                if (!currGuard.token.equals(operationToken))
                    throw currGuard.exception;
            }
        }
    }

    public boolean isSupported() {
        return _supported;
    }

    private static class EmptyToken implements QuiesceToken {
        public static final EmptyToken INSTANCE = new EmptyToken();

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }

    public QuiesceToken createSpaceNameToken() {
        return QuiesceTokenFactory.createStringToken(_spaceImpl.getName());
    }

    private static class Guard {
        private final QuiesceToken token;
        private final QuiesceException exception;

        public Guard(QuiesceToken token, String description, String memberName) {
            this.token = token != null ? token : EmptyToken.INSTANCE;
            this.exception = new QuiesceException("Operation cannot be executed on a quiesced space [" + memberName + "]" +
                    (StringUtils.hasLength(description) ? ", description: " + description : ""));
        }
    }
}

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

import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    public boolean isSuspended() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return currGuard != null && currGuard.suspendLatch != null;
    }

    //disable any non-admin op if q mode on
    public void checkAllowedOp(QuiesceToken operationToken) {
        if (_supported) {
            // Concurrency: snapshot volatile _guard into local variable
            final Guard currGuard = _guard;
            if (currGuard != null)
                currGuard.guard(operationToken);
        }
    }

    public void setQuiesceMode(QuiesceStateChangedEvent newQuiesceInfo) {
        if (newQuiesceInfo.getQuiesceState() == QuiesceState.QUIESCED)
            quiesce(newQuiesceInfo.getDescription(), newQuiesceInfo.getToken());
        else
            unquiesce();
    }

    public void quiesce(String description, QuiesceToken token) {
        if (addGuard(new Guard(description, token, false))) {
            // Cancel (throw exception) on all pending op templates
            if (_spaceImpl.getEngine() != null)
                _spaceImpl.getEngine().getCacheManager().getTemplateExpirationManager().returnWithExceptionFromAllPendingTemplates(_guard.exception);
        }
    }

    public void unquiesce() {
        removeGuard(false);
    }

    public void suspend(String description) {
        addGuard(new Guard(description, createSpaceNameToken(), true));
    }

    public void unsuspend() {
        removeGuard(true);
    }

    private synchronized boolean addGuard(Guard newGuard) {
        if (!_supported) {
            if (QUIESCE_DISABLED)
                _logger.severe("Quiesce is not supported because the '" + SystemProperties.DISABLE_QUIESCE_MODE + "' was set");
            if (_spaceImpl.isLocalCache())
                _logger.severe("Quiesce is not supported for local-cache/local-view");
            return false;
        }

        if (_guard == null || newGuard.supersedes(_guard)) {
            newGuard.innerGuard = _guard;
            _guard = newGuard;
            _logger.info("Quiesce state set to " + desc(newGuard));
            return true;
        }
        if (_guard.supersedes(newGuard) && _guard.innerGuard == null) {
            _guard.innerGuard = newGuard;
            _logger.info("Quiesce guard was added, but is currently masked because state is " + desc(_guard));
            return true;
        }
        _logger.warning("Quiesce guard was discarded due to ambiguity - current state is " + desc(_guard));
        return false;
    }

    private synchronized Guard removeGuard(boolean suspendableGuard) {
        Guard prevGuard;
        if (_guard == null) {
            prevGuard = null;
            _logger.warning("No guard to remove");
        } else if (suspendableGuard) {
            if (_guard.suspendLatch != null) {
                prevGuard = _guard;
                _guard = _guard.innerGuard;
                _logger.info("Removed " + desc(prevGuard) + ", new state is " + desc(_guard));
            } else {
                prevGuard = null;
                _logger.warning("No suspend guard to remove");
            }
        } else {
            if (_guard.suspendLatch == null) {
                prevGuard = _guard;
                _guard = null;
                _logger.info("Removed " + desc(prevGuard) + ", new state is " + desc(_guard));
            } else if (_guard.innerGuard != null) {
                prevGuard = _guard.innerGuard;
                _guard.innerGuard = null;
                _logger.info("Removed inner " + desc(prevGuard) + ", state remains " + desc(_guard));
            } else {
                prevGuard = null;
                _logger.warning("No quiesce guard to remove");
            }
        }
        if (prevGuard != null)
            prevGuard.close();
        return prevGuard;
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

    private static String desc(Guard guard) {
        if (guard == null)
            return "UNQUIESCED";
        return guard.suspendLatch == null ? "QUIESCED" : "SUSPENDED";
    }

    private class Guard implements Closeable {
        private final QuiesceToken token;
        private final CountDownLatch suspendLatch;
        private final QuiesceException exception;
        Guard innerGuard;

        Guard(String description, QuiesceToken token, boolean suspend) {
            this.token = token != null ? token : EmptyToken.INSTANCE;
            this.suspendLatch = suspend ? new CountDownLatch(1) : null;
            String errorMessage = "Operation cannot be executed - space [" + _spaceImpl.getServiceName() + "] is " +
                    (suspend ? "suspended" : "quiesced") +
                    (StringUtils.hasLength(description) ? " (" + description + ")" : "");
            this.exception = new QuiesceException(errorMessage);
        }

        @Override
        public void close() {
            if (suspendLatch != null)
                suspendLatch.countDown();
        }

        void guard(QuiesceToken operationToken) {
            if (!token.equals(operationToken)) {
                if (suspendLatch != null) {
                    if (safeAwait()) {
                        // Wait a random bit before returning to avoid storming the space.
                        safeSleep(new Random().nextInt(1000));
                        return;
                    }
                }
                throw exception;
            }
        }

        boolean supersedes(Guard otherGuard) {
            return this.suspendLatch != null && otherGuard.suspendLatch == null;
        }

        private boolean safeAwait() {
            try {
                // TODO: Timeout should be configurable.
                return suspendLatch.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return suspendLatch.getCount() == 0;
            }
        }

        private void safeSleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

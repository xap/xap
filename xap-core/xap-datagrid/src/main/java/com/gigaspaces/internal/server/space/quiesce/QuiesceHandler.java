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

import com.gigaspaces.admin.quiesce.*;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.suspend.SuspendTypeChangedInternalListener;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.server.space.suspend.SuspendInfo;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Suspend/Quiesce core functionality
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
    private volatile SuspendInfo _suspendInfo;
    private final Collection<SuspendTypeChangedInternalListener> suspendTypeChangeListeners = new ConcurrentHashSet<SuspendTypeChangedInternalListener>();

    public QuiesceHandler(SpaceImpl spaceImpl, QuiesceStateChangedEvent quiesceStateChangedEvent) {
        _spaceImpl = spaceImpl;
        _logger = LoggerFactory.getLogger(Constants.LOGGER_SUSPEND + '.' + spaceImpl.getNodeName());
        _supported = !QUIESCE_DISABLED && !_spaceImpl.isLocalCache();
        _guard = null;
        setSuspendInfo(new SuspendInfo(SuspendType.NONE));

        if (quiesceStateChangedEvent != null && quiesceStateChangedEvent.getQuiesceState() == QuiesceState.QUIESCED)
            setQuiesceMode(quiesceStateChangedEvent);
    }

    public boolean isOn() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return currGuard != null;
    }

    public SuspendInfo getSuspendInfo() {
        return _suspendInfo;
    }

    public boolean isSuspended() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return hasGuard(currGuard, Status.DISCONNECTED);
    }

    public boolean isQuiesced() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return hasGuard(currGuard, Status.QUIESCED);
    }


    //TODO - change when moving to special suspend type
    public boolean isHorizontalScale() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return hasGuard(currGuard, Status.QUIESCED) && currGuard.getMessage().contains("SCALE");
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

    public void quiesceDemote(String description) {
        Guard guard = new Guard(description, null, Status.DEMOTING);
        if (addGuard(guard)) {
            // Cancel (throw exception) on all pending op templates
            if (_spaceImpl.getEngine() != null)
                _spaceImpl.getEngine().getTemplateScanner().cancelAllNonNotifyTemplates(_guard.getException());
        }
    }

    public void quiesce(String description, QuiesceToken token) {
        if (addGuard(new Guard(description, token, Status.QUIESCED))) {
            // Cancel (throw exception) on all pending op templates
            if (_spaceImpl.getEngine() != null)
                _spaceImpl.getEngine().getTemplateScanner().cancelAllNonNotifyTemplates(_guard.getException());
        }
    }

    public void unquiesce() {
        removeGuard(Status.QUIESCED);
    }

    public void unquiesceDemote() {
        removeGuard(Status.DEMOTING);
    }

    public void suspend(String description) {
        addGuard(new Guard(description, createSpaceNameToken(), Status.DISCONNECTED));
    }

    public void unsuspend() {
        removeGuard(Status.DISCONNECTED);
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
            return "NONE";
        return guard.status.suspendType.name();
    }

    protected enum Status {
        DISCONNECTED(0, "disconnected", SuspendType.DISCONNECTED),
        DEMOTING(1, "demoting", SuspendType.DEMOTING),
        QUIESCED(2, "quiesced", SuspendType.QUIESCED);

        private int order;
        private String description;
        private SuspendType suspendType;

        Status(int order, String description, SuspendType suspendType) {
            this.order = order;
            this.description = description;
            this.suspendType = suspendType;
        }

        private boolean supersedes(Status other) {
            return this.order < other.order;
        }
    }


    protected class Guard implements Closeable {
        private final QuiesceToken token;
        private final Status status;
        private final CountDownLatch suspendLatch;
        private final String errorMessage;
        private Guard innerGuard;
        private QuiesceException quiesceException;

        Guard(String description, QuiesceToken token, Status status) {
            this.token = token != null ? token : EmptyToken.INSTANCE;
            this.status = status;
            this.suspendLatch = (status == Status.DISCONNECTED) ? new CountDownLatch(1) : null;

            this.errorMessage = "Operation cannot be executed - space [" + _spaceImpl.getServiceName() + "] is " +
                    status.description +
                    (StringUtils.hasLength(description) ? " (" + description + ")" : "");
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
                QuiesceException qe = new QuiesceException(errorMessage);
                this.quiesceException = qe;
                throw qe;
            }
        }

        public String getMessage() {
            return errorMessage;
        }

        public Exception getException() {
            return quiesceException;
        }

        boolean supersedes(Guard otherGuard) {
            return this.status.supersedes(otherGuard.status);
        }

        public void close() {
            if (suspendLatch != null)
                suspendLatch.countDown();

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


        @Override
        public String toString() {
            return "Guard{" +
                    "status=" + status +
                    ", innerGuard=" + innerGuard +
                    '}';
        }

        Status getStatus() {
            return status;
        }

        Guard getInnerGuard() {
            return innerGuard;
        }
    }

    private boolean hasGuard(Guard currentGuard, Status status) {
        if (currentGuard == null) return false;

        return currentGuard.status.equals(status) || hasGuard(currentGuard.innerGuard, status);
    }

    private Guard getGuard(Guard currentGuard, Status status) {
        if (currentGuard == null) return null;
        if (currentGuard.status.equals(status)) return currentGuard;
        return getGuard(currentGuard.innerGuard, status);
    }

    synchronized boolean addGuard(Guard newGuard) {
        if (!_supported) {
            if (QUIESCE_DISABLED)
                _logger.error("Suspend is not supported because the '" + SystemProperties.DISABLE_QUIESCE_MODE + "' was set");
            if (_spaceImpl.isLocalCache())
                _logger.error("Suspend is not supported for local-cache/local-view");
            return false;
        }

        if (hasGuard(_guard, newGuard.status)) {
            _logger.warn("Suspend guard [" + newGuard.status + "] was discarded, it already exists - current state is " + desc(_guard));
            return false;
        }

        if (!guardCanBeAdded(_guard, newGuard)) {
            _logger.warn("Suspend guard couldn't be added - current state is " + desc(_guard));
            return false;
        }

        try {
            Guard prevGuard = _guard;
            _guard = addGuardHelper(_guard, newGuard);
            if (prevGuard == _guard) {
                _logger.info("Suspend guard " + desc(newGuard) + " was added, but is currently masked because state is " + desc(_guard));
            } else {
                _logger.info("Suspend state set to " + desc(_guard));
            }

            if (_guard != null) {
                setSuspendInfo(new SuspendInfo(_guard.status.suspendType));
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean guardCanBeAdded(Guard current, Guard newGuard) {
        if (current == null) return true;
        if (current.supersedes(newGuard) || newGuard.supersedes(current)) {
            return guardCanBeAdded(current.innerGuard, newGuard);
        } else {
            return false;
        }
    }

    private Guard addGuardHelper(Guard currentGuard, Guard newGuard) throws Exception {
        Guard res;
        if (currentGuard == null) {
            res = newGuard;
        } else if (currentGuard.supersedes(newGuard)) {
            res = currentGuard;
            res.innerGuard = addGuardHelper(currentGuard.innerGuard, newGuard);
        } else if (newGuard.supersedes(currentGuard)) {
            res = newGuard;
            res.innerGuard = addGuardHelper(currentGuard.innerGuard, currentGuard);
        } else {
            throw new Exception("Guard could not be added due to ambiguity");
        }

        return res;
    }

    synchronized void removeGuard(Status status) {
        if (_guard == null) {
            _logger.warn("No guard to remove");
            return;
        }

        Guard guardToRemove = getGuard(_guard, status);
        if (guardToRemove == null) {
            _logger.warn("No " + status + " guard to remove");
            return;
        }

        guardToRemove.close();
        _guard = removeGuardHelper(_guard, status);
        _logger.info("Removed " + status + ", new state is " + desc(_guard));
        if (_guard != null) {
            setSuspendInfo(new SuspendInfo(_guard.status.suspendType));
        } else {
            setSuspendInfo(new SuspendInfo(SuspendType.NONE));
        }
    }


    //returns the new guard
    private Guard removeGuardHelper(Guard guard, Status status) {
        if (guard.status.equals(status)) {
            return guard.innerGuard;
        } else {
            guard.innerGuard = removeGuardHelper(guard.innerGuard, status);
            return guard;
        }
    }

    private void setSuspendInfo(SuspendInfo suspendInfo) {
        boolean isSuspendTypeChanged = true;

        if (_suspendInfo != null && _suspendInfo.getSuspendType().equals(suspendInfo.getSuspendType())) {
            isSuspendTypeChanged = false;
        }

        this._suspendInfo = suspendInfo;

        // Todo: check this with a test
        if (isSuspendTypeChanged) {
            _logger.info("Dispatch suspendInfo [type="+suspendInfo.getSuspendType()+"] event to " + suspendTypeChangeListeners.size() + " listeners");
            for (SuspendTypeChangedInternalListener listener : suspendTypeChangeListeners) {
                try {
                    listener.onSuspendTypeChanged(suspendInfo.getSuspendType());
                } catch (Throwable t) {
                    _logger.warn("Failed to dispatch suspendInfo [type="+suspendInfo.getSuspendType()+"] event to listener [" + listener + "]: " + t.getMessage(), t);
                }
            }
        }
    }

    public void addSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        suspendTypeChangeListeners.add(listener);
    }

    public void removeSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        suspendTypeChangeListeners.remove(listener);
    }

    Guard getGuard() {
        return _guard;
    }

}

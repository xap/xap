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
    protected volatile Guard _guard;

    public QuiesceHandler(SpaceImpl spaceImpl, QuiesceStateChangedEvent quiesceStateChangedEvent) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_QUIESCE + '.' + spaceImpl.getNodeName());
        _supported = !QUIESCE_DISABLED && !_spaceImpl.isLocalCache();
        _guard = null;
        if (quiesceStateChangedEvent != null && quiesceStateChangedEvent.getQuiesceState() == QuiesceState.QUIESCED)
            setQuiesceMode(quiesceStateChangedEvent);
    }

    public boolean isOn() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return currGuard != null;
    }

    public boolean isSuspended() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return hasGuard(currGuard, Status.SUSPENDED);
    }

    public boolean isQuiesced() {
        // Concurrency: snapshot volatile _guard into local variable
        final Guard currGuard = _guard;
        return hasGuard(currGuard, Status.QUIESCED);
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
        Guard guard = new Guard(description, null, Status.QUIESCED_DEMOTE);
        if (addGuard(guard)) {
            // Cancel (throw exception) on all pending op templates
            if (_spaceImpl.getEngine() != null)
                _spaceImpl.getEngine().getCacheManager().getTemplateExpirationManager().returnWithExceptionFromAllPendingTemplates(_guard.exception);
        }
    }

    public void quiesce(String description, QuiesceToken token) {
        if (addGuard(new Guard(description, token, Status.QUIESCED))) {
            // Cancel (throw exception) on all pending op templates
            if (_spaceImpl.getEngine() != null)
                _spaceImpl.getEngine().getCacheManager().getTemplateExpirationManager().returnWithExceptionFromAllPendingTemplates(_guard.exception);
        }
    }

    public void unquiesce() {
        removeGuard(Status.QUIESCED);
    }

    public void unquiesceDemote() {
        removeGuard(Status.QUIESCED_DEMOTE);
    }

    public void suspend(String description) {
        addGuard(new Guard(description, createSpaceNameToken(), Status.SUSPENDED));
    }

    public void unsuspend() {
        removeGuard(Status.SUSPENDED);
    }

    private synchronized boolean addGuard3(Guard newGuard) {
        if (!_supported) {
            if (QUIESCE_DISABLED)
                _logger.severe("Quiesce is not supported because the '" + SystemProperties.DISABLE_QUIESCE_MODE + "' was set");
            if (_spaceImpl.isLocalCache())
                _logger.severe("Quiesce is not supported for local-cache/local-view");
            return false;
        }

        if (_guard == null || newGuard.supersedes(_guard)) { // if there is no guard or new guard > current guard
            newGuard.innerGuard = _guard;
            _guard = newGuard; // guard = new guard, guard.inner = prevGuard
            _logger.info("Quiesce state set to " + desc(newGuard));
            return true;
        }
        if (_guard.supersedes(newGuard) && _guard.innerGuard == null) { // if current guard > newguard and no inner guard
            _guard.innerGuard = newGuard; // mark inner guard = new guard
            _logger.info("Quiesce guard was added, but is currently masked because state is " + desc(_guard));
            return true;
        }
        _logger.warning("Quiesce guard was discarded due to ambiguity - current state is " + desc(_guard));
        return false;
    }
//
    private synchronized Guard removeGuardHelper(boolean suspendableGuard) {
        Guard prevGuard;
        if (_guard == null) {
            prevGuard = null;
            _logger.warning("No guard to remove");
        } else if (suspendableGuard) { // remove suspend
            if (_guard.suspendLatch != null) { // current is suspend
                prevGuard = _guard; // store current guard in temp
                _guard = _guard.innerGuard; // make this = current.innerGuard
                _logger.info("Removed " + desc(prevGuard) + ", new state is " + desc(_guard));
            } else {
                prevGuard = null; // current is not suspend
                _logger.warning("No suspend guard to remove");
            }
        } else { // remove quiesce
            if (_guard.suspendLatch == null) { // current is not suspend
                prevGuard = _guard;
                _guard = null;
                _logger.info("Removed " + desc(prevGuard) + ", new state is " + desc(_guard));
            } else if (_guard.innerGuard != null) { // current is suspend and inner exists (Quiesce)
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
        return guard.status == Status.SUSPENDED ? "SUSPENDED" : "QUIESCED";
    }

    protected enum Status {
        SUSPENDED, QUIESCED_DEMOTE, QUIESCED
    }


//    private class Guard implements Closeable {
//        private final QuiesceToken token;
//        private final CountDownLatch suspendLatch;
//        private final QuiesceException exception;
//        private boolean closed;
//        Guard innerGuard;
//
//        Guard(String description, QuiesceToken token, Status status) {
//            this.token = token != null ? token : EmptyToken.INSTANCE;
//            boolean suspend = (status == Status.SUSPENDED);
//            this.suspendLatch = suspend ? new CountDownLatch(1) : null;
//            String errorMessage = "Operation cannot be executed - space [" + _spaceImpl.getServiceName() + "] is " +
//                    (suspend ? "suspended" : "quiesced") +
//                    (StringUtils.hasLength(description) ? " (" + description + ")" : "");
//            if (status == Status.QUIESCED_DEMOTE) {
//                this.exception = new QuiesceDemoteException(errorMessage);
//            } else {
//                this.exception = new QuiesceException(errorMessage);
//            }
//        }
//
//        @Override
//        public void close() {
//            if (suspendLatch != null)
//                suspendLatch.countDown();
//            closed = true;
//        }
//
//        protected boolean isClosed() {
//            return closed;
//        }
//
//        void guard(QuiesceToken operationToken) {
//            if (!token.equals(operationToken)) {
//                if (suspendLatch != null) { // if suspend
//                    if (safeAwait()) { // wait for unsuspend for 20s. if didn't happen -> throw the exception
//                        // Wait a random bit before returning to avoid storming the space.
//                        safeSleep(new Random().nextInt(1000));
//                        return;
//                    }
//                }
//                throw exception;
//            }
//        }
//
//        boolean supersedes(Guard otherGuard) {
//            //I am suspend and other is not suspend(quiesce)
//            return this.suspendLatch != null && otherGuard.suspendLatch == null;
//        }
//
//        private boolean safeAwait() {
//            try {
//                // TODO: Timeout should be configurable.
//                return suspendLatch.await(20, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                return suspendLatch.getCount() == 0;
//            }
//        }
//
//        private void safeSleep(long millis) {
//            try {
//                Thread.sleep(millis);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//    }

    protected class Guard implements Closeable {

        protected final QuiesceException exception;
        protected final String description;
        protected final QuiesceToken token;
        protected final Status status;
        private final CountDownLatch suspendLatch;

        protected Guard innerGuard;

        Guard(String description, QuiesceToken token, Status status) {
            this.description = description;
            this.token = token != null ? token : EmptyToken.INSTANCE;
            this.status = status;
            String guardReason; // space [..] is _____
            switch (status) {
                case QUIESCED:
                    guardReason = "quiesced";
                    suspendLatch = null;
                    break;
                case QUIESCED_DEMOTE:
                    guardReason = "demoting";
                    suspendLatch = null;
                    break;
                case SUSPENDED:
                    guardReason = "suspended";
                    suspendLatch = new CountDownLatch(1);
                    break;
                default:
                    guardReason = "";
                    suspendLatch = null;
                    break;
            }

            String errorMessage = "Operation cannot be executed - space [" + _spaceImpl.getServiceName() + "] is " +
                    guardReason +
                    (StringUtils.hasLength(description) ? " (" + description + ")" : "");
            if (status == Status.QUIESCED_DEMOTE) {
                this.exception = new QuiesceDemoteException(errorMessage);
            } else {
                this.exception = new QuiesceException(errorMessage);
            }
        }

        void guard(QuiesceToken operationToken) {
            if (!token.equals(operationToken)) {
                if (suspendLatch != null) { // if suspend
                    if (safeAwait()) { // wait for unsuspend for 20s. if didn't happen -> throw the exception
                        // Wait a random bit before returning to avoid storming the space.
                        safeSleep(new Random().nextInt(1000));
                        return;
                    }
                }
                throw exception;
            }
        }

        boolean supersedes(Guard otherGuard) {
            return this.status.compareTo(otherGuard.status) < 0;
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
    }

    private boolean hasGuard(Guard currentGuard, Status status) {
        if (currentGuard == null) return false;

        return currentGuard.status.equals(status) || hasGuard(currentGuard.innerGuard, status);
    }

    protected synchronized boolean addGuard(Guard newGuard) {
        if (!_supported) {
            if (QUIESCE_DISABLED)
                _logger.severe("Quiesce is not supported because the '" + SystemProperties.DISABLE_QUIESCE_MODE + "' was set");
            if (_spaceImpl.isLocalCache())
                _logger.severe("Quiesce is not supported for local-cache/local-view");
            return false;
        }

        if (hasGuard(_guard, newGuard.status)) {
            _logger.warning("Quiesce guard was discarded due to ambiguity - current state is " + desc(_guard));
            return false;
        }

        if (!guardCanBeAdded(_guard, newGuard)) {
            _logger.warning("Quiesce guard couldn't be added - current state is " + desc(_guard));
            return false;
        }

        try {
            Guard prevGuard = _guard;
            _guard = addGuardHelper(_guard, newGuard);
            if (prevGuard == _guard) {
                _logger.info("Quiesce guard "+desc(newGuard)+" was added, but is currently masked because state is " + desc(_guard));
            } else {
                _logger.info("Quiesce state set to " + desc(_guard));
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

    protected Guard addGuardHelper(Guard currentGuard, Guard newGuard) throws Exception {
        Guard res;
        if (currentGuard == null) {
            res = newGuard;
            //newGuard.innerGuard = null; ??
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

    protected synchronized boolean removeGuard(Status status) {
        if (_guard == null) {
            _logger.warning("No guard to remove");
            return false;
        }
        if (!hasGuard(_guard, status)) {
            _logger.warning("No "+status+" guard to remove");
            return false;
        }
        _guard = removeGuardHelper(_guard, status);
        _logger.info("Removed " + status + ", new state is " + desc(_guard));
        return true;
    }


    //returns the new guard
    protected Guard removeGuardHelper(Guard guard, Status status) {
        if (guard.status.equals(status)) {
            return guard.innerGuard;
        } else {
            guard.innerGuard = removeGuardHelper(guard.innerGuard, status);
            return guard;
        }
    }

}

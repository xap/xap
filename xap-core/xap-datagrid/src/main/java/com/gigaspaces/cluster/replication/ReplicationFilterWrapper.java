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

package com.gigaspaces.cluster.replication;

import com.j_spaces.core.IJSpace;
import com.j_spaces.core.cluster.IReplicationFilter;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.kernel.ClassLoaderHelper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper for the {@link IReplicationFilter}
 *
 * @author anna
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationFilterWrapper {
    // logger
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_REPLICATION);

    private final IReplicationFilter _filter;

    private static enum State {UNINITIALIZED, INITIALIZED, CLOSED;}

    ;

    private volatile State _state = State.UNINITIALIZED;

    public ReplicationFilterWrapper(String className) {
        // create the filter object
        try {
            _filter = (IReplicationFilter) ClassLoaderHelper.loadClass(className)
                    .newInstance();
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Failed to add replication filter "
                        + className + ": " + "newInstance() aborted: ", e);
            }

            throw new RuntimeException("Failed to add ReplicationFilter "
                    + className + ":" + " newInstance() aborted", e);
        }
    }

    public ReplicationFilterWrapper(IReplicationFilter filter) {
        _filter = filter;
    }

    public void init(IJSpace space, String paramUrl,
                     ReplicationPolicy replicationPolicy) {
        // check if filter was already initialized
        // this can happen is the input filter is the same as the output filter
        if (isInitialized())
            return;

        try {
            _filter.init(space, paramUrl, replicationPolicy);
            _state = State.INITIALIZED;

        } catch (RuntimeException re) {
            if (_logger.isErrorEnabled()) {
                _logger.error(
                        "Failed to initialize replication filter "
                                + getClassName(),
                        re);
            }

            close();
            throw re;
        }

    }

    /**
     * Delegates the call the the user filter
     */
    public void process(int direction,
                        IReplicationFilterEntry replicationFilterEntry,
                        String remoteSpaceMemberName) {
        if (!isInitialized())
            return;
        _filter.process(direction,
                replicationFilterEntry,
                remoteSpaceMemberName);
    }

    public void close() {
        if (!isInitialized())
            return;

        try {
            _filter.close();

            if (_logger.isDebugEnabled()) {
                _logger.debug("ReplicationFilterManager: Filter: "
                        + getClassName() + " closed successfully.");
            }
            _state = State.CLOSED;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(
                        "ReplicationFilterManager:  Failed to close "
                                + getClassName(),
                        e);
            }
        }
    }

    public boolean isInitialized() {
        return _state == State.INITIALIZED;
    }

    public String getClassName() {
        return _filter.getClass().getName();
    }

    @Override
    public String toString() {
        return "ReplicationFilterWrapper [_filter=" + _filter + ", _state="
                + _state + "]";
    }


}

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

package com.gigaspaces.cluster.activeelection.core;


import com.gigaspaces.cluster.activeelection.ICandidateEntry;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.lookup.entry.HostName;

import net.jini.core.lookup.ServiceItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class SplitBrainServiceEntry
        implements Comparable<SplitBrainServiceEntry> {
    @SuppressWarnings("UnusedDeclaration")
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_CLUSTER_ACTIVE_ELECTION);


    private final ServiceItem _service;
    private final ServiceReplicationStatus _replicationStatus;
    private final ICandidateEntry _electionPriorityEntry;

    public SplitBrainServiceEntry(ServiceItem service,
                                  ServiceReplicationStatus replicationStatus,
                                  ICandidateEntry electionPriorityEntry) {
        super();
        _service = service;
        _replicationStatus = replicationStatus;
        _electionPriorityEntry = electionPriorityEntry;
    }

    public ICandidateEntry getElectionPriorityEntry() {
        return _electionPriorityEntry;
    }

    public ServiceItem getService() {
        return _service;
    }


    public ServiceReplicationStatus getReplicationStatus() {
        return _replicationStatus;
    }

    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") SplitBrainServiceEntry o) {
        //first try to resolve based on the replication status - the most replication consistent space wins
        int replicationStatusCompare = _replicationStatus.compareTo(o.getReplicationStatus());
        if (replicationStatusCompare != 0)
            return replicationStatusCompare;

        //finally try to resolve based on the original election priority
        return _electionPriorityEntry.compareTo(o.getElectionPriorityEntry());

    }

    @Override
    public String toString() {
        StringBuilder logBuf = new StringBuilder();
        String host = HostName.getHostNameFrom(_service.attributeSets);
        Object service = _service.getService();
        logBuf.append("[")
                .append(service)
                .append("] space");
        if (host != null)
            logBuf.append(" on [")
                    .append(host)
                    .append("] machine");
        logBuf.append(" [pid=");
        logBuf.append(_replicationStatus.getProcessId());
        logBuf.append("]");
        logBuf.append(StringUtils.NEW_LINE);
        logBuf.append("Service properties = ").append(StringUtils.NEW_LINE);
        logBuf.append("{");
        logBuf.append(_replicationStatus).append(StringUtils.NEW_LINE);

        logBuf.append("}");
        return logBuf.toString();
    }


}

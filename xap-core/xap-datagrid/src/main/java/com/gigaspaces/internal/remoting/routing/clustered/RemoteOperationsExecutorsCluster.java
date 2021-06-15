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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.SpaceProxyLoadBalancerType;
import com.gigaspaces.internal.exceptions.ChunksMapGenerationException;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.RemoteOperationRouterException;
import com.gigaspaces.internal.utils.concurrent.CompetitionExecutor;
import com.gigaspaces.internal.utils.concurrent.CompetitiveTask;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.TimedCompetitionExecutor;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.exception.ClosedResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RemoteOperationsExecutorsCluster {
    private final Logger _logger;
    private final String _name;
    private final SpaceClusterInfo _clusterInfo;
    private final int _partitionId;
    private final String _partitionName;
    private final Map<String, RemoteOperationsExecutorProxy> _members;
    private final RemoteOperationsExecutorsClusterConfig _config;
    private final Object _lock;
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final RemoteOperationsExecutorProxyLocator _proxyLocator;
    private final SpaceProxyLoadBalancingStrategy _loadBalancer;
    private SpaceProxyImpl _spaceProxy;
    private boolean _closed;

    public RemoteOperationsExecutorsCluster(SpaceProxyImpl spaceProxy, String name, SpaceClusterInfo clusterInfo, int partitionId,
                                            Collection<String> members, RemoteOperationsExecutorsClusterConfig config,
                                            IAsyncHandlerProvider asyncHandlerProvider, RemoteOperationsExecutorProxyLocator proxyLocator,
                                            RemoteOperationsExecutorProxy defaultMember) {
        this._logger = LoggerFactory.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER_LOOKUP + '.' + name);
        this._lock = new Object();
        this._name = name;
        this._clusterInfo = clusterInfo;
        this._partitionId = partitionId;
        this._partitionName = _partitionId == -1 ? name : name + '#' + (partitionId + 1);
        this._members = new HashMap<String, RemoteOperationsExecutorProxy>();
        for (String member : members)
            this._members.put(member, null);
        this._config = config;
        this._asyncHandlerProvider = asyncHandlerProvider;
        this._proxyLocator = proxyLocator;
        this._loadBalancer = createLoadBalancer(config, members, defaultMember);
        this._spaceProxy = spaceProxy;
        if (config.getLoadBalancerType() == SpaceProxyLoadBalancerType.ROUND_ROBIN)
            refreshDisconnectedMembers();
    }

    public RemoteOperationsExecutorsCluster(String name, SpaceClusterInfo clusterInfo, int partitionId,
                                            Collection<String> members, RemoteOperationsExecutorsClusterConfig config,
                                            IAsyncHandlerProvider asyncHandlerProvider, RemoteOperationsExecutorProxyLocator proxyLocator,
                                            RemoteOperationsExecutorProxy defaultMember) {
        this._logger = LoggerFactory.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER_LOOKUP + '.' + name);
        this._lock = new Object();
        this._name = name;
        this._clusterInfo = clusterInfo;
        this._partitionId = partitionId;
        this._partitionName = _partitionId == -1 ? name : name + '#' + (partitionId + 1);
        this._members = new HashMap<String, RemoteOperationsExecutorProxy>();
        for (String member : members)
            this._members.put(member, null);
        this._config = config;
        this._asyncHandlerProvider = asyncHandlerProvider;
        this._proxyLocator = proxyLocator;
        this._loadBalancer = createLoadBalancer(config, members, defaultMember);
        if (config.getLoadBalancerType() == SpaceProxyLoadBalancerType.ROUND_ROBIN)
            refreshDisconnectedMembers();
    }

    public String getName() {
        return _name;
    }

    public Logger getLogger() {
        return _logger;
    }

    public SpaceClusterInfo getClusterInfo() {
        return _clusterInfo;
    }

    public int getPartitionId() {
        return _partitionId;
    }

    public String getPartitionDesc() {
        return _partitionId == -1 ? "" : " for partition #" + (_partitionId + 1);
    }

    public Collection<String> getMembersNames() {
        synchronized (_lock) {
            return _members.keySet();
        }
    }

    public SpaceProxyLoadBalancingStrategy getLoadBalancer() {
        return _loadBalancer;
    }

    public RemoteOperationsExecutorsClusterConfig getConfig() {
        return _config;
    }

    public RemoteOperationsExecutorProxy getAvailableMember(boolean activeOnly, long timeout)
            throws InterruptedException, RemoteOperationRouterException {
        if (timeout <= 0)
            throw new IllegalArgumentException("Timeout must be bigger than 0, got: " + timeout);

        final boolean timeBased = true;
        MemberLocatorTask[] tasks;
        synchronized (_lock) {
            validateNotClosed();
            tasks = new MemberLocatorTask[_members.size()];
            int i = 0;
            for (Map.Entry<String, RemoteOperationsExecutorProxy> entry : _members.entrySet())
                tasks[i++] = new MemberLocatorTask(entry.getKey(), entry.getValue(), activeOnly, timeBased);
        }

        if (_logger.isDebugEnabled()) {
            String message = "Starting " + (activeOnly ? "active" : "available") + " server lookup" + getPartitionDesc() + " [timeout=" + timeout + "ms, members=(" + tasks[0]._memberName;
            for (int i = 1; i < tasks.length; i++)
                message += ", " + tasks[i]._memberName;
            message += ")]...";
            _logger.debug(message);
        }

        final String competitionName = (activeOnly ? "Active" : "Available") + "MemberLocator" + "_" + _partitionName;
        final long competitionInterval = _config.getActiveServerLookupSamplingInterval();
        CompetitionExecutor<CompetitiveTask> competition;
        if(this.getClusterInfo().isChunksRouting() && this._spaceProxy.isClustered()) {
            CompetitiveTask[] taskToExecute = new CompetitiveTask[tasks.length + 1];
            System.arraycopy(tasks, 0, taskToExecute, 0, tasks.length);
            taskToExecute[tasks.length] = new CheckRoutingGenerationTask(_spaceProxy, _clusterInfo.getTopology().getGeneration());
            competition = new TimedCompetitionExecutor<>(taskToExecute,
                    timeout,
                    competitionName,
                    _asyncHandlerProvider,
                    competitionInterval);
        } else {
            competition = new TimedCompetitionExecutor<>(tasks,
                    timeout,
                    competitionName,
                    _asyncHandlerProvider,
                    competitionInterval);
        }

        try {
            CompetitiveTask winner = competition.await(timeout, TimeUnit.MILLISECONDS);
            if (winner != null){
                if(winner instanceof  MemberLocatorTask){
                    return ((MemberLocatorTask) winner).getProxy();
                }

                if (this.getClusterInfo().isChunksRouting() && winner instanceof CheckRoutingGenerationTask) {
                    int oldGeneration = this.getClusterInfo().getTopology().getGeneration();
                    this._spaceProxy.updateProxyRouter(_spaceProxy.getProxyRouter(), ((CheckRoutingGenerationTask) winner).getNewMap());
                    throw new ChunksMapGenerationException("chunks map generation of client is " + oldGeneration
                            + " but server is at generation " + this._spaceProxy.getProxyRouter().getChunksMapGeneration());
                }
            }
            return  null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ClosedResourceException)
                throw (ClosedResourceException) e.getCause();
            String message = "Unexpected exception while locating an " + (activeOnly ? "active" : "available") + " server" + getPartitionDesc();
            if (_logger.isErrorEnabled())
                _logger.error(message, e.getCause());
            throw new RemoteOperationRouterException(message, e.getCause());
        }
    }

    public void getAllAvailableMembers(List<RemoteOperationsExecutorProxy> availableMembers) {
        List<String> membersToFind = new ArrayList<String>();
        synchronized (_lock) {
            validateNotClosed();
            for (Map.Entry<String, RemoteOperationsExecutorProxy> entry : _members.entrySet()) {
                if (entry.getValue() != null)
                    availableMembers.add(entry.getValue());
                else
                    membersToFind.add(entry.getKey());
            }
        }

        for (String member : membersToFind) {
            RemoteOperationsExecutorProxy proxy = find(member, LookupType.RepeatsBased);
            if (proxy != null)
                availableMembers.add(proxy);
        }
    }


    private void validateNotClosed() {
        if (_closed)
            throw new ClosedResourceException("Proxy is closed");
    }

    public void refreshConnectedMembers() {
        ArrayList<RemoteOperationsExecutorProxy> connectedMembers = new ArrayList<RemoteOperationsExecutorProxy>();
        synchronized (_lock) {
            validateNotClosed();
            for (Map.Entry<String, RemoteOperationsExecutorProxy> entry : _members.entrySet())
                if (entry.getValue() != null)
                    connectedMembers.add(entry.getValue());
        }

        for (RemoteOperationsExecutorProxy proxy : connectedMembers) {
            try {
                proxy.isActive();
            } catch (RemoteException e) {
                disconnect(proxy);
            }
        }
    }

    public void refreshDisconnectedMembers() {
        ArrayList<String> disconnectedMembers = new ArrayList<String>();
        synchronized (_lock) {
            validateNotClosed();
            for (Map.Entry<String, RemoteOperationsExecutorProxy> entry : _members.entrySet())
                if (entry.getValue() == null)
                    disconnectedMembers.add(entry.getKey());
        }

        for (String member : disconnectedMembers)
            find(member, LookupType.RepeatsBased);
    }

    public RemoteOperationsExecutorProxy find(String member, LookupType lookupType) {
        RemoteOperationsExecutorProxy proxy = _proxyLocator.locateMember(member, lookupType);
        if (proxy != null)
            add(proxy);
        return proxy;
    }

    public void add(RemoteOperationsExecutorProxy proxy) {
        synchronized (_lock) {
            validateNotClosed();
            _members.put(proxy.getName(), proxy);
        }
        _loadBalancer.onMemberConnected(proxy);
    }


    public void disconnect(RemoteOperationsExecutorProxy proxy) {
        if (proxy == null)
            return;

        synchronized (_lock) {
            validateNotClosed();
            RemoteOperationsExecutorProxy oldProxy = _members.get(proxy.getName());
            if (oldProxy != proxy)
                return;
            _members.put(proxy.getName(), null);
        }
        _loadBalancer.onMemberDisconnected(proxy.getName());
    }

    public void close() {
        synchronized (_lock) {
            if (_closed)
                return;

            for (RemoteOperationsExecutorProxy proxy : _members.values()) {
                if (proxy != null)
                    proxy.close();
            }
            _members.clear();
        }
    }

    public class MemberLocatorTask implements CompetitiveTask {
        private final String _memberName;
        private final boolean _activeOnly;
        private volatile RemoteOperationsExecutorProxy _proxy;
        private final boolean _timeBased;

        public MemberLocatorTask(String name, RemoteOperationsExecutorProxy proxy, boolean activeOnly, boolean timeBased) {
            this._memberName = name;
            this._proxy = proxy;
            this._activeOnly = activeOnly;
            this._timeBased = timeBased;
        }

        public RemoteOperationsExecutorProxy getProxy() {
            return _proxy;
        }

        @Override
        public boolean execute(boolean isLastIteration) {
            if (_proxy == null) {
                LookupType lookupType;

                if (!_timeBased)
                    lookupType = LookupType.RepeatsBased;
                else if (isLastIteration)
                    lookupType = LookupType.TimeBasedLastIteration;
                else
                    lookupType = LookupType.TimeBased;

                _proxy = find(_memberName, lookupType);
                if (_proxy == null)
                    return false;
            }

            if (_logger.isTraceEnabled())
                _logger.trace("Checking if " + _memberName + (_activeOnly ? " is active..." : " is available..."));
            try {
                boolean isActive = _proxy.isActiveQuiesceTokenAware();
                if (!_activeOnly) {
                    if (_logger.isDebugEnabled())
                        _logger.debug("Member " + _memberName + " is available.");
                    return true;
                }

                if (isActive) {
                    if (_logger.isDebugEnabled())
                        _logger.debug("Member " + _memberName + " is active.");
                } else {
                    if (_logger.isDebugEnabled())
                        _logger.debug("Member " + _memberName + " is not active.");
                }
                return isActive;
            } catch (RemoteException e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Member " + _memberName + " failed to respond: " + e.getClass().getName() + ": " + e.getMessage());
                disconnect(_proxy);
                _proxy = null;
                return false;
            }
        }
    }

    public long getRemainingTime(RemoteOperationRequest<?> request, long initialFailureTime) {
        long remainingTime = _config.getActiveServerLookupTimeout() - (SystemTime.timeMillis() - initialFailureTime);
        if (remainingTime <= 0) {
            request.setRemoteOperationExecutionError(new RemoteException(generateTimeoutErrorMessage(initialFailureTime, request)));
        }
        return remainingTime;
    }

    public String generateTimeoutErrorMessage(long initialFailureTime, RemoteOperationRequest<?> request) {
        //todo
        return "Failed to find an active server" + getPartitionDesc() + " to execute " + request.toString() + getElapsedTime(initialFailureTime);
    }

    public String getElapsedTime(long initialFailureTime) {
        long elapsedTime = SystemTime.timeMillis() - initialFailureTime;
        return "(elapsed time: " + elapsedTime + "ms)";
    }

    private SpaceProxyLoadBalancingStrategy createLoadBalancer(RemoteOperationsExecutorsClusterConfig config, Collection<String> members, RemoteOperationsExecutorProxy defaultMember) {
        SpaceProxyLoadBalancerType loadBalancerType = config.getLoadBalancerType();
        switch (loadBalancerType) {
            case STICKY:
                return new LazyLoadBalancingStrategy(this, defaultMember);
            case ROUND_ROBIN:
                return new RoundRobinLoadBalancingStrategy(this, members, config.getNumOfOperationsTypes());
            default:
                throw new IllegalStateException("Unsupported load balancer type " + loadBalancerType);
        }
    }
}

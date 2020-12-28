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

package com.gigaspaces.internal.client.spaceproxy.router;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceOperationResult;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.SpaceProxyLoadBalancerType;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.quiesce.QuiesceTokenProviderImpl;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.clustered.PostponedAsyncOperationsQueue;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsCluster;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsClusterConfig;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteSpaceProxyLocator;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.ScheduledThreadPoolAsyncHandlerProvider;
import com.j_spaces.core.Constants;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.SpaceURLParser;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyRouter {
    private final Logger _logger;
    private final SpaceClusterInfo _clusterInfo;
    private SpaceContext _defaultSpaceContext;
    private final SpaceRemoteOperationsExecutorsClusterConfig _config;
    private final SpaceProxyRemoteOperationRouter _router;
    private final PostponedAsyncOperationsQueue _postponedAsyncOperationsQueue;
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final RemoteSpaceProxyLocator _proxyLocator;
    private final boolean isGateway;
    private final boolean isSecured;
    private final boolean clusteredProxy;
    private final QuiesceTokenProviderImpl quiesceTokenProvider;

    public SpaceProxyRouter(SpaceProxyImpl spaceProxy) {
        this._logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEPROXY_ROUTER + '.' + spaceProxy.getName());
        this._clusterInfo = spaceProxy.getSpaceClusterInfo();
        this.isGateway = spaceProxy.isGatewayProxy();
        this.isSecured = spaceProxy.isSecured();
        this.clusteredProxy = spaceProxy.isClustered() && !spaceProxy.isEmbedded();
        this.quiesceTokenProvider = new QuiesceTokenProviderImpl();
        updateDefaultSpaceContext(null);
        Properties properties = loadConfig(spaceProxy.getProxySettings().getCustomProperties(), _clusterInfo);
        this._config = new SpaceRemoteOperationsExecutorsClusterConfig(properties);
        if (_clusterInfo.isPartitioned() && _config.getLoadBalancerType() == SpaceProxyLoadBalancerType.ROUND_ROBIN)
            throw new IllegalStateException("Cannot use round robin load balancing with a partitioned space.");
        if (_logger.isDebugEnabled()) {
            _logger.debug("Initializing space proxy router - [" +
                    "Active server lookup timeout=" + _config.getActiveServerLookupTimeout() +
                    ", Active server lookup sampling interval=" + _config.getActiveServerLookupSamplingInterval() +
                    ", Thread pool size=" + _config.getThreadPoolSize() +
                    "]");
        }
        this._proxyLocator = createProxyLocator(spaceProxy);
        this._asyncHandlerProvider = new ScheduledThreadPoolAsyncHandlerProvider(spaceProxy.getName() + "-router-threadpool", _config.getThreadPoolSize());
        if (spaceProxy.isEmbedded() && (!_clusterInfo.isPartitioned() || !spaceProxy.isClustered())) {
            int partitionId = PartitionedClusterUtils.extractPartitionIdFromSpaceName(spaceProxy.getRemoteMemberName());
            this._postponedAsyncOperationsQueue = null;
            this._router = new SpaceEmbeddedRemoteOperationRouter(spaceProxy, partitionId, quiesceTokenProvider);
        } else {
            this._postponedAsyncOperationsQueue = new PostponedAsyncOperationsQueue(spaceProxy.getName());
            if (spaceProxy.isClustered()) {
                if (_clusterInfo.isPartitioned())
                    this._router = createPartitionedRouter(spaceProxy, _clusterInfo, _config);
                else
                    this._router = createClusteredRouter(spaceProxy, _clusterInfo.getMembersNames(), _config);
            } else
                this._router = createClusteredRouter(spaceProxy, CollectionUtils.toList(spaceProxy.getRemoteMemberName()), _config);
        }

        warnIfOldConfigIsUsed(properties);
    }

    public IAsyncHandlerProvider getAsyncHandlerProvider() {
        return _asyncHandlerProvider;
    }

    private void warnIfOldConfigIsUsed(Properties properties) {
        if (_logger.isWarnEnabled()) {
            testDeprecatedProperty(properties, Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_FULL);
            testDeprecatedProperty(properties, Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_FULL);
            testDeprecatedProperty(properties, Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_FULL);
            testDeprecatedProperty(properties, Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_FULL);
            testDeprecatedProperty(properties, Constants.SpaceProxy.OldRouter.RETRY_CONNECTION_FULL);
        }
    }

    private void testDeprecatedProperty(Properties properties, String propertyName) {
        if (properties.containsKey(propertyName))
            _logger.warn("Property is ignored when using the new space proxy router: " + propertyName);
    }

    private SpaceProxyRemoteOperationRouter createClusteredRouter(SpaceProxyImpl spaceProxy, List<String> membersNames,
                                                                  RemoteOperationsExecutorsClusterConfig config) {
        final RemoteOperationsExecutorProxy defaultProxy = new RemoteOperationsExecutorProxy(spaceProxy.getRemoteMemberName(), spaceProxy.getRemoteJSpace(), quiesceTokenProvider);
        final RemoteOperationsExecutorsCluster cluster = new RemoteOperationsExecutorsCluster(spaceProxy, spaceProxy.getName(), _clusterInfo, -1,
                membersNames, config, _asyncHandlerProvider, _proxyLocator, defaultProxy);
        return new SpaceClusterRemoteOperationRouter(cluster, _postponedAsyncOperationsQueue, spaceProxy);
    }

    private SpaceProxyRemoteOperationRouter createPartitionedRouter(SpaceProxyImpl spaceProxy, SpaceClusterInfo clusterInfo,
                                                                    RemoteOperationsExecutorsClusterConfig config) {
        final SpaceProxyRemoteOperationRouter[] partitions = new SpaceProxyRemoteOperationRouter[clusterInfo.getNumberOfPartitions()];
        String embeddedMemberName = spaceProxy.isEmbedded() ? spaceProxy.getRemoteMemberName() : null;
        for (int i = 0; i < partitions.length; i++) {
            final List<String> members = clusterInfo.getPartitionMembersNames(i);
            if (isEmbeddedPartition(members, embeddedMemberName))
                partitions[i] = new SpaceEmbeddedRemoteOperationRouter(spaceProxy, i, quiesceTokenProvider);
            else {
                RemoteOperationsExecutorsCluster cluster = new RemoteOperationsExecutorsCluster(spaceProxy, spaceProxy.getName(),
                        clusterInfo, i, members, config, _asyncHandlerProvider, _proxyLocator, null);
                partitions[i] = new SpaceClusterRemoteOperationRouter(cluster, _postponedAsyncOperationsQueue,
                        spaceProxy);
            }
        }

        RemoteOperationsExecutorsCluster partitionedCluster = new RemoteOperationsExecutorsCluster(spaceProxy, spaceProxy.getName(),
                clusterInfo,
                PartitionedClusterUtils.NO_PARTITION,
                clusterInfo.getMembersNames(),
                config,
                _asyncHandlerProvider,
                _proxyLocator,
                null);

        return new SpacePartitionedClusterRemoteOperationRouter(spaceProxy.getName(),
                partitions,
                clusterInfo.isBroadcastDisabled(),
                partitionedCluster, clusterInfo, spaceProxy);
    }

    private Properties loadConfig(Properties properties, SpaceClusterInfo clusterInfo) {
        if (!properties.containsKey(Constants.SpaceProxy.Router.LOAD_BALANCER_TYPE)) {
            if (clusterInfo != null && clusterInfo.getLoadBalancerType() != null && clusterInfo.getLoadBalancerType() != SpaceProxyLoadBalancerType.STICKY) {
                properties.setProperty(Constants.SpaceProxy.Router.LOAD_BALANCER_TYPE, clusterInfo.getLoadBalancerType().toString());
                if (_logger.isWarnEnabled())
                    _logger.warn("Setting load balance type via cluster policy is deprecated, use space property '" + Constants.SpaceProxy.Router.LOAD_BALANCER_TYPE + "' instead.");
            }
        }
        return properties;
    }

    private RemoteSpaceProxyLocator createProxyLocator(SpaceProxyImpl spaceProxy) {
        SpaceURL spaceUrl = spaceProxy.getFinderURL();
        if (spaceUrl == null)
            spaceUrl = spaceProxy.getURL();

        if (spaceUrl.isJiniProtocol()) {
            spaceUrl = spaceUrl.clone();
            spaceUrl.remove(SpaceURL.USE_LOCAL_CACHE);
        } else {
            String url = SpaceUrlUtils.buildJiniUrl(spaceUrl.getContainerName(), spaceUrl.getSpaceName(),
                    spaceUrl.getProperty(SpaceURL.GROUPS), null /*locators*/);

            try {
                spaceUrl = SpaceURLParser.parseURL(url);
            } catch (MalformedURLException e) {
                throw new ProxyInternalSpaceException(e);
            }
        }

        return new RemoteSpaceProxyLocator(spaceProxy.getName(), spaceUrl, quiesceTokenProvider);
    }

    private boolean isEmbeddedPartition(List<String> members, String embeddedMemberName) {
        if (embeddedMemberName == null)
            return false;
        for (String member : members)
            if (member.equals(embeddedMemberName))
                return true;
        return false;
    }

    /**
     * Closes this space proxy router. should be executed under a lock.
     */
    public void close() {
        if (_postponedAsyncOperationsQueue != null)
            _postponedAsyncOperationsQueue.close();
        if (_asyncHandlerProvider != null)
            _asyncHandlerProvider.close();
        _router.close();
    }

    public <T extends SpaceOperationResult> void execute(RemoteOperationRequest<T> request)
            throws InterruptedException {
        _router.execute(request);
    }

    public <T extends SpaceOperationResult> RemoteOperationFutureListener<T> executeAsync(RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener) {
        RemoteOperationFutureListener<T> futureListener = _router.createFutureListener(request, listener);
        _router.executeAsync(request, futureListener);
        return futureListener;
    }

    public void executeOneway(RemoteOperationRequest<?> request) throws InterruptedException {
        _router.executeOneway(request);
    }

    public RemoteOperationsExecutorsClusterConfig getConfig() {
        return _config;
    }

    public IRemoteSpace getAnyAvailableSpace() {
        RemoteOperationsExecutorProxy member = _router.getAnyAvailableMember();
        return member != null ? (IRemoteSpace) member.getExecutor() : null;
    }

    public IRemoteSpace getAnyActiveSpace() {
        RemoteOperationsExecutorProxy member = _router.getAnyActiveMember();
        return member != null ? (IRemoteSpace) member.getExecutor() : null;
    }

    private List<RemoteOperationsExecutorProxy> getAllAvailableMembers() {
        List<RemoteOperationsExecutorProxy> result = new ArrayList<RemoteOperationsExecutorProxy>();
        _router.getAllAvailableMembers(result);
        return result;
    }

    public List<IRemoteSpace> getAllAvailableSpaces() {
        List<RemoteOperationsExecutorProxy> availableMembers = getAllAvailableMembers();
        List<IRemoteSpace> result = new ArrayList<IRemoteSpace>();
        for (RemoteOperationsExecutorProxy member : availableMembers)
            result.add((IRemoteSpace) member.getExecutor());
        return result;
    }

    public SpaceURL getMemberUrl(String memberName) {
        return memberName == null ? null : _proxyLocator.getMemberUrl(memberName);
    }

    public SpaceURL getPrimaryMemberUrl(int partitionId) {
        return getMemberUrl(_router.getActiveMemberName(partitionId));
    }

    public RemoteSpaceProxyLocator getProxyLocator() {
        return _proxyLocator;
    }

    public SpaceContext getDefaultSpaceContext() {
        return _defaultSpaceContext;
    }

    public int getChunksMapGeneration() {
        return _clusterInfo.isChunksRouting() ? _clusterInfo.getTopology().getGeneration() : 0;
    }

    public void setQuiesceToken(QuiesceToken token) {
        updateDefaultSpaceContext(token);
    }

    private void updateDefaultSpaceContext(QuiesceToken token) {
        this._defaultSpaceContext = isSecured || isGateway || token != null || _clusterInfo.isChunksRouting() || clusteredProxy
                ? new SpaceContext(isGateway, getChunksMapGeneration(), clusteredProxy) : null;
        if (token != null) {
            _defaultSpaceContext.setQuiesceToken(token);
        }
        quiesceTokenProvider.setToken(token);
    }

    public SpaceProxyRemoteOperationRouter getOperationRouter() {
        return this._router;
    }
}

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

package com.gigaspaces.internal.extension;

import com.gigaspaces.internal.client.spaceproxy.executors.SpaceDataSourceLoadTask;
import com.gigaspaces.internal.server.space.executors.SpaceDataSourceLoadExecutor;
import com.gigaspaces.internal.client.spaceproxy.executors.*;
import com.gigaspaces.internal.cluster.node.impl.ReplicationUtils;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilderFactory;
import com.gigaspaces.internal.server.space.executors.*;
import com.gigaspaces.internal.server.space.quiesce.WaitForDataDrainExecutors;
import com.gigaspaces.internal.server.space.repartitioning.CopyChunksTask;
import com.gigaspaces.internal.server.space.repartitioning.DeleteChunksTask;
import com.gigaspaces.internal.server.space.repartitioning.SpaceCopyChunksExecutor;
import com.gigaspaces.internal.server.space.repartitioning.SpaceDeleteChunksExecutor;
import com.gigaspaces.internal.utils.XapRuntimeReporter;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.client.SpaceFinderListener;

import java.io.Externalizable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class XapExtensions {
    private static final Logger logger = LoggerFactory.getLogger(XapExtensions.class.getName());
    private XapRuntimeReporter xapRuntimeReporter = new XapRuntimeReporter();
    private SpaceFinderListener spaceFinderListener;
    private ReplicationNodeConfigBuilder replicationNodeConfigBuilder = new ReplicationNodeConfigBuilder();
    private ReplicationRouterBuilderFactory replicationRouterBuilderFactory = new ReplicationRouterBuilderFactory();
    private ReplicationUtils replicationUtils = new ReplicationUtils();
    private final Map<Class<? extends SystemTask>, SpaceActionExecutor> actionExecutors = new HashMap<Class<? extends SystemTask>, SpaceActionExecutor>();
    private final Map<Class<? extends Externalizable>, CustomSerializer> customSerializers =
            new HashMap<Class<? extends Externalizable>, CustomSerializer>();

    private static XapExtensions instance;

    public static synchronized XapExtensions getInstance() {
        if (instance == null) {
            instance = new XapExtensions();
            XapExtensionActivator.scanAndActivate(
                    XapExtensions.class.getClassLoader(), "extensions-core");
        }
        return instance;
    }

    private XapExtensions() {
        registerSystemTaskExecutor(GetTypeDescriptorTask.class, new SpaceGetTypeDescriptorExecutor());
        registerSystemTaskExecutor(RegisterTypeDescriptorTask.class, new SpaceRegisterTypeDescriptorExecutor());
        registerSystemTaskExecutor(UnregisterTypeDescriptorTask.class, new SpaceUnregisterTypeDescriptorExecutor());
        registerSystemTaskExecutor(AddTypeIndexesTask.class, new SpaceAddTypeIndexesExecutor());
        registerSystemTaskExecutor(RegisterReplicationLocalViewTask.class, new SpaceRegisterReplicationLocalViewExecutor());
        registerSystemTaskExecutor(UnregisterReplicationLocalViewTask.class, new SpaceUnregisterReplicationLocalViewExecutor());
        registerSystemTaskExecutor(RegisterReplicationNotificationTask.class, new SpaceRegisterReplicationNotificationExecutor());
        registerSystemTaskExecutor(UnregisterReplicationNotificationTask.class, new SpaceUnregisterReplicationNotificationExecutor());
        registerSystemTaskExecutor(GetBatchForIteratorDistributedSpaceTask.class, new SpaceGetBatchForIteratorExecutor());
        registerSystemTaskExecutor(SinglePartitionGetBatchForIteratorSpaceTask.class, new SpaceGetBatchForIteratorExecutor());
        registerSystemTaskExecutor(CloseIteratorDistributedSpaceTask.class, new SpaceCloseIteratorExecutor());
        registerSystemTaskExecutor(RenewIteratorLeaseDistributedSpaceTask.class, new SpaceRenewIteratorLeaseExecutor());
        registerSystemTaskExecutor(SpaceDataSourceLoadTask.class, new SpaceDataSourceLoadExecutor());
        registerSystemTaskExecutor(CopyChunksTask.class, new SpaceCopyChunksExecutor());
        registerSystemTaskExecutor(DeleteChunksTask.class, new SpaceDeleteChunksExecutor());
        registerSystemTaskExecutor(CollocatedJoinSpaceTask.class, new SpaceCollocatedJoinExecutor());
        registerSystemTaskExecutor(BroadcastTableSpaceTask.class, new SpaceBroadcastTableExecutor());
        registerSystemTaskExecutor(GetEntriesTieredMetaDataTask.class, new SpaceGetEntriesTieredMetaDataExecutor());
        registerSystemTaskExecutor(WaitForDataDrainTask.class, new WaitForDataDrainExecutors());
    }

    public ReplicationRouterBuilderFactory getReplicationRouterBuilderFactory() {
        return replicationRouterBuilderFactory;
    }

    public void setReplicationRouterBuilderFactory(ReplicationRouterBuilderFactory factory) {
        this.replicationRouterBuilderFactory = factory;
    }

    public SpaceFinderListener getSpaceFinderListener() {
        return spaceFinderListener;
    }

    public void setSpaceFinderListener(SpaceFinderListener listener) {
        spaceFinderListener = listener;
    }

    public ReplicationNodeConfigBuilder getReplicationNodeConfigBuilder() {
        return replicationNodeConfigBuilder;
    }

    public void setReplicationNodeConfigBuilder(ReplicationNodeConfigBuilder configBuilder) {
        replicationNodeConfigBuilder = configBuilder;
    }

    public ReplicationUtils getReplicationUtils() {
        return replicationUtils;
    }

    public void setReplicationUtils(ReplicationUtils replicationUtils) {
        this.replicationUtils = replicationUtils;
    }

    public Map<Class<? extends SystemTask>, SpaceActionExecutor> getActionExecutors() {
        return Collections.unmodifiableMap(actionExecutors);
    }

    public void registerSystemTaskExecutor(Class<? extends SystemTask> taskClass, SpaceActionExecutor executor) {
        if (logger.isDebugEnabled())
            logger.debug("Registering system task" + taskClass.getName() + " => " + executor.getClass().getName());
        actionExecutors.put(taskClass, executor);
    }

    public <T extends Externalizable> CustomSerializer<T> getCustomSerializer(Class<T> c, PlatformLogicalVersion version) {
        final CustomSerializer customSerializer = customSerializers.get(c);
        return customSerializer != null && customSerializer.supports(version) ? customSerializer : null;
    }

    public <T extends Externalizable> void registerCustomSerializer(Class<T> c, CustomSerializer<T> serializer) {
        if (logger.isDebugEnabled())
            logger.debug("Registering custom serializer " + c.getName() + " => " + serializer.getClass().getName());
        customSerializers.put(c, serializer);
    }

    public XapRuntimeReporter getXapRuntimeReporter() {
        return xapRuntimeReporter;
    }

    public void registerXapRuntimeReporter(XapRuntimeReporter reporter) {
        xapRuntimeReporter = reporter;
    }
}

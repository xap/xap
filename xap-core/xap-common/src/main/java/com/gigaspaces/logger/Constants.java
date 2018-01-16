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

package com.gigaspaces.logger;

public interface Constants {
    //administrative
    final public static String LOGGER_EXCEPTIONS = "com.gigaspaces.exceptions";

    //MANAGEMENT
    final public static String LOGGER_ADMIN = "com.gigaspaces.admin";
    final public static String LOGGER_CLI = "com.gigaspaces.admin.cli";
    final public static String LOGGER_UI = "com.gigaspaces.admin.ui";


    final public static String LOGGER_WEB_UI_ADMIN = "com.gigaspaces.webui.admin";
    final public static String LOGGER_WEB_UI_REST = "com.gigaspaces.webui.rest";
    final public static String LOGGER_WEB_UI_COMMON = "com.gigaspaces.webui.common";
    final public static String LOGGER_WEB_UI_LIFECYCLE = "com.gigaspaces.webui.lifecycle";
    final public static String LOGGER_WEB_UI_SECURITY = "com.gigaspaces.webui.security";

    final public static String LOGGER_WEB_UI_STATISTICS_PU_INSTANCE = "com.gigaspaces.webui.statistics.puinstance";
    final public static String LOGGER_WEB_UI_STATISTICS_SPACE_INSTANCE = "com.gigaspaces.webui.statistics.spaceinstance";
    final public static String LOGGER_WEB_UI_STATISTICS_VM = "com.gigaspaces.webui.statistics.vm";
    final public static String LOGGER_WEB_UI_STATISTICS_OS = "com.gigaspaces.webui.statistics.os";
    final public static String LOGGER_WEB_UI_SPACE_MODE_CHANGE = "com.gigaspaces.webui.spacemode";
    final public static String LOGGER_WEB_UI_ALERTS = "com.gigaspaces.webui.alerts";
    final public static String LOGGER_WEB_UI_PU_EVENTS = "com.gigaspaces.webui.pu.events";
    final public static String LOGGER_WEB_UI_PUI_STATUS_EVENTS = "com.gigaspaces.webui.pui.status.events";
    final public static String LOGGER_WEB_UI_ELASTIC_EVENTS = "com.gigaspaces.webui.elastic.events";
    final public static String LOGGER_WEB_UI_RUNTIME_INFO = "com.gigaspaces.webui.runtimeinfo";
    final public static String LOGGER_WEB_UI_STATISTICS_PROVIDER = "com.gigaspaces.webui.statistics.provider";
    final public static String LOGGER_WEB_UI_REMOTE_ACTIVITIES = "com.gigaspaces.webui.remote.activities";

    final public static String LOGGER_CLUSTER_VIEW = "com.gigaspaces.admin.ui.cluster.view";
    final public static String LOGGER_SPACEBROWSER = "com.gigaspaces.admin.ui.spacebrowser";
    final public static String LOGGER_JMX = "com.gigaspaces.jmx";

    //Server
    final public static String LOGGER_SERVICE = "com.gigaspaces.service";
    final public static String LOGGER_CACHE = "com.gigaspaces.cache";
    final public static String LOGGER_DIRECT_PERSISTENCY = "com.gigaspaces.direct_persistency";
    final public static String LOGGER_CLASSLOADERS_CACHE = "com.gigaspaces.core.classloadercache";
    final public static String LOGGER_CLASSLOADERS_CLEANER = "com.gigaspaces.core.classloadercleaner";
    final public static String LOGGER_FILTERS = "com.gigaspaces.filters";
    final public static String LOGGER_LRMI = "com.gigaspaces.lrmi";
    final public static String LOGGER_LRMI_FILTERS = LOGGER_LRMI + ".filters";
    final public static String LOGGER_LRMI_STUB_CACHE = LOGGER_LRMI + ".stubcache";
    final public static String LOGGER_LRMI_CONTEXT = LOGGER_LRMI + ".context";
    final public static String LOGGER_LRMI_MARSHAL = LOGGER_LRMI + ".marshal";
    final public static String LOGGER_LRMI_EXPORTER = LOGGER_LRMI + ".exporter";
    final public static String LOGGER_LRMI_WATCHDOG = LOGGER_LRMI + ".watchdog";
    final public static String LOGGER_LRMI_SLOW_COMSUMER = LOGGER_LRMI + ".slow_consumer";
    final public static String LOGGER_LRMI_CLASSLOADING = LOGGER_LRMI + ".classloading";
    final public static String LOGGER_LRMI_COMMUNICATION_MANAGER = LOGGER_LRMI + ".communication.manager";
    final public static String LOGGER_LRMI_COMMUNICATION_TRANSPORT = LOGGER_LRMI + ".communication.transport";
    final public static String LOGGER_LRMI_CHANNEL_TRANSPORT = LOGGER_LRMI + ".channel.transport";
    final public static String LOGGER_LRMI_CHANNEL_MANAGER = LOGGER_LRMI + ".channel.manager";
    final public static String LOGGER_LRMI_CHANNEL_PROTOCOL = LOGGER_LRMI + ".channel.protocol";
    final public static String LOGGER_XA = "com.gigaspaces.core.xa";
    final public static String LOGGER_JCA = "com.gigaspaces.jca";
    final public static String LOGGER_PERSISTENT = "com.gigaspaces.persistent";
    final public static String LOGGER_PERSISTENT_SHARED_ITERATOR = LOGGER_PERSISTENT + ".shared_iterator";
    final public static String LOGGER_QUERY = "com.gigaspaces.query";
    final public static String LOGGER_JMS = "com.gigaspaces.jms";
    final public static String LOGGER_KERNEL = "com.gigaspaces.kernel";
    final public static String LOGGER_WORKER = "com.gigaspaces.worker";
    final public static String LOGGER_MULTICAST_WORKER = "com.gigaspaces.worker.multicast";
    final public static String LOGGER_SPRING = "com.gigaspaces.spring";
    final public static String LOGGER_METADATA = "com.gigaspaces.metadata";
    final public static String LOGGER_METADATA_POJO = "com.gigaspaces.metadata.pojo";
    final public static String LOGGER_CONTAINER = "com.gigaspaces.container";
    final public static String LOGGER_COMMON = "com.gigaspaces.core.common";
    final public static String LOGGER_CONFIG = "com.gigaspaces.core.config";
    final public static String LOGGER_SPACE = "com.gigaspaces.space";
    final public static String LOGGER_ENGINE = "com.gigaspaces.space.engine";
    final public static String LOGGER_ENGINE_OPERATIONS = "com.gigaspaces.space.operations";
    final public static String LOGGER_LEASE = "com.gigaspaces.core.lease";
    final public static String LOGGER_REFLECTION = "com.gigaspaces.core.reflection";
    final public static String LOGGER_LOOKUPMANAGER = "com.gigaspaces.core.lookupmanager";
    final public static String LOGGER_MEMORYMANAGER = "com.gigaspaces.memory-manager";
    final public static String LOGGER_NOTIFY = "com.gigaspaces.core.notify";
    final public static String LOGGER_FIFO = "com.gigaspaces.core.fifo";

    final public static String LOGGER_REPLICATION = "com.gigaspaces.replication";
    final public static String LOGGER_MIRROR_REPLICATION = LOGGER_REPLICATION + ".mirror";
    final public static String LOGGER_CLUSTER_ACTIVE_ELECTION = "com.gigaspaces.space.active-election";
    final public static String LOGGER_CLUSTER_ACTIVE_ELECTION_XBACKUP = "com.gigaspaces.space.active-election.xbackup";
    final public static String LOGGER_SPACE_TYPEMANAGER = "com.gigaspaces.space.typemanager";
    final public static String LOGGER_SPACE_TEMPLATE_SCANNER = "com.gigaspaces.space.templatescanner";

    final public static String LOGGER_REPLICATION_NODE = LOGGER_REPLICATION + ".node";
    final public static String LOGGER_REPLICATION_REPLICA = LOGGER_REPLICATION + ".replica";
    final public static String LOGGER_REPLICATION_GROUP = LOGGER_REPLICATION + ".group";
    final public static String LOGGER_REPLICATION_CHANNEL = LOGGER_REPLICATION + ".channel";
    final public static String LOGGER_REPLICATION_CHANNEL_VERBOSE = LOGGER_REPLICATION_CHANNEL + ".verbose";
    final public static String LOGGER_REPLICATION_ROUTER = LOGGER_REPLICATION + ".router";
    final public static String LOGGER_REPLICATION_ROUTER_COMMUNICATION = LOGGER_REPLICATION_ROUTER + ".communication";
    final public static String LOGGER_REPLICATION_BACKLOG = LOGGER_REPLICATION + ".backlog";

    final public static String LOGGER_REPLICATION_ENTRYPACKET_VERBOSE = LOGGER_REPLICATION + ".entrypacket.verbose";

    //.NET Bridge
    final public static String LOGGER_DISPATCHER_BRIDGE = "com.gigaspaces.bridge.dispatcher";
    final public static String LOGGER_DOTNET_PERSISTENCY = "com.gigaspaces.externaldatasource.dotnet";
    final public static String LOGGER_DOTNET_SPACE_FILTER = "com.gigaspaces.spacefilter.dotnet";
    final public static String LOGGER_PBS_EXECUTERS = "com.gigaspaces.bridge.pbsexecuter";

    //Client
    final public static String LOGGER_CLIENT = "com.gigaspaces.client";
    final public static String LOGGER_GSITERATOR = "com.gigaspaces.client.gsiterator";

    final public static String LOGGER_QUIESCE = "com.gigaspaces.quiesce";

    // Space Proxy
    final public static String LOGGER_SPACEPROXY_ROUTER = "com.gigaspaces.spaceproxy.router";
    final public static String LOGGER_SPACEPROXY_ROUTER_LOOKUP = "com.gigaspaces.spaceproxy.router.lookup";
    final public static String LOGGER_SPACEPROXY_DATA_EVENTS = "com.gigaspaces.spaceproxy.data_events";
    final public static String LOGGER_SPACEPROXY_DATA_EVENTS_LISTENER = "com.gigaspaces.spaceproxy.data_events.listener";

    // Metrics
    final public static String LOGGER_METRICS_MANAGER = "com.gigaspaces.metrics.manager";
    final public static String LOGGER_METRICS_REGISTRY = "com.gigaspaces.metrics.registry";
    final public static String LOGGER_METRICS_SAMPLER = "com.gigaspaces.metrics.sampler";

    //C++
    final public static String LOGGER_CPP_PROXY = "com.gigaspaces.cpp.proxy";

    /**
     * SpaceURL, SpaceValidator, SpaceURLParser logger
     */
    final public static String LOGGER_SPACE_URL = "com.gigaspaces.common.spaceurl";
    /**
     * SpaceFinder, CacheFinder related logger
     */
    final public static String LOGGER_SPACEFINDER = "com.gigaspaces.common.spacefinder";
    /**
     * LookupFinder related logger. When a cluster node is not found or when a jini:// SpaceFinder
     * lookup is used.
     */
    final public static String LOGGER_LOOKUPFINDER = "com.gigaspaces.common.lookupfinder";
    /**
     * ResourceLoader related logger. Set to FINE/FINEST when resources where not loaded or when
     * suspect in class loader issues impact the configuration setting. resources are space,
     * container, cluster, properties, security etc. configuration and jar files
     */
    final public static String LOGGER_RESOURCE_LOADER = "com.gigaspaces.common.resourceloader";


    //Spring
    final public static String LOGGER_SPRING_INTEGRATION = "org.springmodules.javaspaces.gigaspaces.SpringTracer";

    final public static String LOGGER_LOCAL_VIEW = "com.gigaspaces.localview";
    final public static String LOGGER_LOCAL_CACHE = "com.gigaspaces.localcache";

    public static final String LOGGER_DEV = "com.gigaspaces.dev";
}

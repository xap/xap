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

package com.gigaspaces.internal.client.spaceproxy.events;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.ManagedRemoteEventListener;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.events.batching.LRMIBatchNotifyDelegatorListener;
import com.gigaspaces.events.lease.EventLeaseRenewalManager;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.NotifyProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntriesListenerSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntriesListenerSpaceOperationResult;
import com.gigaspaces.internal.lease.SpaceNotifyLease;
import com.gigaspaces.internal.lrmi.stubs.LRMINotifyDelegatorListener;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.GenericExporter;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEventListener;
import net.jini.export.Exporter;

import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyDataEventsManager {
    private final Logger _logger;
    private final SpaceProxyImpl _spaceProxy;
    private final MultiplexDataEventListener _multiplexListener;
    private final EventLeaseRenewalManager _renewalManager;
    private final GenericExporter _exporter;
    private final ITransportConfig _remoteTransportConfiguration;
    private final ExecutorService _threadPool;
    /**
     * should handle slow consumers --> the connection is non blocking to RemoteEventListener. NOTE:
     * slow-consumer is working only with {@link GenericExporter} & {@link
     * com.gigaspaces.config.lrmi.nio.NIOConfiguration}
     */
    private final boolean _slowConsumer;

    public SpaceProxyDataEventsManager(SpaceProxyImpl spaceProxy, ITransportConfig remoteTransportConfig) {
        this._logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEPROXY_DATA_EVENTS + '.' + spaceProxy.getName());
        this._spaceProxy = spaceProxy;
        this._multiplexListener = new MultiplexDataEventListener();
        Exporter exporter = ServiceConfigLoader.getExporter();
        if (!(exporter instanceof GenericExporter))
            throw new IllegalArgumentException("Encountered usage of an Exporter which is not GenericExporter");

        _exporter = (GenericExporter) exporter;
        _remoteTransportConfiguration = remoteTransportConfig;
        _slowConsumer = _remoteTransportConfiguration.getSlowConsumerThroughput() > 0;
        _threadPool = _spaceProxy.getThreadPool();
        _renewalManager = new BatchLeaseRenewalManager(_spaceProxy, _logger);
    }

    public void close() {
        _renewalManager.close();
        _multiplexListener.close();
        _spaceProxy.getStubHandler().unexportObject(_multiplexListener);
    }

    public Logger getLogger() {
        return _logger;
    }

    public EventLeaseRenewalManager getRenewalManager() {
        return _renewalManager;
    }

    public EventRegistration addListener(Object template, long lease, NotifyInfo notifyInfo, boolean isMultiplex)
            throws RemoteException {
        try {
            _spaceProxy.beforeSpaceAction();

            final ITemplatePacket templatePacket = createTemplatePacket(template);
            if (!notifyInfo.isFifo()) {
                if (_spaceProxy.isFifo() || (templatePacket.getTypeDescriptor() != null && templatePacket.getTypeDescriptor().isFifoDefault()))
                    notifyInfo.setFifo(true);
            }

            wrapAndExportListener(notifyInfo, isMultiplex);

            final RegisterEntriesListenerSpaceOperationRequest request = new RegisterEntriesListenerSpaceOperationRequest(
                    templatePacket, notifyInfo, lease);
            _spaceProxy.getProxyRouter().execute(request);
            RegisterEntriesListenerSpaceOperationResult result = request.getFinalResult();
            result.processExecutionException();
            final GSEventRegistration eventRegistration = result.getEventRegistration();
            eventRegistration.setLease(new SpaceNotifyLease(
                    _spaceProxy,
                    templatePacket.getTypeName(),
                    ((LeaseContext) eventRegistration.getLease()).getUID(),
                    templatePacket.getRoutingFieldValue(),
                    eventRegistration.getLease().getExpiration(),
                    (ManagedRemoteEventListener) notifyInfo.getListener(),
                    eventRegistration));
            return eventRegistration;
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }
    }

    private ITemplatePacket createTemplatePacket(Object template) {
        NotifyProxyActionInfo actionInfo = new NotifyProxyActionInfo(_spaceProxy, template);

        if (actionInfo.isSqlQuery)
            return _spaceProxy.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, null);
        return actionInfo.queryPacket;
    }

    private void wrapAndExportListener(NotifyInfo info, boolean isMultiplex)
            throws ExportException {
        final String templateId = info.getOrInitTemplateUID();
        if (info.isFifo())
            info.setListener(new FifoDataEventListener(info.getListener(), _threadPool, info));

        ManagedRemoteEventListener listenerToExport;
        if (isMultiplex) {
            _multiplexListener.add(templateId, new SpaceProxyDataEventListener(
                    info.getListener(), _threadPool, _spaceProxy, null));
            listenerToExport = _multiplexListener;
        } else {
            listenerToExport = new SpaceProxyDataEventListener(info.getListener(), _threadPool, _spaceProxy, _exporter);
        }

        info.setListener(export(listenerToExport, info));
    }

    private ManagedRemoteEventListener export(ManagedRemoteEventListener remoteListener, NotifyInfo info)
            throws ExportException {
        ITransportConfig localLRMIConfig = _exporter.getConfiguration();
        ITransportConfig config = localLRMIConfig;

        if (_slowConsumer && _remoteTransportConfiguration instanceof NIOConfiguration) {
            //If slow consumer, we need to export using the remote space transport config
            config = _remoteTransportConfiguration.clone();
            //We need to export with the local lrmi bind port and bind host config and not the one received from the server
            //This is so ugly!
            ((NIOConfiguration) config).setBindHost(((NIOConfiguration) localLRMIConfig).getBindHostName());
            ((NIOConfiguration) config).setBindPort(((NIOConfiguration) localLRMIConfig).getBindPort());
            //FIXME: This only supports old LRMI, must be fixed if new lrmi will ever be used
            ((NIOConfiguration) config).setBlockingConnection(false);
        }

        RemoteEventListener rel = (RemoteEventListener) _exporter.export(remoteListener, config, false);

        return info.isBatching()
                ? new LRMIBatchNotifyDelegatorListener((BatchRemoteEventListener) remoteListener,
                (BatchRemoteEventListener) rel)
                : new LRMINotifyDelegatorListener(remoteListener, rel);
    }
}

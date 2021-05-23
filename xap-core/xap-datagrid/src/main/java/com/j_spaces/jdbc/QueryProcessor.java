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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.service.ServiceConfigLoader;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.export.Exporter;

import java.io.FileInputStream;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The QueryProcessor main class.
 *
 * @author Michael Mitrani - 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class QueryProcessor implements IQueryProcessor {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private static QueryProcessorConfiguration defaultConfig;

    private IJSpace _spaceCluster;
    private IJSpace _spaceRegular;

    // Internal handler that executes the JDBC
    private QueryHandler _queryHandler;
    private IQueryProcessor _stub;
    private Exporter _exporter;
    private final AtomicLong _sessionCounter = new AtomicLong();
    // client connections by the context
    private final Map<ConnectionContext, QuerySession> _sessions = new ConcurrentHashMap<ConnectionContext, QuerySession>();
    private final QueryProcessorConfiguration _config;
    private SecurityInterceptor securityInterceptor;
    private Transaction _transaction;

    public QueryProcessor(IJSpace clusterProxy, IJSpace spaceProxy, Properties overrideProperties, SecurityInterceptor securityInterceptor)
            throws Exception {
        this._spaceCluster = clusterProxy;
        this._spaceRegular = spaceProxy;
        this.securityInterceptor = securityInterceptor;
        this._config = new QueryProcessorConfiguration(SQLUtil.getAdmin(_spaceRegular).getConfig(), overrideProperties);
        setDefaultConfig(_config);
        init();
    }

    private synchronized void init() {
        _queryHandler = new QueryHandler(_spaceCluster, _spaceRegular, _config, securityInterceptor);
    }

    public void initStub() throws ExportException {
        _exporter = ServiceConfigLoader.getExporter();

        IQueryProcessor qpStub = (IQueryProcessor) _exporter.export(this);

        _stub = new QueryProcessorStub(this, qpStub);
    }

    public void close() throws RemoteException {

        if (_exporter != null)
            _exporter.unexport(true);

        if (_queryHandler != null)
            _queryHandler.close();

        if (_sessions != null)
            _sessions.clear();

        //help gc
        _spaceRegular = null;
        _spaceCluster = null;
        securityInterceptor = null;
    }

    @Override
    public ResponsePacket executeQuery(RequestPacket request, ConnectionContext context)
            throws RemoteException, SQLException {
        try {
            if (context == null)
                throw new SQLException("Invalid connection context.");

            QuerySession session = _sessions.get(context);
            if (session == null)
                throw new SQLException("Invalid connection context.");

            if (session.getTransaction() == null)
                session.setTransaction(_transaction);


            // update session connection context
            session.setConnectionContext(context);

            session.setModifiers(request.getModifiers());

            ResponsePacket response = null;
            if (_config.isTraceExecTime()) {

                long startTime = System.currentTimeMillis();

                response = request.accept(_queryHandler, session);
                long queryTime = System.currentTimeMillis() - startTime;
                if (_logger.isInfoEnabled()) {
                    _logger.info("Statement=" + request.toString() + ", execution time=" + queryTime + " milliseconds.");
                }
            } else {
                response = request.accept(_queryHandler, session);
            }
            return response;
        } catch (LeaseDeniedException e) {
            throw new SQLException("Failed to execute query; Cause: " + e, e);
        } catch (TransactionException e) {
            throw new SQLException("Failed to execute query; Cause: " + e, e);
        } catch (AccessDeniedException e) {
            throw new SQLException("Failed to execute query; Cause: " + e, e);
        }
    }

    @Override
    public void setTransaction(Transaction transaction) throws RemoteException {
        _transaction = transaction;
    }

    @Override
    public boolean isAvailable() throws RemoteException {
        try {
            _spaceRegular.ping();
            return true;
        } catch (RemoteException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Connection to space failed.", e);
            return false;
        }
    }

    @Override
    public ConnectionContext newConnection() throws RemoteException {
        QuerySession session = new QuerySession(QuerySession.class.getSimpleName() + "_" + _sessionCounter.incrementAndGet());
        session.setAutoCommit(_config.isAutoCommit());
        _sessions.put(session.getConnectionContext(), session);
        return session.getConnectionContext();
    }

    @Override
    public QuerySession getSession(ConnectionContext context) throws RemoteException {
        return context == null ? null : _sessions.get(context);
    }

    @Override
    public void closeConnection(ConnectionContext context) throws RemoteException {
        _sessions.remove(context);
    }

    public IQueryProcessor getStub() {
        return _stub;
    }

    public static synchronized void setDefaultConfig(QueryProcessorConfiguration defaultConfig) {
        if (QueryProcessor.defaultConfig == null){
            QueryProcessor.defaultConfig = defaultConfig;
            if( _logger.isDebugEnabled()) {
                _logger.debug("~~~INITIALIZING of QueryProcessor.defaultConfig" +
                             ", DateTimeFormat=" + defaultConfig.getLocalDateTimeFormat() +
                             ", DateFormat=" + defaultConfig.getSqlDateFormat() +
                             ", TimeFormat=" + defaultConfig.getSqlTimeFormat());
            }
        }
    }

    public static QueryProcessorConfiguration getDefaultConfig() {
        if (defaultConfig == null)
            setDefaultConfig(new QueryProcessorConfiguration(null, new Properties()));
        return defaultConfig;
    }

    public void clean() throws RemoteException {
        if (_queryHandler != null)
            _queryHandler.close();

        _spaceCluster.getDirectProxy().directClean();
        _spaceRegular.getDirectProxy().directClean();
        init();
    }

    /**
     * Run standalone QP
     */
    public static void main(String[] args) throws Throwable {
        if (args.length != 1) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Usage: QueryProcessor <property file>");
            }
            System.exit(-1);
        }

        // load qp properties file
        Properties config = new Properties();
        FileInputStream fileIn = new FileInputStream(args[0]);
        config.load(fileIn);
        fileIn.close();

        QueryProcessorConfiguration qpConfig = new QueryProcessorConfiguration(null, config);

        IJSpace space = (IJSpace) SpaceFinder.find(qpConfig.getSpaceURL());
        IRemoteSpace remoteSpace = ((IDirectSpaceProxy) space).getRemoteJSpace();
        QueryProcessor qp = (QueryProcessor) QueryProcessorFactory.newInstance(space, remoteSpace, config);

        try {
            int listenPort = qpConfig.getListenPort();
            Registry registry = LocateRegistry.createRegistry(listenPort);
            qp.initStub();
            registry.bind(IQueryProcessor.QP_LOOKUP_NAME, qp._stub);
            _logger.info("QueryProcessor started on port:" + listenPort);
        } catch (Throwable e) {
            qp.close();
            throw e;
        }

        // keep the server alive
        Object keepAlive = new Object();

        synchronized (keepAlive) {
            try {
                keepAlive.wait();
            } catch (InterruptedException e) {

            }
        }
    }
}

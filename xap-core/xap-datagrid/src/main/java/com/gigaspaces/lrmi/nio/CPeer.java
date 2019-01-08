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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.async.SettableFuture;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.exception.lrmi.ApplicationException;
import com.gigaspaces.exception.lrmi.LRMINoSuchObjectException;
import com.gigaspaces.exception.lrmi.LRMIUnhandledException;
import com.gigaspaces.exception.lrmi.LRMIUnhandledException.Stage;
import com.gigaspaces.exception.lrmi.ProtocolException;
import com.gigaspaces.internal.backport.java.util.concurrent.atomic.LongAdder;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.lrmi.ConnectionUrlDescriptor;
import com.gigaspaces.internal.lrmi.LRMIMonitoringModule;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.*;
import com.gigaspaces.lrmi.LRMIInvocationContext.InvocationStage;
import com.gigaspaces.lrmi.classloading.*;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.HandshakeRequest;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.gigaspaces.lrmi.netty.NettyChannel;
import com.gigaspaces.lrmi.nio.async.AsyncContext;
import com.gigaspaces.lrmi.nio.async.FutureContext;
import com.gigaspaces.lrmi.nio.async.LRMIFuture;
import com.gigaspaces.lrmi.nio.filters.IOBlockFilterManager;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.gigaspaces.lrmi.nio.selector.handler.client.ClientConversationRunner;
import com.gigaspaces.lrmi.nio.selector.handler.client.ClientHandler;
import com.gigaspaces.lrmi.rdma.*;
import com.gigaspaces.lrmi.tcp.TcpChannel;
import com.j_spaces.kernel.SystemProperties;
import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.RDMA_SYNC_OP_TIMEOUT;


/**
 * CPeer is the LRMI over NIO Client Peer.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class CPeer extends BaseClientPeer {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    private static final Logger _contextLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_CONTEXT);


    // should the thread name be changed to include socket information during sychonous invocations
    private static final boolean CHANGE_THREAD_NAME_ON_INVOCATION = Boolean.getBoolean("com.gs.lrmi.change.thread.name");

    private static final LongAdder connections = new LongAdder();

    private long _generatedTraffic;
    private long _receivedTraffic;

    private final AtomicBoolean disconnected = new AtomicBoolean(false);
    /**
     * Dummy method instance - used for sending dummy requests
     */
    final private static LRMIMethod _dummyMethod;

    static {
        try {
            _dummyMethod = new LRMIMethod(ReflectionUtil.createMethod(CPeer.class.getMethod("sendKeepAlive")), false, false, false, false, false, false, false, -1);
        } catch (Exception e) {
            throw new RuntimeException("InternalError: Failed to reflect sendKeepAlive() method.", e);
        }
    }


    private LrmiChannel _channel;
    private final RequestPacket _requestPacket;
    private final IRemoteClassProviderProvider _remoteConnection = new ClientRemoteClassProviderProvider();
    private long _objectClassLoaderId;
    private long _remoteLrmiRuntimeId;
    private LRMIRemoteClassLoaderIdentifier _remoteClassLoaderIdentifier;

    // WatchedObject are used to monitor the CPeer connection to the server
    private ClientPeerWatchedObjectsContext _watchdogContext;

    private ITransportConfig _config;
    private InetSocketAddress m_Address;
    private final ClientHandler _handler;
    private final ClientConversationRunner clientConversationRunner;
    final private PlatformLogicalVersion _serviceVersion;

    @SuppressWarnings("FieldCanBeLocal")
    private IOFilterManager _filterManager;
    private final Object _closedLock = new Object();
    private volatile boolean _closed;
    private boolean _protocolValidationEnabled;

    final private LRMIMonitoringModule _monitoringModule = new LRMIMonitoringModule();

    final private Object _memoryBarrierLock = new Object();
    // Not volatile for the following reason:
    // right after setting this value during async invocation, we add the context for selector registration
    // in ClientHander and it is added to a concurrent linked list there, so the assumption is the this value
    // is flushed to main memory, in the watch dog request timeout observer, the call to getAsyncContext will
    // go through volatile read
    private AsyncContext _asyncContext = null;
    private boolean _asyncConnect;

    private final boolean isNetty = RdmaConstants.NETTY_ENABLED;

    public static LongAdder getConnectionsCounter() {
        return connections;
    }

    public CPeer(PAdapter pAdapter, ClientHandler handler, ClientConversationRunner clientConversationRunner, PlatformLogicalVersion serviceVersion) {
        super(pAdapter);
        this._handler = handler;
        this.clientConversationRunner = clientConversationRunner;
        _serviceVersion = serviceVersion;
        _requestPacket = new RequestPacket();
        connections.increment();
    }

    /*
    * @see com.j_spaces.kernel.lrmi.ClientPeer#init(com.gigaspaces.transport.ITransportConfig)
    */
    public void init(ITransportConfig config) {
        _config = config;
        _protocolValidationEnabled = ((NIOConfiguration) config).isProtocolValidationEnabled();
        _asyncConnect = System.getProperty(SystemProperties.LRMI_USE_ASYNC_CONNECT) == null || Boolean.getBoolean(SystemProperties.LRMI_USE_ASYNC_CONNECT);
    }

    @Override
    public LRMIMonitoringModule getMonitoringModule() {
        return _monitoringModule;
    }

    public synchronized void connect(String connectionURL, LRMIMethod lrmiMethod) throws MalformedURLException, RemoteException {
        if (_asyncConnect && IOBlockFilterManager.getFilterFactory() == null && _config.getSlowConsumerThroughput() == 0 && clientConversationRunner != null) {
            connectAsync(connectionURL, lrmiMethod);
        } else {
            connectSync(connectionURL, lrmiMethod);
        }
    }

    //Flush the local members to the main memory once done
    public synchronized void connectAsync(String connectionURL, LRMIMethod lrmiMethod) throws MalformedURLException, RemoteException {
        synchronized (_closedLock) {
            if (_closed)
                DynamicSmartStub.throwProxyClosedExeption(connectionURL);
        }

        if (_logger.isDebugEnabled())
            detailedLogging("CPeer.connect", "trying to connect using connection url [" + connectionURL + "], calling method [" + lrmiMethod.realMethod.getDeclaringClass().getSimpleName() + "." + lrmiMethod.realMethod.getName() + "]");

        // parse connection URL
        ConnectionUrlDescriptor connectionUrlDescriptor = ConnectionUrlDescriptor.fromUrl(connectionURL);

        _objectClassLoaderId = connectionUrlDescriptor.getObjectClassLoaderId();
        _remoteLrmiRuntimeId = connectionUrlDescriptor.getLrmiRuntimeId();

        _remoteClassLoaderIdentifier = new LRMIRemoteClassLoaderIdentifier(_remoteLrmiRuntimeId, _objectClassLoaderId);

        // connect to server
        try {
            createChannel(connectionUrlDescriptor, true, lrmiMethod);

            // save connection URL
            setConnectionURL(connectionURL);

            // save object ID
            setObjectId(connectionUrlDescriptor.getObjectId());

            // Add the CPeer to the watchdog
            // The watched object doesn't contain specific thread.
            // The thread is set by the CPeer in invoke
            _watchdogContext = new ClientPeerWatchedObjectsContext(this);

        } catch (Exception ex) {
            disconnect();
            throw new java.rmi.ConnectException("Connect Failed to [" + connectionURL + "]", ex);
        }

        // mark state as 'connected'
        setConnected(true);
        _watchdogContext.watchIdle();
    }

    private void createChannel(ConnectionUrlDescriptor connection, boolean async, LRMIMethod lrmiMethod) throws IOException {
        ServerAddress address = LRMIRuntime.getRuntime().getNetworkMapper().map(new ServerAddress(connection.getHostname(), connection.getPort()));
        if (_channel != null) {
            _generatedTraffic += _channel.getWriter().getGeneratedTraffic();
            _receivedTraffic += _channel.getReader().getReceivedTraffic();
        }
        if (isNetty) {
            _channel = NettyChannel.create(address);
            m_Address = null;//((RdmaChannel) _channel).getEndpoint();
        } else {
            _channel = async ? TcpChannel.createAsync(address, _config, lrmiMethod, clientConversationRunner) : TcpChannel.createSync(address, _config);
            m_Address = ((TcpChannel) _channel).getEndpoint();
        }
    }

    //Flush the local members to the main memory once done
    public synchronized void connectSync(String connectionURL, LRMIMethod lrmiMethod) throws MalformedURLException, RemoteException {
        synchronized (_closedLock) {
            if (_closed)
                DynamicSmartStub.throwProxyClosedExeption(connectionURL);
        }

        if (_logger.isDebugEnabled())
            detailedLogging("CPeer.connect", "trying to connect using connection url [" + connectionURL + "], calling method [" + lrmiMethod.realMethod.getDeclaringClass().getSimpleName() + "." + lrmiMethod.realMethod.getName() + "]");

        // parse connection URL
        ConnectionUrlDescriptor connectionUrlDescriptor = ConnectionUrlDescriptor.fromUrl(connectionURL);

        _objectClassLoaderId = connectionUrlDescriptor.getObjectClassLoaderId();
        _remoteLrmiRuntimeId = connectionUrlDescriptor.getLrmiRuntimeId();

        _remoteClassLoaderIdentifier = new LRMIRemoteClassLoaderIdentifier(_remoteLrmiRuntimeId, _objectClassLoaderId);

        // connect to server
        try {
            createChannel(connectionUrlDescriptor, false, null);

            // save connection URL
            setConnectionURL(connectionURL);

            // save object ID
            setObjectId(connectionUrlDescriptor.getObjectId());

            // Add the CPeer to the watchdog
            // The watched object doesn't contain specific thread.
            // The thread is set by the CPeer in invoke
            _watchdogContext = new ClientPeerWatchedObjectsContext(this);

            if (_protocolValidationEnabled) {
                validateProtocol();
            }

            try {
                _filterManager = IOBlockFilterManager.createFilter(_channel.getReader(), _channel.getWriter(), true, _channel.getSocketChannel());
            } catch (Exception e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to load communication filter " + System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY), e);
                throw new InternalSpaceException("Failed to load communication filter " + System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY), e);
            }
            if (_filterManager != null) {
                try {
                    _filterManager.beginHandshake();
                } catch (Exception e) {
                    throw new ConnectException("Failed to perform communication filter handshake using " + System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY) + " filter", e);
                }
            }
        } catch (Exception ex) {
            disconnect();
            throw new java.rmi.ConnectException("Connect Failed to [" + connectionURL + "]", ex);
        }

        // mark state as 'connected'
        setConnected(true);

        try {
            doHandshake(lrmiMethod);
        } catch (Exception ex) {
            disconnect();
            throw new java.rmi.ConnectException("Connect Failed to [" + connectionURL + "], handshake failure", ex);
        }

        _watchdogContext.watchIdle();
    }

    private void validateProtocol() throws IOException {
        _watchdogContext.watchRequest("protocol-validation");
        try {
            _channel.getWriter().writeProtocolValidationHeader();
        } finally {
            _watchdogContext.watchNone();
        }
    }

    private void detailedLogging(String methodName, String description) {
        if (_logger.isDebugEnabled()) {
            String localAddress = "not connected";
            if (_channel != null) {
                //Avoid possible NPE if socket gets disconnected
                SocketAddress localSocketAddress = _channel.getLocalSocketAddress();
                //Avoid possible NPE if socket gets disconnected
                if (localSocketAddress != null)
                    localAddress = localSocketAddress.toString();
            }
            _logger.debug("At " + methodName + " method, " + description + " [invoker address=" + localAddress + ", ServerEndPoint=" + getConnectionURL() + "]");
        }
        if (_logger.isTraceEnabled()) {
            _logger.trace("At " + methodName + ", thread stack:" + StringUtils.NEW_LINE + StringUtils.getCurrentStackTrace());
        }
    }

    //This closes the underline socket and change the socket state to closed, we cannot do disconnect logic
    //as we may not acquire the lock for the cpeer and we can have concurrent thread using this cpeer for invocation
    @Override
    public void close() {
        synchronized (_closedLock) {
            if (_closed)
                return;

            _closed = true;

            closeSocketAndUnregisterWatchdog();
        }
    }

    @Override
    protected boolean isClosed() {
        return _closed;
    }

    public void disconnect() {
        if (disconnected.compareAndSet(false, true)) {
            connections.decrement();
        }
        closeSocketAndUnregisterWatchdog();

        LrmiChannel channel = _channel;
        if (channel != null) {
            if (channel.getWriter() != null) {
                //Clear the context the inner streams hold upon disconnection
                channel.getWriter().getSerializer().closeContext();
                _generatedTraffic += channel.getWriter().getGeneratedTraffic();
            }
            if (channel.getReader() != null) {
                //Clear the context the inner streams hold upon disconnection
                channel.getReader().closeContext();
                _receivedTraffic += channel.getReader().getReceivedTraffic();
            }
        }
        _channel = null;
        setConnected(false);
    }

    private void closeSocketAndUnregisterWatchdog() {
        try {
            if (_channel != null)
                _channel.close();
        } catch (Exception ex) {
            // nothing todo
            if (_logger.isDebugEnabled())
                _logger.debug("Failed to disconnect from " + getConnectionURL(), ex);

        } finally {
            if (_watchdogContext != null)
                _watchdogContext.close();
        }
    }

    public Reader getReader() {
        return _channel != null ? _channel.getReader() : null;
    }

    public LrmiChannel getChannel() {
        return _channel;
    }

    public Writer getWriter() {
        return _channel != null ? _channel.getWriter() : null;
    }

    public void afterInvoke() {
        _watchdogContext.watchIdle();
        _requestPacket.clear();
    }

    public IClassProvider getClassProvider() {
        return getProtocolAdapter().getClassProvider();
    }


    private class ClientRemoteClassProviderProvider implements IRemoteClassProviderProvider {
        private LRMIRemoteClassLoaderIdentifier _remoteClassLoaderIdentifier;

        public synchronized IClassProvider getClassProvider() throws IOException, IOFilterException {
            try {
                RequestPacket requestForClass = new RequestPacket(new ClassProviderRequest());
                _channel.getWriter().writeRequest(requestForClass);
                ReplyPacket<IClassProvider> response = _channel.getReader().readReply(true);

                return response.getResult();
            } catch (ClassNotFoundException e) {
                final IOException exp = new IOException();
                exp.initCause(e);
                throw exp;
            }
        }


        public LRMIRemoteClassLoaderIdentifier getRemoteClassLoaderIdentifier() {
            return _remoteClassLoaderIdentifier;
        }

        public LRMIRemoteClassLoaderIdentifier setRemoteClassLoaderIdentifier(LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier) {
            LRMIRemoteClassLoaderIdentifier prev = _remoteClassLoaderIdentifier;
            _remoteClassLoaderIdentifier = remoteClassLoaderIdentifier;
            return prev;
        }
    }

    private void doHandshake(LRMIMethod lrmiMethod) throws IOException, IOFilterException, ClassNotFoundException {
        RequestPacket requestPacket = new RequestPacket(new HandshakeRequest(PlatformLogicalVersion.getLogicalVersion()));
        requestPacket.operationPriority = getOperationPriority(lrmiMethod, LRMIInvocationContext.getCurrentContext());

        String previousThreadName = updateThreadNameIfNeeded();
        _watchdogContext.watchRequest("handshake");
        // read empty response
        try {
            _channel.getWriter().writeRequest(requestPacket);
            _watchdogContext.watchResponse("handshake");

            //In slow consumer we must read this in blocking mode
            if (_config.isBlockingConnection())
                _channel.getReader().readReply(0, 1000);
            else
                _channel.getReader().readReply(_config.getSlowConsumerLatency(), 1000);
        } catch (ClassNotFoundException e) {
            if (_logger.isErrorEnabled())
                _logger.error("unexpected exception occured at handshake sequence: [" + getConnectionURL() + "]", e);

            throw e;
        } finally {
            _watchdogContext.watchNone();
            restoreThreadNameIfNeeded(previousThreadName);
        }
    }

    public static OperationPriority getOperationPriority(LRMIMethod lrmiMethod, LRMIInvocationContext currentContext) {
        if (lrmiMethod.isLivenessPriority && currentContext.isLivenessPriorityEnabled())
            return OperationPriority.LIVENESS;

        if (lrmiMethod.isMonitoringPriority)
            return OperationPriority.MONITORING;

        if (currentContext.isCustomPriorityEnabled())
            return OperationPriority.CUSTOM;

        return OperationPriority.REGULAR;
    }

    public Object invoke(Object proxy, LRMIMethod lrmiMethod, Object[] args, ConnectionPool connPool)
            throws ApplicationException, ProtocolException, RemoteException, InterruptedException {
        if (_logger.isDebugEnabled())
            detailedLogging("CPeer.invoke", "trying to invoke method [" + lrmiMethod.realMethod.getDeclaringClass().getSimpleName() + "." + lrmiMethod.realMethod.getName() + "]");

        LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
        if (_contextLogger.isDebugEnabled()) {
            LRMIInvocationTrace trace = currentContext.getTrace();
            if (trace != null) {
                trace = trace.setIdentifier(_channel.getCurrSocketDisplayString());
                currentContext.setTrace(trace);
            }
        }

        // If prop.update.thread.name == true we change the thread name
        // on sync operations
        String previousThreadName = null;

        //Put the current lrmi connection context while keeping the previous
        IRemoteClassProviderProvider previousConnection = LRMIConnection.putConnection(_remoteConnection);
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            long clientClassLoaderId = getClassProvider().putClassLoader(contextClassLoader);

            final boolean isCallBack = lrmiMethod.isCallBack || currentContext.isCallbackMethod();
            final OperationPriority priority = getOperationPriority(lrmiMethod, currentContext);
            _requestPacket.set(getObjectId(), lrmiMethod.orderId, args, lrmiMethod.isOneWay,
                    isCallBack, lrmiMethod, clientClassLoaderId, priority, _serviceVersion);

            final String monitoringId = Pivot.extractMonitoringId(_requestPacket);

            // register the thread with the request watchdog
            if (!isNetty)
                _watchdogContext.watchRequest(monitoringId);

            if (lrmiMethod.isAsync) {
                LRMIFuture result = (LRMIFuture) FutureContext.getFutureResult();
                if (result == null) {
                    result = new LRMIFuture(contextClassLoader);
                } else {
                    result.reset(contextClassLoader);
                }

                if (isNetty) {
                    CompletableFuture<ReplyPacket> future = ((NettyChannel) _channel).submit(_requestPacket);
                    final LRMIFuture finalResult = result;
                    future.whenComplete((replyPacket, throwable) -> {
                        if (throwable != null)
                            finalResult.setResult(throwable);
                        else
                            finalResult.setResultPacket(replyPacket);
                    });
                } else {
                    final AsyncContext ctx = new AsyncContext(connPool,
                            _handler,
                            _requestPacket,
                            result,
                            this,
                            _remoteConnection,
                            contextClassLoader,
                            _remoteClassLoaderIdentifier,
                            monitoringId,
                            _watchdogContext);
                    _asyncContext = ctx;
                    _channel.getWriter().setWriteInterestManager(ctx);
                    _handler.addChannel(_channel.getSocketChannel(), ctx);
                }
                FutureContext.setFutureResult(result);
                return null;
            }

            previousThreadName = updateThreadNameIfNeeded();
            CompletableFuture<ReplyPacket> nettyFuture = null;
            if (isNetty) {
                nettyFuture = ((NettyChannel) _channel).submit(_requestPacket);
            } else {
                _channel.getWriter().writeRequest(_requestPacket);
            }

            /** if <code>true</code> the client peer mode is one way, don't wait for reply */
            if (lrmiMethod.isOneWay) {
                if (!isNetty)
                    _monitoringModule.monitorActivity(monitoringId, _channel.getWriter(), _channel.getReader());
                return null;
            }

            //Update stage to CLIENT_RECEIVE_REPLY, no new snapshot is required
            LRMIInvocationContext.updateContext(null, null, InvocationStage.CLIENT_RECEIVE_REPLY, null, null, false, null, null);

            boolean hasMoreIntermidiateRequests = true;
            ReplyPacket replyPacket;
            // Put the class loader id of the remote object in thread local in case a there's a need
            // to load a remote class, we will use the class loader of the exported object
            LRMIRemoteClassLoaderIdentifier previousIdentifier = RemoteClassLoaderContext.set(_remoteClassLoaderIdentifier);
            try {
                if (isNetty) {
                    replyPacket = nettyFuture.get(10, TimeUnit.MILLISECONDS);
                } else {
                    replyPacket = new ReplyPacket();
                    while (hasMoreIntermidiateRequests) {
                        // read response
                        _watchdogContext.watchResponse(monitoringId);
                        _channel.getReader().readReply(replyPacket);
                        if (replyPacket.getResult() instanceof ClassProviderRequest) {
                            replyPacket.clear();
                            _watchdogContext.watchRequest(monitoringId);
                            _channel.getWriter().writeRequest(new RequestPacket(getClassProvider()), false);
                        } else
                            hasMoreIntermidiateRequests = false;
                    }
                }
                // check for exception from server
                //noinspection ThrowableResultOfMethodCallIgnored
                if (replyPacket.getException() != null)
                    throw replyPacket.getException();

                return replyPacket.getResult();
            } finally {
                RemoteClassLoaderContext.set(previousIdentifier);
                if (!isNetty) {
                    _monitoringModule.monitorActivity(monitoringId, _channel.getWriter(), _channel.getReader());
                }
            }
        } catch (LRMIUnhandledException ex) {
            if (ex.getStage() == Stage.DESERIALIZATION) {
                if (_logger.isTraceEnabled())
                    _logger.debug("LRMI caught LRMIUnhandledException during deserialization stage at end point, reseting writer context.", ex);
                //We must reset the context because the other side have not completed reading the stream and therefore didn't
                //learn all the new context
                _channel.getWriter().getSerializer().resetContext();
            }
            if (_logger.isDebugEnabled())
                _logger.debug("LRMI caught LRMIUnhandledException, propogating it upwards.", ex);
            //Throw exception as is
            throw ex;
        } catch (NoSuchObjectException ex) {
            // broken connection
            disconnect();

            //noinspection UnnecessaryLocalVariable
            NoSuchObjectException detailedException = handleNoSuchObjectException(lrmiMethod, ex);
            throw detailedException;

        } catch (IOException ex) {
            // broken connection
            disconnect();

            String exMessage = "LRMI transport protocol over NIO broken connection with ServerEndPoint: [" + getConnectionURL() + "]";

            if (_logger.isDebugEnabled())
                _logger.debug(exMessage, ex);

            if (_watchdogContext.requestWatchHasException())
                throw new ConnectIOException(exMessage, _watchdogContext.getAndClearRequestWatchException());

            if (_watchdogContext.responseWatchHasException())
                throw new ConnectIOException(exMessage, _watchdogContext.getAndClearResponseWatchException());

            throw new ConnectException(exMessage, ex);
        } catch (MarshalContextClearedException ex) {
            // broken connection
            disconnect();

            String exMessage = "LRMI transport protocol over NIO broken connection with ServerEndPoint: [" + getConnectionURL() + "]";

            if (_logger.isDebugEnabled())
                _logger.debug(exMessage, ex);

            throw new RemoteException(exMessage, ex);
        } catch (Throwable ex) {
            /** no need to close connection on ApplicationException */
            if (ex instanceof ApplicationException)
                throw (ApplicationException) ex;

            String exMsg = "LRMI transport protocol over NIO connection [" + getConnectionURL() + "] caught unexpected exception: " + ex.toString();

            LogLevel logLevel = ex instanceof RuntimeException ? LogLevel.SEVERE : LogLevel.DEBUG;

            if (logLevel.isEnabled(_logger))
                logLevel.log(_logger, exMsg, ex);

            // broken connection
            disconnect();

            //noinspection ConstantConditions
            if (ex instanceof RemoteException)
                throw (RemoteException) ex;

            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;

            if (ex instanceof ProtocolException) {
                if (ex.getCause() instanceof NoSuchObjectException) {
                    NoSuchObjectException detailedException = handleNoSuchObjectException(lrmiMethod,
                            (NoSuchObjectException) ex.getCause());
                    throw new ProtocolException(exMsg, detailedException);
                }
                throw (ProtocolException) ex;
            }
            throw new ProtocolException(exMsg, ex);
        } finally {
            if (!lrmiMethod.isAsync) {
                // unregister the thread with the watchdog
                afterInvoke();
            }
            //Restore the original lrmi connection state
            LRMIConnection.setConnection(previousConnection);

            // Restore thread name in case it was changed
            restoreThreadNameIfNeeded(previousThreadName);
        }
    }

    private String updateThreadNameIfNeeded() {
        if (!CHANGE_THREAD_NAME_ON_INVOCATION)
            return null;

        String previousThreadName = Thread.currentThread().getName();
        String newThreadName = previousThreadName + "[" + _channel.getSocketDisplayString() + "]";
        Thread.currentThread().setName(newThreadName);
        return previousThreadName;
    }

    private void restoreThreadNameIfNeeded(String previousThreadName) {
        if (previousThreadName != null)
            Thread.currentThread().setName(previousThreadName);
    }

    private NoSuchObjectException handleNoSuchObjectException(
            LRMIMethod lrmiMethod, NoSuchObjectException ex) {
        if (_logger.isDebugEnabled())
            _logger.debug("LRMI made an attempt to invoke a method ["
                    + LRMIUtilities.getMethodDisplayString(lrmiMethod.realMethod)
                    + "] on an RemoteObject: [" + getConnectionURL()
                    + "]\n that no longer "
                    + " exists in the remote virtual machine.", ex);

        //noinspection UnnecessaryLocalVariable
        NoSuchObjectException detailedException = new LRMINoSuchObjectException("LRMI made an attempt to invoke a method ["
                + LRMIUtilities.getMethodDisplayString(lrmiMethod.realMethod)
                + "] on an RemoteObject: [" + getConnectionURL()
                + "] that no longer "
                + " exists in the remote virtual machine", ex);
        return detailedException;
    }

    public long getGeneratedTraffic() {
        Writer writer = getWriter();
        return _generatedTraffic + (writer != null ? writer.getGeneratedTraffic() : 0);
    }

    public long getReceivedTraffic() {
        Reader reader = getReader();
        return _receivedTraffic + (reader != null ? reader.getReceivedTraffic() : 0);
    }

    @Override
    public void disable() {
        disconnect();
    }

    /**
     * Sends a dummy one way request to the server - to check if it's alive
     *
     * @return <tt>true</tt> if keep alive was sent successfully
     */
    public boolean sendKeepAlive() {
        try {
            if (!isConnected())
                return false;

            // write request
            _requestPacket.set(LRMIRuntime.DUMMY_OBJECT_ID, 0, new Object[]{}, true, false, _dummyMethod, -1, OperationPriority.REGULAR, _serviceVersion);
            _channel.getWriter().writeRequest(_requestPacket);

            return true;
        } catch (Throwable t) {
            if (_logger.isDebugEnabled()) {
                String exMessage = "LRMI over NIO broken connection with ServerEndPoint: "
                        + getConnectionURL();
                _logger.debug(exMessage, t);
            }

            return false;
        }
    }

    public PlatformLogicalVersion getServiceVersion() {
        return _serviceVersion;
    }

    public AsyncContext getAsyncContext() {
        synchronized (_memoryBarrierLock) {
            return _asyncContext;
        }
    }

    public void clearAsyncContext() {
        _asyncContext = null;
    }

    // to be used as key to bucket in Watchdog#timeout();
    @Override
    public int hashCode() {
        if (m_Address != null)
            return m_Address.hashCode();
        return 0;
    }

    public String toString() {
        return getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    // to be used as key to bucket in Watchdog#timeout();
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CPeer))
            return false;
        //noinspection SimplifiableIfStatement
        if (m_Address != null) {
            return m_Address.equals(((CPeer) obj).m_Address);
        }
        return false;
    }

}

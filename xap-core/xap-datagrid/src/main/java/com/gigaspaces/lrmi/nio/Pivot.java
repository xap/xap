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

import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.exception.lrmi.LRMIUnhandledException;
import com.gigaspaces.exception.lrmi.LRMIUnhandledException.Stage;
import com.gigaspaces.exception.lrmi.ProtocolException;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.LongObjectMap;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.lrmi.LRMIInboundMonitoringDetailsImpl;
import com.gigaspaces.internal.lrmi.LRMIServiceMonitoringDetailsImpl;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderRunnable;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.*;
import com.gigaspaces.lrmi.LRMIInvocationContext.InvocationStage;
import com.gigaspaces.lrmi.LRMIInvocationContext.ProxyWriteType;
import com.gigaspaces.lrmi.ObjectRegistry.Entry;
import com.gigaspaces.lrmi.classloading.*;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.HandshakeRequest;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.gigaspaces.lrmi.nio.ChannelEntry.State;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.selector.SelectorManager;
import com.gigaspaces.lrmi.nio.selector.handler.ReadSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.WriteSelectorThread;
import com.gigaspaces.management.transport.ITransportConnection;
import com.j_spaces.kernel.ClassLoaderHelper;

import com.j_spaces.kernel.SystemProperties;
import org.jini.rio.boot.LoggableClassLoader;

import java.io.IOException;
import java.io.InvalidClassException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.UnmarshalException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pivot is the center of the NIO Protocol Adapter server-side implementation. It handles
 * channel (connection) management, thread allocation and request/reply transmission. <p/> The Pivot
 * runs several types: <ul> <li> SelectorThread: 1. Which listens on a port and accepts connection
 * requests. When a new connection is accepted, the connection is added to the table of open
 * connections. 2. Charge of selecting connections that have pending data (available for read).
 * </li> <li> A group of worker threads that can handle invocation requests.</li> </ul> <p/> The
 * interaction between connections, threads, requests and replies is as follows: <p/> Each channel
 * is associated with a SelectorThread. A SelectorThread manages all channels. SelectorThread is a
 * thread that listens for incoming Request Packet. When a Request Packet arrives at a channel,
 * SelectorThread builds a small bus packet that contains the channel info and dispatches it to the
 * worker threads' queue. An available worker thread receives the bus packet from the queue, reads
 * the Request Packet from the associated channel, performs the invocation (an up-call to the LRMI
 * Runtime), builds a Reply Packet from the result and sends it through the channel. <p/> A channel
 * is closed implicitly if an IO Exception is thrown when reading or writing to the channel (for
 * example, because the client socket is closed). <p/> A future enhancement is to support automatic
 * closing of channels that are inactive for a period of time. This also implies a keep-alive
 * mechanism.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class Pivot extends AbstractPivot {
    // logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    final private static Logger _contextLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_CONTEXT);

    private final IClassProvider _classProvider;

    public final static class ServerRemoteClassProviderProvider implements IRemoteClassProviderProvider {
        private final ChannelEntry channel;
        private LRMIRemoteClassLoaderIdentifier _remoteClassLoaderIdentifier;
        private IClassProvider _simpleClassProvider;

        public ServerRemoteClassProviderProvider(ChannelEntry channel) {
            this.channel = channel;
        }

        public synchronized IClassProvider getClassProvider() throws IOException, IOFilterException {
            if(isSimpleClassLoadingEnabled() && checkClientBackwardsCompatibility()){
                _logger.debug("Simple remote classloading is enabled, using SimpleClassProvider");
                if(_simpleClassProvider == null){
                    _simpleClassProvider = new SimpleClassProvider(channel);
                }
                return _simpleClassProvider;
            }
            else{
                try {
                    ReplyPacket requestForClass = new ReplyPacket(new ClassProviderRequest(), (Exception) null);
                    channel._writer.writeReply(requestForClass);
                    RequestPacket response = channel._reader.readRequest(true);

                    return (IClassProvider) response.getRequestObject();
                } catch (ClassNotFoundException e) {
                    final IOException exp = new IOException();
                    exp.initCause(e);
                    throw exp;
                }
            }
        }

        public LRMIRemoteClassLoaderIdentifier getRemoteClassLoaderIdentifier() {
            return _remoteClassLoaderIdentifier;
        }

        public LRMIRemoteClassLoaderIdentifier setRemoteClassLoaderIdentifier(LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier) {
            LRMIRemoteClassLoaderIdentifier result = _remoteClassLoaderIdentifier;
            _remoteClassLoaderIdentifier = remoteClassLoaderIdentifier;
            return result;
        }

        private boolean checkClientBackwardsCompatibility(){
            PlatformLogicalVersion clientPlatformLogicalVersion = channel.getSourcePlatformLogicalVersion();
            return clientPlatformLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v15_0_0);
        }

        private boolean isSimpleClassLoadingEnabled(){
            return Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_SIMPLE_CLASSLOADING, SystemProperties.LRMI_SIMPLE_CLASSLOADING_DEFAULT));
        }
    }

    private final static class ChannelEntryTask implements Runnable {
        final private Pivot pivot;
        final private ChannelEntry channelEntry;
        final private MarshalInputStream stream;

        private ChannelEntryTask(Pivot pivot, ChannelEntry channelEntry, MarshalInputStream stream) {
            this.pivot = pivot;
            this.channelEntry = channelEntry;
            this.stream = stream;
        }

        public void run() {
            try {
                setLRMIInvocationContext();

                // setting the threadlocal containing the connection back to the caller
                // in order to retrieve class provider from it if necessary.
                LRMIConnection.setConnection(channelEntry.getRemoteClassProvider());

                RequestPacket requestPacket = channelEntry.unmarshall(stream);

                if (requestPacket == null) {
                    channelEntry.returnSocket(); // releases Reader Selector
                } else {
                    try {
                        //Update stage once we finished unmarshaling the request
                        LRMIInvocationContext.updateContext(null, null, InvocationStage.INVOCATION_HANDLING, null, null, false, null, null);
                        if (_logger.isTraceEnabled())
                            _logger.trace("<-- " + requestPacket);
                        pivot.handleRequest(requestPacket, channelEntry);
                    } finally {
                        //During unmarshal of request packet, a lrmi remote class loader context is switched in case a remote class
                        //loading will be needed, after finished executing the method, the previous should be restored for recursive
                        //calls.
                        requestPacket.restorePreviousLRMIRemoteClassLoaderState();
                    }
                }

                // removing the back connection from the threadlocal.
                // requests for class definitions can be send only during the read phase of the request.
                LRMIConnection.clearConnection();
            } finally {
                //Reset context once the invocation is complete
                LRMIInvocationContext.resetContext();
            }
        }

        private void setLRMIInvocationContext() {
            LRMIInvocationTrace trace = _contextLogger.isDebugEnabled() ?
                    new LRMIInvocationTrace(null, null, NIOUtils.getSocketDisplayString(channelEntry.getSocketChannel()), false) : null;
            //We do not need a new snapshot because this is called by a new task which we control
            LRMIInvocationContext.updateContext(trace, ProxyWriteType.UNCACHED, InvocationStage.SERVER_UNMARSHAL_REQUEST, channelEntry.getSourcePlatformLogicalVersion(), null, false, null, channelEntry.getClientEndPointAddress());
        }
    }

    private static class ReplyTask extends ContextClassLoaderRunnable {
        private final Pivot pivot;
        private final ChannelEntry channel;
        private final ReplyPacket<?> packet;
        private final IResponseContext ctx;

        private ReplyTask(Pivot pivot, ChannelEntry channel, ReplyPacket<?> packet, IResponseContext ctx) {
            this.pivot = pivot;
            this.channel = channel;
            this.packet = packet;
            this.ctx = ctx;
        }

        @Override
        protected void execute() {
            try {
                LRMIInvocationContext.updateContext(ctx.getTrace(), ProxyWriteType.UNCACHED, InvocationStage.SERVER_MARSHAL_REPLY, ctx.getSourcePlatformLogicalVersion(), null, false, null, null);
                pivot.sendResponse(channel, packet, ctx);
            } finally {
                //Reset lrmi context
                LRMIInvocationContext.resetContext();
            }
        }
    }

    final private Map<SocketAddress, ChannelEntry> _clientToChannel;
    final private Map<SocketChannel, ChannelEntry> m_Channels;
    final private Executor _threadPool;
    final private Executor _livenessPriorityThreadPool;
    final private Executor _monitoringPriorityThreadPool;
    final private Executor _customThreadPool;
    final private SelectorManager _selectorManager;

    //default response handler used by the response context.
    final private DefaultResponseHandler _defaultResponseHandler = new DefaultResponseHandler();
    final private boolean _protocolValidationEnabled;

    final private SystemRequestHandler _systemRequestHandler = new SystemRequestHandlerImpl();

    public Pivot(NIOConfiguration config, ProtocolAdapter protocol)
            throws IOException {
        _classProvider = protocol.getClassProvider();
        _clientToChannel = new ConcurrentHashMap<SocketAddress, ChannelEntry>();
        m_Channels = new ConcurrentHashMap<SocketChannel, ChannelEntry>();
        _selectorManager = new SelectorManager(this, config.getBindHostName(),
                config.getBindPort(),
                config.getReadSelectorThreads());

        _threadPool = LRMIRuntime.getRuntime().getThreadPool();
        _livenessPriorityThreadPool = LRMIRuntime.getRuntime().getLivenessPriorityThreadPool();
        _monitoringPriorityThreadPool = LRMIRuntime.getRuntime().getMonitoringPriorityThreadPool();
        _customThreadPool = LRMIRuntime.getRuntime().getCustomThreadPool();

        _protocolValidationEnabled = config.isProtocolValidationEnabled();
    }

    @Override
    public void shutdown() {
        // shutdown the connection manager
        _selectorManager.requestShutdown();

        Collection<ChannelEntry> channelEntries = new LinkedList<ChannelEntry>(m_Channels.values()); //avoid concurrent modification
        for (ChannelEntry chEntry : channelEntries) {
            closeConnection(chEntry);
        }
    }

    public int getPort() {
        return _selectorManager.getPort();
    }

    public String getHostName() {
        return _selectorManager.getHostName();
    }

    public static boolean isMonitorActivity() {
        return LRMIRuntime.getRuntime().isMonitorActivity();
    }

    /**
     * send if possible the catch server exception to the client
     */
    public boolean handleExceptionFromServer(Writer writer, Reader reader, Throwable ex) {
        if (ex instanceof ClosedChannelException) {
            if (_logger.isDebugEnabled())
                _logger.debug("Connection with client closed from [" + writer.getEndPointAddress() + "] endpoint.");

            return true;
        }
        if (ex instanceof MarshalContextClearedException) {
            if (_logger.isDebugEnabled())
                _logger.debug("Marshal context have been cleared, probably because the exported service class loader has been unloaded, service incoming invocation from [" + writer.getEndPointAddress() + "] endpoint.");

            return true;
        }

        try {
            String msg = "LRMI Transport Protocol caught server exception caused by [" + writer.getEndPointAddress() + "] client.";

            if (ex instanceof LRMIUnhandledException) {
                if (_logger.isDebugEnabled())
                    _logger.debug(msg, ex);
                LRMIUnhandledException lrmiue = (LRMIUnhandledException) ex;
                if (lrmiue.getStage() == Stage.DESERIALIZATION) {
                    //If exception thrown during deserialization we must reset read context because the sending side could have sent us
                    //context that we didn't learn yet, the other side must reset its context as well
                    reader.resetContext();
                }
                //Write reply with the penetrating exception
                if (writer.isOpen())
                    writer.writeReply(new ReplyPacket(null, lrmiue));
                //Dont request close connection on this exception if upper layer permits                
                return false;
            } else if (ex instanceof RuntimeException || ex instanceof InvalidClassException) {
                if (_logger.isErrorEnabled())
                    _logger.error(msg, ex);
            } else if (ex instanceof UnmarshalException) {
                if (_logger.isWarnEnabled())
                    _logger.warn(msg, ex);
            } else {
                if (_logger.isDebugEnabled())
                    _logger.debug(msg, ex);
            }

            if (writer.isOpen())
                writer.writeReply(new ReplyPacket(null, new ProtocolException(msg, ex)));

            return true;

        } catch (Exception ex2) {
            if (_logger.isDebugEnabled())
                _logger.debug("Failed to send handledServerException to endpoint [" + writer.getEndPointAddress() + "] , the client disconnected from the server.", ex);
            return true;
        }
    }


    /**
     * Called by the ConnMgr thread when a new connection is created.
     */
    public ChannelEntry newConnection(ReadSelectorThread readHandler, SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();

        WriteSelectorThread writeHandler = _selectorManager.getWriteHandler(key.channel());

        Socket socket = channel.socket();
        InetSocketAddress socketAddress = (InetSocketAddress) (socket == null ? null : socket.getRemoteSocketAddress());
        ChannelEntry channelEntry = new ChannelEntry(writeHandler, readHandler, key, socketAddress, this);
        m_Channels.put(channel, channelEntry);

        if (socketAddress != null) {
            _clientToChannel.put(socketAddress, channelEntry);
        }

        if (_logger.isDebugEnabled())
            _logger.debug("Connected new client from [" + channelEntry.getClientEndPointAddress() + "] endpoint.");

        return channelEntry;
    }

    /**
     * Called by anyone who wants to close a connection.
     */
    public synchronized void closeConnection(ChannelEntry channelEntry) {
        if (_logger.isDebugEnabled())
            _logger.debug("Connection with client closed from [" + channelEntry.getClientEndPointAddress() + "] endpoint.");

        try {
            // cancel channel registration in its selector
            if (channelEntry.getReadSelectionKey() != null)
                channelEntry.getReadSelectionKey().cancel();

            // close channel and remove it from table
            // remove client socket mapping before closing the socket
            SocketChannel socketChannel = channelEntry.getSocketChannel();
            Socket socket = socketChannel.socket();
            if (socket != null) {
                SocketAddress socketAddress = socket.getRemoteSocketAddress();
                if (socketAddress != null)
                    _clientToChannel.remove(socketAddress);
            }
            channelEntry.close();
            m_Channels.remove(socketChannel);

        } catch (IOException ex) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(ex.toString(), ex);
            }
        }
    }

    public void handleProtocolValidation(ChannelEntry channelEntry, Reader.ProtocolValidationContext context,
                                         ReadSelectorThread handler) {
        boolean keepChannelOpen = channelEntry.readProtocolValidationHeader(context);
        if (!keepChannelOpen)
            closeConnection(channelEntry);
        else {
            if (channelEntry.isProtocolValidated())
                context.selectionKey.attach(null);

            handler.registerKey(context.selectionKey);
        }
    }

    /**
     * Called by a Reader Selector when it detects that a channel has pending data for read.
     *
     * @param channelEntry the channel representing the client connection.
     */
    public void handleReadRequest(ChannelEntry channelEntry, Reader.Context ctx, ReadSelectorThread handler) {
        MarshalInputStream stream = channelEntry.readRequest(ctx);
        if ((ctx.phase != Reader.Context.Phase.FINISH) ||
                (stream == null && !ctx.isSystemRequest())) {
            handler.registerKey(ctx.selectionKey);
            return;
        }

        // release current ctx before a class response could arrive
        // so that a new ctx will be created.
        ctx.selectionKey.attach(null);

        OperationPriority operationPriority;
        Runnable task;

        if (ctx.isSystemRequest()) {
            operationPriority = OperationPriority.MONITORING;
            task = ctx.systemRequestContext.getResponseTask(this, channelEntry, ctx.startTimestamp);
        } else {
            operationPriority = RequestPacket.getOperationPriorityFromBytes(ctx.bytes);
            task = new ChannelEntryTask(this, channelEntry, stream);
        }
        //We are using the selector thread indication of priority because it is safer because the channel system priority is not volatile
        executeAccordingToPriority(operationPriority, task);
    }

    public void requestPending(ChannelEntry channel, ReplyPacket<?> respPacket, IResponseContext responseContext) {
        // called by the service, should use its context class loader for invocation
        // This is a thread from the pool and it will have the channel system property state updated as it goes throw volatile
        executeAccordingToPriority(responseContext.getOperationPriority(), new ReplyTask(this, channel, respPacket, responseContext));
    }

    private void executeAccordingToPriority(OperationPriority operationPriority, Runnable command) {
        // dispatch Bus Packet to Worker Threads (called by read selector thread, context irrelevant)
        switch (operationPriority) {
            case CUSTOM:
                //Currently we have custom thread pool for space tasks and notifications, in the future we could have a named custom pool
                _customThreadPool.execute(command);
                break;
            case LIVENESS:
                _livenessPriorityThreadPool.execute(command);
                break;
            case MONITORING:
                _monitoringPriorityThreadPool.execute(command);
                break;
            case REGULAR:
                _threadPool.execute(command);
                break;
        }
    }


    /**
     * Called by a worker thread that has been assigned a request pending on a channel.
     *
     * @param requestPacket the request object
     * @param respContext   the response used for delayed responses
     * @return a reply packet.
     */
    private ReplyPacket consumeAndHandleRequest(RequestPacket requestPacket, IResponseContext respContext, ChannelEntry channelEntry) {
        Object reqObject = requestPacket.getRequestObject();
        if (reqObject != null) {
            if (reqObject instanceof ClassProviderRequest)
                return new ReplyPacket<IClassProvider>(_classProvider, null);
            if (reqObject instanceof HandshakeRequest) {
                HandshakeRequest handshakeRequest = (HandshakeRequest) reqObject;
                channelEntry.setSourceDetails(handshakeRequest.getSourcePlatformLogicalVersion(), handshakeRequest.getSourcePid());
                return new ReplyPacket<Object>(null, null);
            }
        }
        boolean sendResponse = true;
        Exception resultEx = null;
        Object result = null;
        try {
            /** if true, no reply to client */
            if (requestPacket.isOneWay()) {
                //One way method, return read interest here to allow this socket to accept next invocations since the client have already returned
                //the corresponding cpeer to the pool as it was not waiting for a response 
                //and we could have pending invocations already waiting in this socket incoming buffer.
                channelEntry.returnSocket();
                sendResponse = false;
            }

            // Check for dummy packets - ignore them
            if (requestPacket.getObjectId() != LRMIRuntime.DUMMY_OBJECT_ID) {
                if (requestPacket.getInvokeMethod() == null) {
                    _logger.warn("canceling invocation of request packet without invokeMethod : " + requestPacket);
                } else {
                    result = LRMIRuntime.getRuntime().invoked(requestPacket.getObjectId(),
                            requestPacket.getInvokeMethod().realMethod,
                            requestPacket.getArgs());
                }
            }
        } catch (NoSuchObjectException ex) {
            /** the remote object died, no reason to print-out the message in non debug mode,  */
            if (requestPacket.isOneWay()) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Failed to invoke one way method : " + requestPacket.getInvokeMethod() +
                            "\nReason: This remoteObject: " + requestPacket.getObjectId() +
                            " has been already unexported.", ex);
                }
            }

            resultEx = ex;
        } catch (Exception ex) {
            resultEx = ex;
        } finally {
            if (requestPacket.isCallBack) {
                sendResponse = respContext.shouldSendResponse();
            }
        }

        /** if true, no reply to client */
        if (requestPacket.isOneWay()) {
            if (resultEx != null && _logger.isErrorEnabled()) {
                _logger.error("Failed to invoke one-way method: " + requestPacket.getInvokeMethod(), resultEx);
            }
        }

        if (!sendResponse)
            return null;

        /** return response */
        //noinspection unchecked
        return new ReplyPacket(result, resultEx);
    }

    public ChannelEntry getChannelEntryFromChannel(SocketChannel channel) {
        return m_Channels.get(channel);
    }


    /**
     * @return the INetSocketAddress this Selector bind to.
     */
    public InetSocketAddress getServerBindInetSocketAddress() {
        return _selectorManager.getBindInetSocketAddress();
    }


    /**
     * Handles client request and sends reply to the client. Called in the following cases: 1. by
     * the ChannelThread when it detects that a channel has a pending two way request. 2. by the
     * Worker in case of a one way request processing.
     *
     * @param requestPacket the request
     * @param channelEntry  a channel(socket) wrapper
     */
    public void handleRequest(RequestPacket requestPacket, ChannelEntry channelEntry) {
        /* link channelEntry with remoteObjID, this gives us info which channelEntries open vs. remoteObjID */
        channelEntry.setOwnerRemoteObjID(requestPacket.getObjectId());

        IResponseContext respContext = null;
        String monitoringId = extractMonitoringId(requestPacket);
        if (requestPacket.isCallBack) {
            LRMIInvocationTrace trace = _contextLogger.isDebugEnabled() ? LRMIInvocationContext.getCurrentContext().getTrace() : null;
            respContext = new PivotResponseContext(this,
                    channelEntry,
                    _defaultResponseHandler,
                    LRMIInvocationContext.getCurrentContext().getSourceLogicalVersion(),
                    requestPacket.operationPriority,
                    monitoringId,
                    trace);
            ResponseContext.setExistingResponseContext(respContext);
        }

        ReplyPacket replyPacket = consumeAndHandleRequest(requestPacket, respContext, channelEntry);
        ResponseContext.clearResponseContext();

        //	 If replyPacket is null - it's a one way request or callback
        // return without sending reply to the client
        if (replyPacket == null) {
            if (isMonitorActivity())
                channelEntry.monitorActivity(monitoringId);
            return;
        }

        // in case of special requests (such as class request) a fresh MarshalledOutputStream
        // should be created to make sure class definition will be sent to be read by the
        // corresponding (fresh) MarshalledInputStream on the other side.
        boolean reuseBuffer = requestPacket.getRequestObject() == null;
        sendResponse(channelEntry, replyPacket, respContext, reuseBuffer, monitoringId);
    }

    public static String extractMonitoringId(RequestPacket requestPacket) {
        LRMIMethod lrmiMethod = requestPacket.getInvokeMethod();
        if (lrmiMethod == null)
            return null;

        if (!lrmiMethod.isCustomTracking)
            return lrmiMethod.realMethodString;

        Object[] args = requestPacket.getArgs();
        if (args.length < 1)
            return lrmiMethod.realMethodString;

        Object trackingParam = args[0];
        if (!(trackingParam instanceof LRMIMethodTrackingIdProvider))
            return lrmiMethod.realMethodString;

        return ((LRMIMethodTrackingIdProvider) trackingParam).getLRMIMethodTrackingId();
    }

    public void sendResponse(ChannelEntry channelEntry, ReplyPacket replyPacket, IResponseContext respContext) {
        sendResponse(channelEntry, replyPacket, respContext, true, respContext.getLRMIMonitoringId());
    }

    private void sendResponse(ChannelEntry channelEntry, ReplyPacket replyPacket,
                              IResponseContext respContext, boolean reuseBuffer, String monitoringId) {
        try {
            //Update state to marshal reply
            LRMIInvocationContext.updateContext(null, null, InvocationStage.SERVER_MARSHAL_REPLY, null, null, false, null, null);

            if (respContext != null && !respContext.markAsSentResponse()) {
                _logger.error("Trying to send a response twice using the same response context.");
                return;
            }

            // Send reply to the client
            LRMIInvocationTrace trace = respContext != null ? respContext.getTrace() : _contextLogger.isDebugEnabled() ? LRMIInvocationContext.getCurrentContext().getTrace() : null;
            Writer.Context ctx = new Writer.ChannelEntryContext(trace, channelEntry.getWriteExecutionPhaseListener());

            ObjectRegistry.Entry entry = LRMIRuntime.getRuntime().getRegistryObject(channelEntry.getRemoteObjID());
            ClassLoader orgThreadCL = Thread.currentThread().getContextClassLoader();
            ClassLoader marshalClassLoader = entry != null ? entry.getExportedThreadClassLoader() : orgThreadCL;

            final boolean changeCL = orgThreadCL != marshalClassLoader;
            if (changeCL)
                ClassLoaderHelper.setContextClassLoader(marshalClassLoader, true /*ignore security*/);
            try {
                channelEntry.writeReply(replyPacket, reuseBuffer, ctx, monitoringId);
            } finally {
                if (changeCL)
                    ClassLoaderHelper.setContextClassLoader(orgThreadCL, true /*ignore security*/);
            }
        } catch (Exception ex) {
            handleExceptionFromServer(channelEntry._writer, channelEntry._reader, ex);
        }
    }

    /**
     * @return all channels of supplied remote objectID
     */
    protected List<ChannelEntry> getChannels(long remoteObjID) {
        List<ChannelEntry> channelEntries = new LinkedList<ChannelEntry>(m_Channels.values()); //avoid concurrent modification
        Iterator<ChannelEntry> iter = channelEntries.iterator();
        while (iter.hasNext()) {
            /* only if this channelEntry linked with supplied remoteObjID */
            ChannelEntry chEntry = iter.next();
            if (chEntry.getRemoteObjID() != remoteObjID)
                iter.remove();
        }

        return channelEntries;
    }

    /**
     * unexport supplied remote objectID and close all channel sockets
     */
    public void unexport(long remoteObjID) {
        List<ChannelEntry> channelEntries = getChannels(remoteObjID);
        for (ChannelEntry chEntry : channelEntries)
            closeConnection(chEntry);
    }

    /**
     * @return the Transport connection info list for the supplied remote objectID.
     */
    public List<ITransportConnection> getRemoteObjectConnectionsList(long remoteObjID) {
        List<ITransportConnection> result = new ArrayList<ITransportConnection>();

        for (ChannelEntry channelEntry : m_Channels.values()) {
            if (channelEntry.getRemoteObjID() == remoteObjID) {
                InetSocketAddress clientEndPoint = getClientEndPointAddress(channelEntry);
                if (clientEndPoint != null) {
                    result.add(new NIOTransportConnection(remoteObjID, channelEntry.getConnectionID(), channelEntry.getConnectionTimeStamp(),
                            clientEndPoint.getAddress().getHostAddress(), clientEndPoint.getPort(),
                            getServerBindInetSocketAddress().getAddress().getHostAddress(), getServerBindInetSocketAddress().getPort()));
                }
            }
        }

        return result;
    }

    public int countRemoteObjectConnections(long remoteObjID) {
        int result = 0;

        for (ChannelEntry channelEntry : m_Channels.values())
            if (channelEntry.getRemoteObjID() == remoteObjID && getClientEndPointAddress(channelEntry) != null)
                result++;

        return result;
    }

    private InetSocketAddress getClientEndPointAddress(ChannelEntry channelEntry) {
        InetSocketAddress clientEndPoint = channelEntry.getClientEndPointAddress();
        if (clientEndPoint == null)
            return null;
        return clientEndPoint.getAddress() == null ? null : clientEndPoint;
    }

    public boolean isProtocolValidationEnabled() {
        return _protocolValidationEnabled;
    }

    public LRMIInboundMonitoringDetailsImpl getMonitoringDetails() {
        Collection<ChannelEntry> channels = m_Channels.values();
        LongObjectMap<LRMIServiceMonitoringDetailsImpl> servicesTrackingDetails = CollectionsFactory.getInstance().createLongObjectMap();
        for (ChannelEntry channelEntry : channels) {
            long remoteObjID = channelEntry.getRemoteObjID();
            Entry registryObject = LRMIRuntime.getRuntime().getRegistryObject(remoteObjID);
            if (registryObject == null)
                continue;
            Remote exportedService = registryObject.m_Object;
            LRMIServiceMonitoringDetailsImpl monitoringDetails = servicesTrackingDetails.get(remoteObjID);
            if (monitoringDetails == null) {
                String serviceDetails = getServiceDetails(exportedService);
                ClassLoader classLoader = registryObject.getExportedThreadClassLoader();
                String serviceClassLoaderDetails = (classLoader instanceof LoggableClassLoader) ? ((LoggableClassLoader) classLoader).getLogName() : classLoader.toString();
                ServerPeer serverPeer = registryObject.getServerPeer();
                String connectionUrl = serverPeer != null ? serverPeer.getConnectionURL() : null;
                monitoringDetails = new LRMIServiceMonitoringDetailsImpl(serviceDetails, serviceClassLoaderDetails, remoteObjID, connectionUrl);
                servicesTrackingDetails.put(remoteObjID, monitoringDetails);
            }
            monitoringDetails.addChannelDetails(channelEntry);
        }

        return new LRMIInboundMonitoringDetailsImpl(servicesTrackingDetails.getValues(new LRMIServiceMonitoringDetailsImpl[servicesTrackingDetails.size()]));
    }

    public SystemRequestHandler getSystemRequestHandler() {
        return _systemRequestHandler;
    }

    public State getChannelEntryState(SocketAddress monitoredClient) {
        ChannelEntry channelEntry = _clientToChannel.get(monitoredClient);
        if (channelEntry == null)
            return State.NONE;

        return channelEntry.getChannelState();
    }

    public static String getServiceDetails(Remote exportedService) {
        return exportedService.getClass().toString() + ((exportedService instanceof ILRMIService) ? "(" + ((ILRMIService) exportedService).getServiceName() + ")" : "");
    }

}

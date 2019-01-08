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

package com.gigaspaces.lrmi.nio.async;

import com.gigaspaces.exception.lrmi.LRMIUnhandledException;
import com.gigaspaces.exception.lrmi.LRMIUnhandledException.Stage;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.stubcache.MissingCachedStubException;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderRunnable;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ConnectionPool;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationContext.InvocationStage;
import com.gigaspaces.lrmi.LRMIInvocationContext.ProxyWriteType;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.classloading.*;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.gigaspaces.lrmi.nio.CPeer;
import com.gigaspaces.lrmi.nio.ClientPeerWatchedObjectsContext;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.RemoteClassLoaderContext;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.lrmi.nio.RequestPacket;
import com.gigaspaces.lrmi.nio.Writer;
import com.gigaspaces.lrmi.nio.selector.handler.client.ClientHandler;
import com.gigaspaces.lrmi.nio.selector.handler.client.Context;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a client handler context implementation. stores the state of the async operation. a normal
 * operation includes multiple write and read action.
 *
 * first operation is always a write followed by a sequence of read(class requests) and writes
 * (class response). the last action is always a read of the final result.
 *
 * at the end of the operation the associated connection (CPeer) is returned to the pool.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class AsyncContext implements Context {
    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    final private static Logger _contextLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_CONTEXT);


    private final ConnectionPool connectionPool;
    private final ClientHandler handler;
    private final LRMIFuture result;
    private final ClassLoader contextClassLoader;
    private final LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier;

    private volatile boolean reuseBuffer = true;

    private volatile SelectionKey selectionKey;
    private volatile RequestPacket requestPacket;

    private volatile Reader.Context readerCtx;
    private volatile Writer.Context writerCtx;

    final private CPeer cpeer;
    final private IRemoteClassProviderProvider remoteConnection;
    final private AtomicBoolean finished = new AtomicBoolean();

    private final LRMIInvocationTrace invocationTrace;
    private final PlatformLogicalVersion sourceLogicalVersion;
    private final PlatformLogicalVersion targetLogicalVersion;
    private final boolean useStubCache;
    private final String monitoringId;
    private volatile ProxyWriteType proxyWriteType;

    private final ClientPeerWatchedObjectsContext watchedObjectContext;

    public AsyncContext(
            ConnectionPool connPool,
            ClientHandler handler,
            RequestPacket packet,
            LRMIFuture result,
            CPeer cpeer,
            IRemoteClassProviderProvider remoteConnection,
            ClassLoader contextClassLoader,
            LRMIRemoteClassLoaderIdentifier remoteClassLoaderIdentifier,
            String monitoringId,
            ClientPeerWatchedObjectsContext watchedObjectContext) {
        this.connectionPool = connPool;
        this.handler = handler;
        this.result = result;
        this.requestPacket = packet;
        this.cpeer = cpeer;
        this.remoteConnection = remoteConnection;
        this.contextClassLoader = contextClassLoader;
        this.remoteClassLoaderIdentifier = remoteClassLoaderIdentifier;
        this.monitoringId = monitoringId;
        this.watchedObjectContext = watchedObjectContext;

        LRMIInvocationContext invocationContext = LRMIInvocationContext.getCurrentContext();
        invocationTrace = _contextLogger.isDebugEnabled() ? invocationContext.getTrace() : null;
        sourceLogicalVersion = invocationContext.getSourceLogicalVersion();
        targetLogicalVersion = invocationContext.getTargetLogicalVersion();
        proxyWriteType = invocationContext.getProxyWriteType();
        useStubCache = invocationContext.isUseStubCache();
    }

    public void setSelectionKey(SelectionKey key) {
        this.selectionKey = key;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void handleRead() {
        if (finished.get()) {
            return;
        }

        if (readerCtx == null)
            readerCtx = new Reader.Context(selectionKey);

        try {
            if (invocationTrace != null)
                LRMIInvocationContext.updateContext(invocationTrace, null, null, null, null, false, null, null);
            final MarshalInputStream replyStream = cpeer.getReader().readReply(readerCtx);
            if (readerCtx.phase != Reader.Context.Phase.FINISH) {
                handler.setReadInterest(this);
                return;
            }

            readerCtx = null;

            LRMIRuntime.getRuntime().getThreadPool().execute(new ContextClassLoaderRunnable(contextClassLoader) {
                @Override
                protected void execute() {

                    //Cleared at the finish execution
                    LRMIConnection.setConnection(remoteConnection);

                    LRMIRemoteClassLoaderIdentifier previousIdentifier = RemoteClassLoaderContext.set(remoteClassLoaderIdentifier);
                    try {
                        LRMIInvocationContext.updateContext(invocationTrace, proxyWriteType, InvocationStage.CLIENT_RECEIVE_REPLY, sourceLogicalVersion, targetLogicalVersion, false, null, null);

                        //TODO OPT: reuse reply packet?
                        final ReplyPacket replyPacket = cpeer.getReader().unmarshallReply(replyStream);

                        Exception exception = replyPacket.getException();
                        //Check to see if we need to reset the writer context
                        if (exception instanceof LRMIUnhandledException) {
                            if (((LRMIUnhandledException) exception).getStage() == Stage.DESERIALIZATION) {
                                //We must reset the context because the other side have not completed reading the stream and therefore didn't
                                //learn all the new context
                                cpeer.getWriter().getSerializer().resetContext();
                            }
                        }

                        if (proxyWriteType == ProxyWriteType.CACHED_LIGHT && exception instanceof MissingCachedStubException) {
                            proxyWriteType = ProxyWriteType.CACHED_FULL;
                            setWriteInterest();
                        } else if (replyPacket.getResult() instanceof ClassProviderRequest) {
                            long clientClassLoaderId = cpeer.getClassProvider().putClassLoader(contextClassLoader);
                            requestPacket = new RequestPacket(cpeer.getClassProvider());

                            reuseBuffer = false;
                            setWriteInterest();
                        } else if (replyPacket.getResult() instanceof ClassDefinitionRequest) {
                            ClassDefinitionRequest classDefinitionRequest = (ClassDefinitionRequest) replyPacket.getResult();
                            IClassProvider classProvider = cpeer.getClassProvider();
                            byte[] definition = new byte[0];
                            Exception exp = null;
                            if(classDefinitionRequest.getFileType() == FileType.CLASS){
                                try{
                                    definition = classProvider.getClassDefinition(classDefinitionRequest.getClassLoaderId(), classDefinitionRequest.getClassName());
                                }catch (ClassNotFoundException e){
                                    exp = e;
                                }
                            }
                            else if(classDefinitionRequest.getFileType() == FileType.RESOURCE){
                                try{
                                    definition = classProvider.getResource(classDefinitionRequest.getClassLoaderId(), classDefinitionRequest.getResourceName());
                                }catch (ClassNotFoundException e){
                                    exp = e;
                                }
                            }
                            requestPacket = new RequestPacket(new ClassDefinitionResponse(definition, exp));
                            reuseBuffer = false;
                            setWriteInterest();
                        }else {
                            finishExecution(replyPacket, true);
                        }
                    } catch (ClassNotFoundException e) {
                        disconnectAndFinish(e, true);
                    } catch (MarshalContextClearedException e) {
                        disconnectAndFinish(e, true);
                    } catch (RemoteException e) {
                        disconnectAndFinish(e, true);
                    } catch (Exception e) {
                        finishExecution(new ReplyPacket(null, e), true);
                    } finally {
                        LRMIInvocationContext.resetContext();
                        RemoteClassLoaderContext.set(previousIdentifier);
                    }
                }
            });

            LRMIConnection.clearConnection();
        } catch (IOException e) {
            disconnectAndFinish(e, true);
        } catch (Throwable t) {
            Exception e;
            if (t instanceof Exception)
                e = (Exception) t;
            else
                e = new ExecutionException(t);
            finishExecution(new ReplyPacket(null, e), true);
        } finally {
            if (invocationTrace != null)
                LRMIInvocationContext.resetContext();
        }
    }

    private void disconnect() {
        cpeer.disconnect();
    }

    /**
     * @param initiatedFinish see {@link #finishExecution(ReplyPacket, boolean)} for details
     */
    private void disconnectAndFinish(Throwable e, boolean initiatedFinish) {
        if (initiatedFinish && !finished.compareAndSet(false, true)) {
            return;
        }

        disconnect();
        if (_logger.isDebugEnabled())
            _logger.debug("LRMI transport protocol over NIO broken connection with ServerEndPoint: [" + cpeer.getConnectionURL() + "]", e);

        finishExecution(new ReplyPacket(null, convertException(e)), false);
    }

    private Exception convertException(Throwable e) {
        if (e instanceof IOException) {
            if (watchedObjectContext.requestWatchHasException())
                return new ConnectIOException(e.getMessage(), watchedObjectContext.getAndClearRequestWatchException());
            if (watchedObjectContext.responseWatchHasException())
                return new ConnectIOException(e.getMessage(), watchedObjectContext.getAndClearResponseWatchException());
            return new ConnectException(e.getMessage(), (Exception) e);
        }
        if (e instanceof MarshalContextClearedException)
            return new RemoteException(e.getMessage(), e);
        if (e instanceof Exception)
            return (Exception) e;

        return new ExecutionException(e);
    }

    /**
     * @param initiatedFinish Indicates whether the 'finised' flag should be compared and set to
     *                        true The only path in which this value will be false is when calling
     *                        {@link #close(Throwable)} because we already compare and set the
     *                        'finished' flag over there.
     */
    private void finishExecution(final ReplyPacket replyPacket, boolean initiatedFinish) {
        if (initiatedFinish && !finished.compareAndSet(false, true)) {
            return;
        }

        try {
            LRMIConnection.clearConnection();
            cpeer.clearAsyncContext();
            cpeer.getMonitoringModule().monitorActivity(monitoringId, cpeer.getWriter(), cpeer.getReader());
            cpeer.afterInvoke();
            Writer writer = cpeer.getWriter();
            if (writer != null) // avoid NPE if CPeer was already closed on exception
                writer.setWriteInterestManager(null);

            // Must be invoked last to prevent race condition between
            // cleanup and set of the RequestPacket.
            result.setResultPacket(replyPacket);
        } finally {
            // if execution is finished due to registration failure or cancelled key exception there is
            // no need (and impossible) to unregister.
            SelectionKey currentSelectionKey = selectionKey;
            if (currentSelectionKey != null) {
                // must be called last, might release the the CPeer in an async way.
                handler.removeChannel((SocketChannel) currentSelectionKey.channel(), this);
            }
        }
    }


    public void handleWrite() {
        if (finished.get()) {
            return;
        }

        try {
            if (writerCtx == null) {
                try {
                    LRMIInvocationContext.updateContext(invocationTrace, proxyWriteType, InvocationStage.CLIENT_SEND_REQUEST, sourceLogicalVersion, targetLogicalVersion, false, useStubCache, null);
                    writerCtx = new Writer.Context(invocationTrace);
                    cpeer.getWriter().writeRequest(requestPacket, reuseBuffer, writerCtx);
                } finally {
                    LRMIInvocationContext.resetContext();
                }
            } else {
                cpeer.getWriter().onWriteEvent();
            }
        } catch (IOException e) {
            disconnectAndFinish(e, true);
        } catch (MarshalContextClearedException e) {
            disconnectAndFinish(e, true);
        } catch (Throwable t) {
            Exception e;
            if (t instanceof Exception)
                e = (Exception) t;
            else
                e = new ExecutionException(t);
            finishExecution(new ReplyPacket(null, e), true);
        }
    }

    public void handleSetReadInterest() {
        watchedObjectContext.watchResponse(monitoringId);
    }

    public void removeWriteInterest(boolean restoreReadInterest) {
        //Ignore restoreReadInterest, always restore it in this case, this
        //is client side, he will never receive invocation from the server
        if (writerCtx.getPhase() == Writer.Context.Phase.FINISH) {
            writerCtx = null;
            handler.setReadInterest(this);
        }
    }

    public void setWriteInterest() {
        watchedObjectContext.watchRequest(monitoringId);
        handler.setWriteInterest(this);
    }

    public void close(Throwable exception) {
        // this method might be called concurrently from ClientHandler#cancelKey
        // and the watchdog RequestTimeoutObserver#close
        if (!finished.compareAndSet(false, true)) {
            return;
        }

        // passing false so finishExecution will execute and 
        // 'finished' flag will not be tested again
        disconnectAndFinish(exception, false);
        close();
    }

    public void closeAndDisconnect() {
        disconnect();
        close();
    }

    /**
     * Release the CPeer when called on un register from the Selector
     */
    public void close() {
        // Release the CPeer
        connectionPool.freeConnection(cpeer);
    }
}


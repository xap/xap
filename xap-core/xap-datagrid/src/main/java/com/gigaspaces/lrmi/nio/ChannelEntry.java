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

import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.lrmi.LRMIMonitoringModule;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.UIDGen;
import com.gigaspaces.lrmi.classloading.IRemoteClassProviderProvider;
import com.gigaspaces.lrmi.nio.filters.IOBlockFilterManager;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.gigaspaces.lrmi.nio.selector.handler.ReadSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.WriteSelectorThread;
import com.gigaspaces.lrmi.tcp.TcpReader;
import com.gigaspaces.lrmi.tcp.TcpWriter;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.SystemProperties;

import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Channel Entry is an entry in the Pivot Channels Table.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class ChannelEntry implements IWriteInterestManager {
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);


    final private Pivot _pivot;
    final private SocketChannel _socketChannel;
    final private InetSocketAddress _clientEndPointAddress;
    final private SelectionKey _readSelectionKey;
    final public Writer _writer;
    final public Reader _reader;

    public SelectionKey _writeSelectionKey;

    /**
     * the remote objectID this change belong to
     */
    private long _ownerRemoteObjID;

    /**
     * the unique connection ID identifier
     */
    final public long _connectionID;

    /**
     * the connection time stamp in milliseconds
     */
    final private long _connectionTimeStamp;
    final private ReadSelectorThread _readSelectorThread;
    final private WriteSelectorThread _writeSelectorThread;
    final private IRemoteClassProviderProvider _remoteClassProvider = new Pivot.ServerRemoteClassProviderProvider(this);
    final private IOFilterManager _filterManager;

    private boolean _protocolValidated;

    private PlatformLogicalVersion _sourceLogicalVersion;
    private long _sourcePid = -1;

    final static private Map<String, Long> _rejectedProtocolHosts = new HashMap<String, Long>();
    final private LRMIMonitoringModule _monitoringModule = new LRMIMonitoringModule();

    private final WriteExecutionPhaseListener _writeExecutionPhaseListener = new ChannelEntryWriteExecutionPhaseListener();
    private volatile State _currentChannelState = State.IDLE;
    private volatile boolean _firstMessage = true;


    /**
     * Constructor.
     *
     * @param selectionKey          a token representing the registration of a {@link
     *                              SelectableChannel} with a {@link Selector}.
     * @param clientEndPointAddress the - The address of the connecting LRMI client.
     */
    public ChannelEntry(WriteSelectorThread writeSelectorThread, ReadSelectorThread readSelectorThread, SelectionKey selectionKey, InetSocketAddress clientEndPointAddress, Pivot pivot) {
        SocketChannel channel = (SocketChannel) selectionKey.channel();

        _pivot = pivot;
        _readSelectionKey = selectionKey;
        _readSelectorThread = readSelectorThread;
        _writeSelectorThread = writeSelectorThread;
        _socketChannel = channel;
        _writer = new TcpWriter(channel);
        _writer.setWriteInterestManager(this);
        _reader = new TcpReader(channel, _pivot.getSystemRequestHandler());
        _connectionID = UIDGen.nextId();
        _connectionTimeStamp = SystemTime.timeMillis();
        _clientEndPointAddress = clientEndPointAddress;
        try {
            _filterManager = IOBlockFilterManager.createFilter(_reader, _writer, false, _socketChannel);
        } catch (Exception e) {
            throw new InternalSpaceException("Failed to load communication filter " + System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY), e);
        }
        if (_filterManager != null) {
            try {
                _filterManager.beginHandshake();
            } catch (Exception e) {
                throw new InternalSpaceException("Failed to perform communication filter handshake using " + System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY) + " filter", e);
            }
        }
    }

    public MarshalInputStream readRequest(Reader.Context ctx) {
        try {
            if (_firstMessage) {
                ctx.messageSizeLimit = 1024 * 5;
                _firstMessage = false;
            }
            return _reader.readRequest(ctx);
        } catch (ClosedChannelException e) {
            if (_logger.isDebugEnabled())
                _logger.debug("Failed to read from " + getClientEndPointAddress() + " due to closed channel", e);
        } catch (IOFilterException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Closing connection to " + getClientEndPointAddress() + " because of " + e, e);
        } catch (ConnectException e) {
            _logger.warn("Closing connection to " + getClientEndPointAddress() + " because of " + e, e);
        } catch (OutOfMemoryError oome) {
            _logger.warn("Closing connection to " + getClientEndPointAddress() + " because of " + oome + ", message length is " + ctx.dataLength + " bytes", oome);
        } catch (Throwable e) {
            _pivot.handleExceptionFromServer(_writer, _reader, e);
        }
        _pivot.closeConnection(this);
        return null;
    }

    public RequestPacket unmarshall(MarshalInputStream stream) {
        RequestPacket requestPacket = null;
        try {
            requestPacket = _reader.unmarshallRequest(stream);
        } catch (Throwable ex) {
            if (_pivot.handleExceptionFromServer(_writer, _reader, ex))
                _pivot.closeConnection(this);
        }

        return requestPacket;
    }

    /**
     * Set the remote objectID this ChannelEntry belongs to.
     *
     * @remoteObjID the remote objectID.
     */
    public void setOwnerRemoteObjID(long remoteObjID) {
        if (_ownerRemoteObjID == 0)
            _ownerRemoteObjID = remoteObjID;
    }

    /**
     * @return the end point of the connected SocketChannel.
     */
    public InetSocketAddress getClientEndPointAddress() {
        if (_clientEndPointAddress != null) {
            return _clientEndPointAddress;
        }

        Socket socket = _socketChannel.socket();
        if (socket == null)
            return null;
        return (InetSocketAddress) socket.getRemoteSocketAddress();
    }

    public void writeReply(ReplyPacket packet, boolean reuseBuffer, Writer.Context ctx, String monitoringId) {
        try {
            _writer.writeReply(packet, reuseBuffer, ctx);
            monitorActivity(monitoringId);
        } catch (MarshalContextClearedException e) {
            if (_logger.isDebugEnabled())
                _logger.debug("Caught marshal context cleared exception, closing connection", e);
            _pivot.closeConnection(this);
            throw e;
        } catch (Exception ex) {
            _pivot.handleExceptionFromServer(this._writer, _reader, ex);
            _pivot.closeConnection(this);
        }
    }

    public void removeWriteInterest(boolean restoreReadInterest) {
        _writeSelectorThread.removeWriteInterest(_writeSelectionKey);
        _writeSelectionKey = null;

        if (restoreReadInterest)
            returnSocket(); // reregister socket for read events
    }

    public void setWriteInterest() {
        _writeSelectorThread.setWriteInterest(this);
    }

    /**
     * @return the remoteObjectID this ChannelEntry belongs to.
     */
    public long getRemoteObjID() {
        return _ownerRemoteObjID;
    }

    /**
     * @return the timeStamp where client connected to the server
     */
    public long getConnectionTimeStamp() {
        return _connectionTimeStamp;
    }

    /**
     * @return the unique connection ID identifier
     */
    public long getConnectionID() {
        return _connectionID;
    }

    public void returnSocket() {
        if (_readSelectorThread != null && _readSelectionKey != null) {
            _readSelectionKey.attach(null);
            _readSelectorThread.registerKey(_readSelectionKey);
        }
    }

    public void onWriteEvent() {
        try {
            //From channel entry, we should restore read interest if this is the last
            //write event
            _writer.onWriteEvent();
        } catch (Exception ex) {
            _pivot.handleExceptionFromServer(this._writer, _reader, ex);
            _pivot.closeConnection(this);
        }
    }

    public void close() throws IOException {
        //Clear the context the inner streams hold upon disconnection
        _socketChannel.close();
        _writer.getSerializer().closeContext();
        _reader.closeContext();
    }

    //Flush to main memory
    public synchronized void setSourceDetails(PlatformLogicalVersion logicalVersion, long pid) {
        this._sourceLogicalVersion = logicalVersion;
        this._sourcePid = pid;
    }

    public PlatformLogicalVersion getSourcePlatformLogicalVersion() {
        if (_sourceLogicalVersion == null) {
            //Make sure we get from main memory, setSource will always be called and before any prior call to getSource
            synchronized (this) {
                return _sourceLogicalVersion;
            }
        }
        return _sourceLogicalVersion;
    }

    public long getSourcePid() {
        if (_sourcePid == -1) {
            //Make sure we get from main memory, setSource will always be called and before any prior call to getSource
            synchronized (this) {
                return _sourcePid;
            }
        }
        return _sourcePid;
    }

    public long getGeneratedTraffic() {
        return _writer.getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        return _reader.getReceivedTraffic();
    }

    public IRemoteClassProviderProvider getRemoteClassProvider() {
        return _remoteClassProvider;
    }

    public SocketChannel getSocketChannel() {
        return _socketChannel;
    }

    public SelectionKey getReadSelectionKey() {
        return _readSelectionKey;
    }

    public boolean isProtocolValidated() {
        return _protocolValidated;
    }

    public boolean readProtocolValidationHeader(Reader.ProtocolValidationContext context) {
//        try {
//            String protocolHeader = _reader.readProtocolValidationHeader(context);
//            if (!ProtocolValidation.PROTOCOL_STRING.startsWith(protocolHeader)) {
//                removeObsoleteRejectedProtocolHosts();
//                addToRejectedProtocolHosts();
//                return false;
//            }
//
//            if (ProtocolValidation.PROTOCOL_STRING.equals(protocolHeader)) {
//                removeObsoleteRejectedProtocolHosts();
//                _protocolValidated = true;
//            }
//
//            return true;
//        } catch (IOException e) {
//            if (_logger.isLoggable(Level.FINE))
//                _logger.log(Level.FINE, "Failed to read protocol header from " + getClientEndPointAddress() + " due to IOException", e);
//        } catch (Throwable t) {
//            if (_logger.isLoggable(Level.SEVERE))
//                _logger.log(Level.SEVERE, "Failed to read protocol header from " + getClientEndPointAddress(), t);
//        }
        return true;
    }

    public void monitorActivity(String trackingId) {
        _monitoringModule.monitorActivity(trackingId, _writer, _reader);
    }

    public LRMIMonitoringModule getMonitoringModule() {
        return _monitoringModule;
    }

    private void addToRejectedProtocolHosts() {
        synchronized (_rejectedProtocolHosts) {
            InetSocketAddress clientEndPointAddress = getClientEndPointAddress();
            String hostName = clientEndPointAddress == null ? null : clientEndPointAddress.getHostName();
            if (StringUtils.hasText(hostName) && !_rejectedProtocolHosts.containsKey(hostName)) {
                long currentTime = SystemTime.timeMillis();
                _rejectedProtocolHosts.put(hostName, currentTime);
                if (_logger.isWarnEnabled())
                    _logger.warn("Incoming communication from " + getClientEndPointAddress() + " using unrecognized protocol, closing channel");
            }
        }
    }

    public State getChannelState() {
        return _currentChannelState;
    }

    public void setChannelState(State state) {
        _currentChannelState = state;
    }

    private static void removeObsoleteRejectedProtocolHosts() {
        synchronized (_rejectedProtocolHosts) {
            long currentTime = SystemTime.timeMillis();
            //Scan and remove for rejected hosts for which a warning was printed more than 5 minutes ago
            Iterator<Entry<String, Long>> iterator = _rejectedProtocolHosts.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Long> entry = iterator.next();
                //5 minutes
                if ((currentTime - entry.getValue()) > (5 * 60 * 1000))
                    iterator.remove();
            }
        }
    }

    public WriteExecutionPhaseListener getWriteExecutionPhaseListener() {
        return _writeExecutionPhaseListener;
    }

    private class ChannelEntryWriteExecutionPhaseListener implements WriteExecutionPhaseListener {
        public void onPhase(Writer.Context.Phase phase) {
            if (phase == Writer.Context.Phase.FINISH)
                setChannelState(State.IDLE);
        }
    }

    public static enum State {
        NONE(0),
        PROGRESS(1),
        IDLE(2);

        int code;

        State(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static State fromCode(int code) {
            switch (code) {
                case 0:
                    return NONE;
                case 1:
                    return PROGRESS;
                case 2:
                    return IDLE;
                default:
                    throw new IllegalArgumentException("No such state: " + code);
            }
        }
    }
}

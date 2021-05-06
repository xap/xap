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

package com.gigaspaces.lrmi.nio.selector.handler;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.ManagedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * Provides a general selector logic.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 */
public abstract class AbstractSelectorThread extends ManagedRunnable implements Runnable {
    private static final long SELECT_TIMEOUT = 10 * 1000; // 10 sec
    protected static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);

    final private Selector _selector;
    private long lastCleanup = 0;

    protected volatile Thread ownerThread;

    protected AbstractSelectorThread() throws IOException {
        _selector = Selector.open();
    }

    public void run() {
        ownerThread = Thread.currentThread();
        while (!shouldShutdown() && _selector.isOpen()) {
            doSelect();
        }
    }

    private void doSelect() {
        SelectionKey key = null;
        try {
            enableSelectionKeys();
            checkForDeadConnections();
            _selector.select(SELECT_TIMEOUT);
            Set<SelectionKey> readyKeys = _selector.selectedKeys();
            if (readyKeys == null || readyKeys.isEmpty()) {
                return;
            }
            Iterator<SelectionKey> iterator = readyKeys.iterator();
            while (iterator.hasNext()) {
                key = iterator.next();
                iterator.remove();

                if (key.isValid() && key.channel().isOpen()) {
                    handleConnection(key);
                } else {
                    cancelKey(key);
                }
            }
        } catch (ClosedSelectorException ex) {
            _logger.debug("Selector was closed.", ex);
            if (key != null) {
                key.cancel();
            }
        } catch (IOException e) {
            // handle exception in main selection loop; Caused by: java.io.IOException: Too many open files
            if (key != null) {
                if (this instanceof AcceptSelectorThread) {
                    _logger.error("exception in main selection loop, delay selector for 1 second", e);
                    delay(e);
                } else {
                    _logger.error("exception in main selection loop, canceling key", e);
                    key.cancel();
                }
            } else {
                _logger.error("exception in main selection loop", e);
            }
        } catch (Throwable t) {
            _logger.error("exception in main selection loop", t);
            if (key != null) {
                key.cancel();
            }
        }
    }

    private void delay(IOException ex) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            _logger.error("Interrupted while delaying accept selector because of exception " + ex, e);
        }
    }

    protected SelectionKey register(SocketChannel channel, int requestOps) throws ClosedChannelException {

        // workaround for bug: GS-6001
        // if registration fails due to canceled key we gracefully try again
        // 3 times. after that that channel is officially declared corrupted
        // and is closed.
        Exception failureReason = null;
        for (int i = 0; i < 3; i++) {
            try {
                return channel.register(getSelector(), requestOps);
            } catch (CancelledKeyException cke) {
                try {
                    failureReason = cke;
                    if (_logger.isDebugEnabled())
                        _logger.debug("caught CancelledKeyException while registering socket interest [" + channel.toString() + "] at attempt " + (i + 1) + ", retrying...", cke);
                    //GS-6557 if we received cancel key exception it is (probably) because a concurrent thread that uses the same
                    //socket had removed the write interest, hence canceled the SelectionKey associated with this channel (can occur due to concurrency issue described in ChannelEntryTask.run() method)
                    //In that case, channel.register throws this exception because it returns the same key, it will remove this key only
                    //after the selector will do a select cycle, therefore, we do selectNow to update state and retry
                    _selector.selectNow();
                } catch (IOException ioe) {
                    failureReason = ioe;
                    if (_logger.isDebugEnabled())
                        _logger.debug("caught IOException while registering socket interest [" + channel.toString() + "] at attempt " + (i + 1), ioe);
                    //Nothing to do, break attempt and close the channel
                    break;
                }
            }
        }

        if (_logger.isWarnEnabled())
            _logger.warn("failed all attempts of registering socket interest, closing socket [" + channel.toString() + "]", failureReason);
        // no hope, the channel is probably corrupted.
        closeChannel(channel);
        throw new ClosedChannelException();
    }

    private void checkForDeadConnections() {
        final long now = SystemTime.timeMillis();
        if (now > (lastCleanup + 30 * 1000)) {
            lastCleanup = now;

            final Set<SelectionKey> keys = _selector.keys();
            for (SelectionKey key : keys) {
                if (!key.channel().isOpen()) {
                    cancelKey(key);
                }
            }
        }
    }

    abstract protected void enableSelectionKeys();

    abstract protected void handleConnection(SelectionKey key) throws IOException, InterruptedException;

    protected void cancelKey(SelectionKey key) {
        if (key == null || !key.isValid()) {
            return;
        }

        closeChannel((SocketChannel) key.channel());
    }

    /**
     * Close a given channel
     *
     * @param channel target channel to close.
     */
    protected void closeChannel(SocketChannel channel) {
        if (channel == null)
            return;
        Socket socket = channel.socket();
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
        } catch (IOException ex) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("error while closing a key", ex);
            }
        } finally {
            try {
                channel.close();
            } catch (IOException ex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("error while closing a channel", ex);
                }
            }
        }
    }

    @Override
    protected void waitWhileFinish() {
        try {
            // close selector
            _selector.close();
        } catch (IOException ex) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("error while closing the selector", ex);
            }
        }
    }

    protected Selector getSelector() {
        return _selector;
    }
}

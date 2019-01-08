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

package com.gigaspaces.lrmi.nio.selector.handler.client;


import com.gigaspaces.lrmi.nio.LrmiChannel;
import com.gigaspaces.lrmi.nio.selector.handler.AbstractSelectorThread;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A client side selector thread that handles Async remote calls. the handler works against a
 * context to support both read and write actions. all channel activities are non blocking.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ClientHandler extends AbstractSelectorThread {
    final private Queue<RegistrationRequest> _registrations = new ConcurrentLinkedQueue<RegistrationRequest>();
    final private AtomicInteger _registrationsCounter = new AtomicInteger();
    final private Queue<InterestRequest> _interests = new ConcurrentLinkedQueue<InterestRequest>();
    final private AtomicInteger _interestsCounter = new AtomicInteger();

    public ClientHandler() throws IOException {
        super(null);
    }

    @Override
    protected void enableSelectionKeys() {
        handleRegistrations();
        interestSelectionKeys();
    }

    private void handleRegistrations() {
        int size = _registrationsCounter.get();
        boolean hasUnregisterRequests = false;
        for (int i = 0; i < size; i++) {
            RegistrationRequest request = _registrations.poll();
            _registrationsCounter.decrementAndGet();
            if (request.getAction() == RegistrationRequest.Action.REGISTER) {
                register(request);
            } else if (request.getAction() == RegistrationRequest.Action.UNREGISTER) {
                hasUnregisterRequests = true;
                unregister(request);
            }
        }

        // make sure all the unregister will take place to avoid register the same channel before finish unregister
        if (hasUnregisterRequests) {
            // wake-up selector if selectNow() cleaned it from concurrent wake-up
            getSelector().wakeup();
        }
    }

    private void unregister(RegistrationRequest request) {
        final Context context = request.getContext();
        try {
            final SelectionKey selectionKey = context.getSelectionKey();
            selectionKey.attach(null);
            SelectableChannel channel = selectionKey.channel();
            selectionKey.cancel();
            getSelector().selectNow();
            if (channel.isOpen()) {
                channel.configureBlocking(true);
            }
            context.close();
        } catch (Throwable t) {
            _logger.info(t.toString(), t);
            // an answer was already sent to the user, no need to send another
            // just disconnect the channel and return it to the pool.
            context.closeAndDisconnect();
        }
    }

    private void register(RegistrationRequest request) {
        SocketChannel channel = request.getChannel();
        final Context ctx = request.getContext();
        try {
            channel.configureBlocking(false);
            SelectionKey selectionKey = channel.register(getSelector(), SelectionKey.OP_WRITE);
            ctx.setSelectionKey(selectionKey);
            selectionKey.attach(ctx);
        } catch (Throwable t) {
            // send the exception back to the client as a result.
            // if the exception is an IOException it will be trapped by the ExceptionHandler
            // and trigger a failover event.
            ctx.close(t);
            _logger.warn(t.toString(), t);
        }
    }

    private void interestSelectionKeys() {
        int size;
        size = _interestsCounter.get();
        for (int i = 0; i < size; i++) {
            InterestRequest request = _interests.remove();
            _interestsCounter.decrementAndGet();
            int ops = 0;
            switch (request.getAction()) {
                case WRITE:
                    ops = SelectionKey.OP_WRITE;
                    break;
                case READ:
                    request.getContext().handleSetReadInterest();
                    ops = SelectionKey.OP_READ;
                    break;
            }

            //The watchdog could reset the selection key if it disconnected this channel and there was a registration to readInterest which
            //wasn't handled yet by this selection thread.
            final SelectionKey selectionKey = request.getContext().getSelectionKey();
            if (selectionKey == null)
                return;
            try {
                selectionKey.interestOps(ops);
            } catch (CancelledKeyException e) {
                cancelKey(selectionKey);
            }
        }
    }

    @Override
    protected void handleConnection(SelectionKey key) throws IOException, InterruptedException {
        final Context ctx = (Context) key.attachment();
        if (key.isReadable()) {
            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
            ctx.handleRead();
        } else if (key.isWritable()) {
            key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
            ctx.handleWrite();
        }
    }

    public void addChannel(SocketChannel channel, Context ctx) {
        _registrations.offer(new RegistrationRequest(channel, ctx, RegistrationRequest.Action.REGISTER));
        _registrationsCounter.incrementAndGet();
        getSelector().wakeup();
    }

    public void removeChannel(SocketChannel channel, Context ctx) {
        _registrations.offer(new RegistrationRequest(channel, ctx, RegistrationRequest.Action.UNREGISTER));
        _registrationsCounter.incrementAndGet();
        if (ownerThread != Thread.currentThread()) {
            getSelector().wakeup();
        }
    }

    public void setReadInterest(Context ctx) {
        _interests.add(new InterestRequest(ctx, InterestRequest.Action.READ));
        _interestsCounter.incrementAndGet();
    }

    public void setWriteInterest(Context ctx) {
        _interests.add(new InterestRequest(ctx, InterestRequest.Action.WRITE));
        _interestsCounter.incrementAndGet();
        if (ownerThread != Thread.currentThread()) {
            getSelector().wakeup();
        }
    }

    @Override
    protected void cancelKey(SelectionKey key) {
        Context ctx = (Context) key.attachment();
        if (ctx != null) {
            ctx.setSelectionKey(null);
            ctx.close(new ClosedChannelException());
        }
    }

    public void shutdown() {
        requestShutdown();
    }
}

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


package com.gigaspaces.lrmi.nio.filters;

import com.gigaspaces.lrmi.nio.filters.IOFilterResult.HandshakeStatus;
import com.gigaspaces.lrmi.nio.filters.IOFilterResult.Status;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

/**
 * Implementation of SSLFilter that use underline SUN SSLEngine.
 *
 * @author barak
 */

public class IOSSLFilter implements IOBlockFilter {

    private static final Logger logger = LoggerFactory.getLogger(IOBlockFilterContainer.class.getName());
    private final SSLEngine _sslEngine;
    private final SSLContext _sslContext;
    private final String _uid;
    private final SocketAddress _remoteAddress;

    public IOSSLFilter(SSLContext sslContext, SocketAddress remoteAddress) {
        super();
        _sslContext = sslContext;
        _remoteAddress = remoteAddress;
        _sslEngine = _sslContext.createSSLEngine();
        _uid = UUID.randomUUID().toString();
    }

    public void beginHandshake() throws IOFilterException {
        if (logger.isDebugEnabled()) {
            if (getUseClientMode())
                logger.debug("Client IOSSLFilter with uid " + _uid + " connected to " + _remoteAddress + " starting handshake");
            else
                logger.debug("Server IOSSLFilter with uid " + _uid + " connected to " + _remoteAddress + " starting handshake");
        }
        try {
            _sslEngine.beginHandshake();
        } catch (SSLException e) {
            throw new IOFilterException(e);
        }
    }

    public int getApplicationBufferSize() {
        return _sslEngine.getSession().getApplicationBufferSize();
    }

    public Runnable getDelegatedTask() {
        return _sslEngine.getDelegatedTask();
    }

    public HandshakeStatus getHandshakeStatus() {
        return translate(_sslEngine.getHandshakeStatus());
    }

    public int getPacketBufferSize() {
        return _sslEngine.getSession().getPacketBufferSize();
    }

    public boolean getUseClientMode() {
        return _sslEngine.getUseClientMode();
    }

    public void setUseClientMode(boolean mode) {
        _sslEngine.setUseClientMode(mode);
    }

    public IOFilterResult unwrap(ByteBuffer src, ByteBuffer dst)
            throws IOFilterException {
        try {
            SSLEngineResult res = _sslEngine.unwrap(src, dst);
            if (res.getStatus().equals(SSLEngineResult.Status.CLOSED)) {
                throw new IOFilterException("Engine closed");
            }
            return translate(res);
        } catch (SSLException e) {
            throw new IOFilterException(e);
        }

    }

    public IOFilterResult wrap(ByteBuffer src, ByteBuffer dst)
            throws IOFilterException {
        try {
            SSLEngineResult res = _sslEngine.wrap(src, dst);
            return translate(res);
        } catch (SSLException e) {
            throw new IOFilterException(e);
        }
    }

    private HandshakeStatus translate(
            javax.net.ssl.SSLEngineResult.HandshakeStatus handshakeStatus) {
        switch (handshakeStatus) {
            case FINISHED:
                if (logger.isDebugEnabled()) {
                    if (getUseClientMode())
                        logger.info("Client IOSSLFilter with uid " + _uid + " connected to " + _remoteAddress + " finished handshake");
                    else
                        logger.info("Server IOSSLFilter with uid " + _uid + " connected to " + _remoteAddress + " finished handshake");
                }
                return HandshakeStatus.FINISHED;
            case NEED_TASK:
                return HandshakeStatus.NEED_TASK;
            case NEED_UNWRAP:
                return HandshakeStatus.NEED_UNWRAP;
            case NEED_WRAP:
                return HandshakeStatus.NEED_WRAP;
            case NOT_HANDSHAKING:
                return HandshakeStatus.NOT_HANDSHAKING;
            default:
                throw new IllegalArgumentException(String.valueOf(handshakeStatus));
        }
    }

    private IOFilterResult translate(SSLEngineResult sslEngineResult) {
        return new IOFilterResult(translate(sslEngineResult.getStatus()),
                translate(sslEngineResult.getHandshakeStatus()),
                sslEngineResult.bytesConsumed(), sslEngineResult
                .bytesProduced());
    }

    private Status translate(javax.net.ssl.SSLEngineResult.Status status) {
        switch (status) {
            case BUFFER_OVERFLOW:
                return Status.BUFFER_OVERFLOW;
            case BUFFER_UNDERFLOW:
                return Status.BUFFER_UNDERFLOW;
            case CLOSED:
                return Status.CLOSED;
            case OK:
                return Status.OK;
            default:
                throw new IllegalArgumentException(String.valueOf(status));
        }
    }

    @Override
    public String toString() {
        return "IOSSLFilter " + _uid + " " + (_sslEngine.getUseClientMode() ? "client" : "server") + " " + _sslEngine.getHandshakeStatus();
    }


    // public class IOSSLFilter
}

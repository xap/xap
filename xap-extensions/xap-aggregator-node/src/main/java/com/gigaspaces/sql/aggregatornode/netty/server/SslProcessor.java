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
package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.SocketAddress;
import java.security.KeyStore;
import java.security.Security;

public class SslProcessor implements ChannelInboundHandler, ChannelOutboundHandler {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(SslProcessor.class);

    private interface Delegate extends ChannelInboundHandler, ChannelOutboundHandler {}

    private static final class NoOpDelegate implements Delegate {
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
            ctx.fireChannelUnregistered();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            ctx.fireChannelInactive();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ctx.fireChannelRead(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.fireChannelReadComplete();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            ctx.fireChannelWritabilityChanged();
        }


        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
                         ChannelPromise promise) {
            ctx.bind(localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) {
            ctx.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
            ctx.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
            ctx.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
            ctx.deregister(promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) {
            ctx.read();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            ctx.write(msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) {
            ctx.flush();
        }
    }

    private static final class SslDelegate extends SslHandler implements Delegate {
        public SslDelegate(SSLEngine engine) {
            super(engine);
        }
    }

    private static final String DEFAULT_PROTOCOL = "TLS";
    private static final String DEFAULT_ALGORITHM = "SunX509";
    private static final String DEFAULT_KET_STORE_TYPE = "JKS";
    private static final String PROTOCOL = "javax.net.ssl.protocol";
    private static final String ALGORITHM = "javax.net.ssl.algorithm";
    private static final String KEY_STORE = "javax.net.ssl.keyStore";
    private static final String KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String CERTIFICATE_PASSWORD = "javax.net.ssl.certificatePassword";

    private static final SSLContext SSL_CONTEXT = createSslContext();

    private volatile Delegate delegate = new NoOpDelegate();
    private boolean initDone;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        delegate.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        delegate.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        delegate.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        delegate.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (initDone)
            delegate.channelRead(ctx, msg);

        assert msg instanceof ByteBuf;
        ByteBuf msg0 = (ByteBuf) msg;

        if (!msg0.isReadable(8))
            return;

        if (msg0.getInt(msg0.readerIndex() + 4) != Constants.SSL_REQUEST) {
            initDone = true;
            delegate.channelRead(ctx, msg);
        } else {
            msg0.skipBytes(msg0.getInt(msg0.readerIndex()));

            if (SSL_CONTEXT == null)
                ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{'N'}));
            else {
                ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{'S'}))
                        .addListener(f -> delegate = new SslDelegate(createEngine()));
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        delegate.channelReadComplete(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        delegate.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        delegate.channelWritabilityChanged(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        delegate.exceptionCaught(ctx, cause);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        delegate.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        delegate.handlerRemoved(ctx);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        delegate.bind(ctx, localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        delegate.connect(ctx, remoteAddress, localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        delegate.disconnect(ctx, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        delegate.close(ctx, promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        delegate.deregister(ctx, promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        delegate.read(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        delegate.write(ctx, msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        delegate.flush(ctx);
    }

    private SSLEngine createEngine() {
        assert SSL_CONTEXT != null;

        SSLEngine sslEngine = SSL_CONTEXT.createSSLEngine();
        sslEngine.setUseClientMode(false);
        sslEngine.setNeedClientAuth(false);

        return sslEngine;
    }

    private static SSLContext createSslContext() {
        try {
            String protocol = secureProperty(PROTOCOL, DEFAULT_PROTOCOL);
            String algorithm = secureProperty(ALGORITHM, DEFAULT_ALGORITHM);
            String keyStoreType = secureProperty(KEYSTORE_TYPE, DEFAULT_KET_STORE_TYPE);
            String keyStore = secureProperty(KEY_STORE);
            String ksPassword = secureProperty(KEY_STORE_PASSWORD);
            String certPassword = secureProperty(CERTIFICATE_PASSWORD);

            KeyStore ks = KeyStore.getInstance(keyStoreType);
            try (InputStream storeStream = keyStore == null ? null : new FileInputStream(keyStore)) {
                char[] password = ksPassword == null ? null : ksPassword.toCharArray();
                ks.load(storeStream, password);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
            char[] password = certPassword == null ? null : certPassword.toCharArray();
            kmf.init(ks, password);
            KeyManager[] keyManagers = kmf.getKeyManagers();

            SSLContext ctx = SSLContext.getInstance(protocol);
            ctx.init(keyManagers, null, null);

            return ctx;
        } catch (Exception e) {
            log.error("Filed to initialize SSL context", e);

            return null;
        }
    }

    private static String secureProperty(String propertyName) {
        return Security.getProperty(propertyName);
    }

    private static String secureProperty(String propertyName, String defaultValue) {
        String property = secureProperty(propertyName);
        return property != null ? property : defaultValue;
    }
}

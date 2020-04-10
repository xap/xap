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

import com.j_spaces.kernel.ResourceLoader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * A factory class used to load SSL network filter
 *
 * @author barak
 * @since 6.6
 */

public class SSLFilterFactory implements IOFilterFactory {

    final private static Logger _logger = LoggerFactory.getLogger(SSLFilterFactory.class.getName());
    private final String PROTOCOL;
    private final String keystore;
    private final String password;
    private KeyStore ks;
    private boolean autoGenerateCertificate = false;

    public SSLFilterFactory() {
        this(System.getProperty("com.gs.lrmi.filter.security.keystore"), System.getProperty("com.gs.lrmi.filter.security.password"));
    }

    public SSLFilterFactory(String keystore, String password) {
        super();
        if (keystore == null || password == null) {
            createSelfSignedCertificate();
            autoGenerateCertificate = true;
        }
        if (ks == null && keystore == null)
            throw new IllegalArgumentException("SSL keystore cannot be null (com.gs.lrmi.filter.security.keystore)");
        if (ks == null && password == null)
            throw new IllegalArgumentException("SSL password cannot be null (com.gs.lrmi.filter.security.password)");
        this.keystore = keystore;
        this.password = password;
        this.PROTOCOL = System.getProperty("com.gs.lrmi.filter.security.protocol", "TLS");
    }

    private void createSelfSignedCertificate() {
        try {
            ks = SelfSignedCertificate.keystore();
        } catch (Throwable e) {
            if (_logger.isTraceEnabled()) {
                _logger.trace("Failed to create self signed certificate using sun classes will try Bouncy Castle.", e);
            } else if (_logger.isInfoEnabled()) {
                _logger.info("Could not create self signed certificate using sun classes - trying Bouncy Castle");
            }
            try {
                ks = BouncyCastleSelfSignedCertificate.keystore();
            } catch (Throwable t) {
                _logger.warn("Failed to create self signed certificate using Bouncy Castle classes.\n" +
                        " please add Bouncy Castle jars to classpath (or add the artifact org.bouncycastle.bcpkix-jdk15on to maven)", t);

            }
        }
    }

    public IOFilter createClientFilter(InetSocketAddress remoteAddress) throws Exception {
        return createFilter(true, remoteAddress);
    }

    public IOFilter createServerFilter(InetSocketAddress remoteAddress) throws Exception {
        return createFilter(false, remoteAddress);
    }

    private IOFilter createFilter(boolean clientMode, InetSocketAddress remoteAddress) throws Exception {
        if (_logger.isDebugEnabled())
            _logger.debug("Create a new SSL filter clientMode=" + clientMode);
        SSLContext sslContext = createSSLContext(clientMode, keystore, password);
        IOSSLFilter filter = new IOSSLFilter(sslContext, remoteAddress);
        filter.setUseClientMode(clientMode);
        return filter;
    }

    protected SSLContext createSSLContext(boolean clientMode, String keystore,
                                          String password) throws Exception {
        // Create/initialize the SSLContext with key material
        char[] passphrase = ((password == null) ? "foo".toCharArray() : password.toCharArray());
        // First initialize the key and trust material.
        KeyStore ks = loadKeyStore(keystore, passphrase);
        SSLContext sslContext = SSLContext.getInstance(PROTOCOL);

        if (clientMode) {
            if (!autoGenerateCertificate) {
                // TrustManager's decide whether to allow connections.
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ks);
                sslContext.init(null, tmf.getTrustManagers(), null);
            } else {
                sslContext.init(null, new TrustManager[]{TrustManagers.TRUST_ALL}, null);
            }
        } else {
            // KeyManager's decide which key material to use.
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            kmf.init(ks, passphrase);
            sslContext.init(kmf.getKeyManagers(), null, null);
        }
        return sslContext;
    }

    protected synchronized KeyStore loadKeyStore(String keystore, char[] passphrase) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        if (this.ks == null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            URL url = ResourceLoader.getResourceURL(keystore);
            if (url == null) {
                File f = new File(keystore);
                if (f.exists()) {
                    InputStream in = new FileInputStream(f);
                    ks.load(in, passphrase);
                    in.close();
                    return ks;
                } else {
                    throw new KeyStoreException("Could not find SSL keystore file " + keystore + " in classpath or as a direct path");
                }
            }
            InputStream is = url.openStream();
            ks.load(is, passphrase);
            is.close();
            return ks;
        }
        return this.ks;
    }

}

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

import com.gigaspaces.start.SystemInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Barak Bar Orion 2/1/15.
 */
@com.gigaspaces.api.InternalApi
public class BouncyCastleSelfSignedCertificate {
    private final static Logger logger = LoggerFactory.getLogger(BouncyCastleSelfSignedCertificate.class.getName());

    private static final Provider PROVIDER = new BouncyCastleProvider();
    static final SecureRandom random = new SecureRandom();
    static final Date NOT_BEFORE = new Date(System.currentTimeMillis() - 86400000L * 365);
    static final Date NOT_AFTER = new Date(253402300799000L);

    private final KeyStore keyStore;
    private static BouncyCastleSelfSignedCertificate instance;
    private static boolean created;

    public static synchronized KeyStore keystore() {
        if (!created) {
            created = true;
            try {
                instance = new BouncyCastleSelfSignedCertificate();
            } catch (Exception e) {
                logger.warn("Failed to create self signed certificate", e);
            }
        }
        if (instance != null) {
            return instance.getKeyStore();
        } else {
            return null;
        }
    }

    private BouncyCastleSelfSignedCertificate() throws Exception {
        this(SystemInfo.singleton().network().getHost().getCanonicalHostName(), random, 1024);
    }


    private BouncyCastleSelfSignedCertificate(String fqdn, SecureRandom random, int bits) throws Exception {
        // Generate an RSA key pair.
        final KeyPair keypair;
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(bits, random);
            keypair = keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            // Should not reach here because every Java implementation must have RSA key pair generator.
            throw new Error(e);
        }

        keyStore = generateKeyStore(fqdn, keypair, random);
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    private KeyStore generateKeyStore(String fqdn, KeyPair keypair, SecureRandom random) throws Exception {
        PrivateKey key = keypair.getPrivate();

        // Prepare the information required for generating an X.509 certificate.
        X500Name owner = new X500Name("CN=" + fqdn);
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                owner, new BigInteger(64, random), NOT_BEFORE, NOT_AFTER, owner, keypair.getPublic());

        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(key);
        X509CertificateHolder certHolder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter().setProvider(PROVIDER).getCertificate(certHolder);
        cert.verify(keypair.getPublic());

        String keyStoreType = KeyStore.getDefaultType();
        final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, null);
        keyStore.setKeyEntry("key", keypair.getPrivate(), "foo".toCharArray(), new Certificate[]{cert});
        return keyStore;
    }
}

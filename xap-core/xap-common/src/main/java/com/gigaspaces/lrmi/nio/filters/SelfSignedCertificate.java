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
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 2/1/15.
 */
@com.gigaspaces.api.InternalApi
public class SelfSignedCertificate {
    private final static Logger logger = Logger.getLogger(SelfSignedCertificate.class.getName());

    static final SecureRandom random = new SecureRandom();
    static final Date NOT_BEFORE = new Date(System.currentTimeMillis() - 86400000L * 365);
    static final Date NOT_AFTER = new Date(253402300799000L);

    private final KeyStore keyStore;
    private static SelfSignedCertificate instance;
    private static boolean created;

    public static synchronized KeyStore keystore() {
        if (!created) {
            created = true;
            try {
                instance = new SelfSignedCertificate();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to create self signed certificate", e);
            }
        }
        if (instance != null) {
            return instance.getKeyStore();
        } else {
            return null;
        }
    }

    private SelfSignedCertificate() throws Exception {
        this(SystemInfo.singleton().network().getHost().getCanonicalHostName(), random, 1024);
    }

    private SelfSignedCertificate(String fqdn, SecureRandom random, int bits) throws Exception {
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
//        String[] paths = generate(fqdn, keypair, random);
//
//        certificate = new File(paths[0]);
//        privateKey = new File(paths[1]);
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    private KeyStore generateKeyStore(String fqdn, KeyPair keypair, SecureRandom random) throws Exception {
        PrivateKey key = keypair.getPrivate();

        // Prepare the information required for generating an X.509 certificate.
        X509CertInfo info = new X509CertInfo();
        X500Name owner = new X500Name("CN=" + fqdn);
        info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
        info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(new BigInteger(64, random)));
        try {
            info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
        } catch (CertificateException ignore) {
            info.set(X509CertInfo.SUBJECT, owner);
        }
        try {
            info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
        } catch (CertificateException ignore) {
            info.set(X509CertInfo.ISSUER, owner);
        }
        info.set(X509CertInfo.VALIDITY, new CertificateValidity(NOT_BEFORE, NOT_AFTER));
        info.set(X509CertInfo.KEY, new CertificateX509Key(keypair.getPublic()));
        info.set(X509CertInfo.ALGORITHM_ID,
                new CertificateAlgorithmId(new AlgorithmId(AlgorithmId.sha1WithRSAEncryption_oid)));

        // Sign the cert to identify the algorithm that's used.
        X509CertImpl cert = new X509CertImpl(info);
        cert.sign(key, "SHA1withRSA");

        // Update the algorithm and sign again.
        info.set(CertificateAlgorithmId.NAME + '.' + CertificateAlgorithmId.ALGORITHM, cert.get(X509CertImpl.SIG_ALG));
        cert = new X509CertImpl(info);
        cert.sign(key, "SHA1withRSA");
        cert.verify(keypair.getPublic());

        String keyStoreType = KeyStore.getDefaultType();
        final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, null);
//        keyStore.setCertificateEntry("CAcert-root", cert);
        keyStore.setKeyEntry("key", keypair.getPrivate(), "foo".toCharArray(), new Certificate[]{cert});
        return keyStore;
    }
}

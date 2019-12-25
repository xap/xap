/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package com.gigaspaces.client.storage_adapters;

import com.gigaspaces.api.ExperimentalApi;
import com.j_spaces.kernel.SystemProperties;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Base Adapter for encrypting properties
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@ExperimentalApi
public abstract class AbstractCipherAdapter implements PropertyStorageAdapter {

    private final Random secureRandom = initSecureRandom();
    private final String transformation = initTransformation();
    private final String algorithm = initAlgorithm();
    private final ThreadLocal<Cipher> cipherWrapper = new ThreadLocal<>();

    @Override
    public String getName() {
        return transformation + (useBase64Wrapper() ? "-base64" : "");
    }

    @Override
    public Class<?> getStorageClass() {
        return useBase64Wrapper() ? String.class : BinaryWrapper.class;
    }

    @Override
    public Object toSpace(Object value) throws IOException {
        try {
            return wrap(encrypt(serialize(value), getSecretKey()));
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to encrypt", e);
        }
    }

    @Override
    public Object fromSpace(Object value) throws IOException, ClassNotFoundException {
        try {
            return deserialize(decrypt(unwrap(value), getSecretKey()));
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to decrypt", e);
        }
    }

    protected SecretKey getSecretKey() {
        return new SecretKeySpec(getBinaryKey(), algorithm);
    }

    protected byte[] getBinaryKey() {
        String key = System.getProperty(SystemProperties.CIPHER_KEY_SYSTEM_PROPERTY);
        if (key == null)
            throw new IllegalStateException("Key must be provided using the " + SystemProperties.CIPHER_KEY_SYSTEM_PROPERTY +" system property");
        return key.getBytes();
    }

    protected Random initSecureRandom() {
        return new SecureRandom();
    }

    protected Random getSecureRandom() {
        return secureRandom;
    }

    protected Cipher getCipher() {
        Cipher cipher = cipherWrapper.get();
        if (cipher == null) {
            try {
                cipher = Cipher.getInstance(transformation);
            } catch (Exception e) {
                throw new IllegalStateException("could not get cipher instance using '" + transformation + "'", e);
            }
            cipherWrapper.set(cipher);
            return cipherWrapper.get();
        } else {
            return cipher;
        }
    }

    protected abstract String initTransformation();

    protected abstract String initAlgorithm();

    protected abstract byte[] encrypt(byte[] data, SecretKey key) throws GeneralSecurityException;

    protected abstract byte[] decrypt(byte[] data, SecretKey key) throws GeneralSecurityException;
}

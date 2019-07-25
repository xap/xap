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


package com.gigaspaces.security.encoding;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;


/**
 * A convenient factory for handling secret keys. <p> Keys can be randomly generated using {@link
 * #generateKey(String)} or using one of the other generate key methods and supplying a key. The
 * {@link SecretKey} can then be stored as a resource using {@link #storeKey(SecretKey, File)}, and
 * loaded using the {@link #loadKey(String)}.
 *
 * <pre>
 * Example usage: Generate a key and store it into a file.
 *
 *  SecretKey key = KeyFactory.generateKey("AES");
 *  File file = new File("my-secret.key");
 *  AesKeyFactory.storeKey(key, file);
 *  ...
 *  AesKeyFactory.loadKey(file.getAbsolutePath());
 * </pre>
 * </code>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class KeyFactory {

    /**
     * logger
     */
    private static final Logger logger = Logger.getLogger(KeyFactory.class.getPackage().getName());

    /**
     * Generate a 128 bit key using the key generator of the algorithm provided.
     *
     * @param algorithm algorithm to use.
     * @return a secret key.
     * @throws EncodingException If any exception occurred while generating a key.
     */
    public static SecretKey generateKey(String algorithm) throws EncodingException {
        try {
            KeyGenerator kgen = KeyGenerator.getInstance(algorithm);
            kgen.init(128);
            SecretKey key = kgen.generateKey();
            return key;
        } catch (Exception e) {
            throw new EncodingException(e);
        }
    }

    /**
     * Generate a key based on the provided secret.
     *
     * @param key       a string to convert using UTF-8 (at least 8 bytes).
     * @param algorithm algorithm to use
     * @return a secret key
     * @throws EncodingException If any exception occurred while generating a key.
     */
    public static SecretKey generateKey(String key, String algorithm) throws EncodingException {
        try {
            SecretKey skey = new SecretKeySpec(key.getBytes("UTF-8"), algorithm);
            return skey;
        } catch (Exception e) {
            throw new EncodingException(e);
        }
    }

    /**
     * Generate a key based on a secret key from the given byte array.
     *
     * @param key       a key to use (at least 8 bytes).
     * @param algorithm algorithm to use
     * @return a secret key
     * @throws EncodingException If any exception occurred while generating a key.
     */
    public static SecretKey generateKey(byte[] key, String algorithm) throws EncodingException {
        try {
            SecretKey skey = new SecretKeySpec(key, algorithm);
            return skey;
        } catch (Exception e) {
            throw new EncodingException(e);
        }
    }

    /**
     * Loads the {@link SecretKey} file from a resource located in the classpath or jar.
     *
     * @param resourceName The full resource name. Can't be null.
     * @return the secret key stored in the specified resource.
     */
    public static SecretKey loadKey(String resourceName) {
        if (resourceName == null)
            throw new IllegalArgumentException("Unable to load a secret-key from a null resource");

        try {
            InputStream inputStream = null;
            File file = new File(resourceName);
            if (file.exists()) {
                inputStream = new FileInputStream(file);
            } else {
                inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
                if (inputStream == null) {
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest("Security key [" + resourceName + "] not located in classpath; Assuming default");
                    }
                    return null;
                }
            }

            if (logger.isLoggable(Level.CONFIG)) {
                logger.config("Security key [" + resourceName + "] located in classpath");
            }

            try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
                return (SecretKey) in.readObject();
            }
        } catch (Exception e) {
            throw new EncodingException(e);
        }
    }

    /**
     * Stores the secret key to the provided file.
     *
     * @param key  a secret key.
     * @param file a file to write to.
     */
    public static void storeKey(SecretKey key, File file) {
        try {
            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
                out.writeObject(key);
                out.flush();
            }
        } catch (Exception e) {
            throw new EncodingException(e);
        }
    }
}

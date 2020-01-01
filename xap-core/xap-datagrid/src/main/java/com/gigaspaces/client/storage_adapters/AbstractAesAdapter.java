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

import com.gigaspaces.internal.utils.GsEnv;
import com.j_spaces.kernel.SystemProperties;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;
import java.util.Random;

/**
 * Base class for implementing adapters which encrypt properties using AES.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public abstract class AbstractAesAdapter implements PropertyStorageAdapter {
    protected static final byte[] EMPTY = new byte[0];

    private final Random secureRandom = initSecureRandom();
    private final String transformation = initTransformation();
    private final String encryptionAlgorithm = initEncryptionAlgorithm();
    private final int encryptionKeyLength = initEncryptionKeyLength();
    private final int ivLength = initIvLengthBytes();
    private final boolean macEnabled = initMacEnabled();
    private final String macAlgorithm = initMacAlgorithm();
    private final int macKeyLength = initMacKeyLength();
    private final ThreadLocal<Cipher> cipherWrapper = new ThreadLocal<>();
    private final ThreadLocal<Mac> macWrapper = new ThreadLocal<>();

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
            return wrap(encrypt(serialize(value)));
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to encrypt", e);
        }
    }

    @Override
    public Object fromSpace(Object value) throws IOException, ClassNotFoundException {
        try {
            return deserialize(decrypt(unwrap(value)));
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to decrypt", e);
        }
    }

    /**
     * Returns the pass phrase which is used to generate the encryption key and mac key.
     */
    protected byte[] getPassPhrase() {
        String key = GsEnv.property(SystemProperties.AES_PASSPHRASE).get();
        if (key == null)
            throw new IllegalStateException("Passphrase must be provided using the " + SystemProperties.AES_PASSPHRASE +" system property");
        return key.getBytes();
    }

    protected Random getSecureRandom() {
        return secureRandom;
    }

    protected Random initSecureRandom() {
        return new SecureRandom();
    }

    /**
     * Provides the cipher transformation string.
     */
    protected abstract String initTransformation();

    /**
     * Provides the initialization vector length, in bytes.
     */
    protected abstract int initIvLengthBytes();

    /**
     * Provides the encryption algorithm.
     */
    protected String initEncryptionAlgorithm() {
        return "AES";
    }

    /**
     * Provides the encryption key length, in bytes.
     */
    protected int initEncryptionKeyLength() {
        return GsEnv.propertyInt(SystemProperties.AES_KEY_LENGTH).get(16);
    }

    /**
     * Encrypts the provided plainText, and packs it with the iv and mac.
     */
    protected byte[] encrypt(byte[] plainText) throws GeneralSecurityException {
        // Generate one-time Initialization Vector:
        byte[] iv = generateInitializationVector(ivLength);
        // Encrypt:
        byte[] cipherText = cipher(iv, plainText, Cipher.ENCRYPT_MODE);
        // Generate MAC:
        byte[] mac = generateMac(cipherText, iv);
        // Pack IV + mac + cipherText
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + iv.length + 1 + mac.length + cipherText.length);
        writeArrayWithByteLength(byteBuffer, iv);
        writeArrayWithByteLength(byteBuffer, mac);
        writeArrayRemaining(byteBuffer, cipherText);
        return byteBuffer.array();
    }

    /**
     * Unpacks the data to iv, ciphertext, mac, validates and decrypts.
     */
    protected byte[] decrypt(byte[] data) throws GeneralSecurityException {
        // Unpack:
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byte[] iv = readArrayWithByteLength(byteBuffer);
        byte[] mac = readArrayWithByteLength(byteBuffer);
        byte[] cipherText = readArrayRemaining(byteBuffer);
        // Verify mac:
        byte[] actualMac = generateMac(cipherText, iv);
        // Important: use constant time comparison to protect against timing attacks. See: https://codahale.com/a-lesson-in-timing-attacks/
        if (!MessageDigest.isEqual(mac, actualMac)) {
            throw new GeneralSecurityException("encryption integrity exception: mac does not match");
        }
        // Decrypt:
        return cipher(iv, cipherText, Cipher.DECRYPT_MODE);
    }

    /**
     * Generate initialization vector for encryption.
     * NOTE: Security best practices dictate that you should not reuse an IV with the same key, as this can be exploited
     * to discover the key, hence the default implementation produces a random IV. As a result, the encryption output is
     * not deterministic, which means encrypted properties cannot be used in queries. If querying is required and the
     * security risk is acceptable for your use case, you can override this property and provide a deterministic initialization
     * vector (constant or content-related).
     */
    protected byte[] generateInitializationVector(int length) {
        byte[] iv = new byte[length];
        getSecureRandom().nextBytes(iv);
        return iv;
    }

    protected byte[] cipher(byte[] iv, byte[] input, int mode) throws GeneralSecurityException {
        SecretKey key = generateSubKey("gs.key.encrypt", encryptionKeyLength, encryptionAlgorithm);
        Cipher cipher = getOrCreateCipher();
        cipher.init(mode, key, toParameterSpec(iv));
        return cipher.doFinal(input);
    }

    protected Cipher getOrCreateCipher() throws GeneralSecurityException {
        Cipher cipher = cipherWrapper.get();
        if (cipher == null) {
            cipher = Cipher.getInstance(transformation);
            cipherWrapper.set(cipher);
        }
        return cipher;
    }

    protected AlgorithmParameterSpec toParameterSpec(byte[] iv) {
        return new IvParameterSpec(iv);
    }

    // MAC-related methods

    protected abstract boolean initMacEnabled();

    protected String initMacAlgorithm() {
        return GsEnv.property(SystemProperties.AES_MAC_ALGORITHM).get("HmacSHA256");
    }

    protected int initMacKeyLength() {
        return GsEnv.propertyInt(SystemProperties.AES_MAC_KEY_LENGTH).get(32);
    }

    protected byte[] generateMac(byte[] cipherText, byte[] iv) throws GeneralSecurityException {
        if (!macEnabled)
            return EMPTY;
        SecretKey key = generateSubKey("gs.key.mac", macKeyLength, macAlgorithm);
        Mac mac = getOrCreateMac();
        mac.init(key);
        mac.update(iv);
        mac.update(cipherText);
        return mac.doFinal();
    }

    protected Mac getOrCreateMac() throws GeneralSecurityException {
        Mac mac = macWrapper.get();
        if (mac == null) {
            mac = Mac.getInstance(macAlgorithm);
            macWrapper.set(mac);
        }
        return mac;
    }

    protected SecretKey generateSubKey(String subKey, int length, String algorithm) throws GeneralSecurityException {
        byte[] derivedKey = generateSubKey(getPassPhrase(), length, null, subKey.getBytes());
        return new SecretKeySpec(derivedKey, algorithm);
    }

    protected byte[] generateSubKey(byte[] rawKey, int length, byte[] salt, byte[] subKey)
            throws GeneralSecurityException {

        Mac mac = getOrCreateMac();
        final int hashLengthBytes = mac.getMacLength();

        int counter = 1;
        int outputLenSum = 0;

        if (salt == null)
            salt = new byte[getBlockLengthByte(mac.getAlgorithm())];
        mac.init(new SecretKeySpec(salt, mac.getAlgorithm()));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        int reps = (int) Math.ceil((float) length / (float) hashLengthBytes);

        do {
            mac.reset();
            mac.update(ByteBuffer.allocate(4).putInt(counter).array());
            mac.update(rawKey);
            mac.update(subKey);

            buffer.put(mac.doFinal(), 0, reps == counter ? length - outputLenSum : hashLengthBytes);
            outputLenSum += hashLengthBytes;
        } while (counter++ < reps);

        return buffer.array();
    }

    private static int getBlockLengthByte(String algorithm) {
        String name = Objects.requireNonNull(algorithm).toLowerCase().trim().replace("-", "");

        if (name.startsWith("sha1") || name.startsWith("sha224") || name.startsWith("sha256")
                || name.startsWith("hmacsha1") || name.startsWith("hmacsha256")) {
            return 64;
        } else if (name.startsWith("sha512") || name.startsWith("sha384") || name.equals("sha256")
                || name.startsWith("hmacsha512")) {
            return 128;
        } else if (name.startsWith("sha3224")) {
            return 144;
        } else if (name.startsWith("sha3256")) {
            return 136;
        } else if (name.startsWith("sha3384")) {
            return 104;
        } else if (name.startsWith("sha3512")) {
            return 72;
        } else {
            throw new IllegalStateException("unknown hash algorithm; cannot choose input block length: see NIST SP 800-56C REV. 1 Table 2.");
        }
    }

    // Helper methods for packing/unpacking cipher-related arrays to a binary payload

    protected static void writeArrayWithByteLength(ByteBuffer byteBuffer, byte[] array) {
        byteBuffer.put((byte) array.length);
        if (array.length != 0)
            byteBuffer.put(array);
    }

    protected static byte[] readArrayWithByteLength(ByteBuffer byteBuffer) {
        int length = (byteBuffer.get() & 0xFF);
        if (length == 0)
            return EMPTY;
        byte[] array = new byte[length];
        byteBuffer.get(array);
        return array;
    }

    protected static void writeArrayRemaining(ByteBuffer byteBuffer, byte[] array) {
        byteBuffer.put(array);
    }

    protected static byte[] readArrayRemaining(ByteBuffer byteBuffer) {
        byte[] array = new byte[byteBuffer.remaining()];
        byteBuffer.get(array);
        return array;
    }
}

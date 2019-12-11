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
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.*;

/**
 * Adapter for encrypting properties using AES/GCM.
 *
 * Based on https://proandroiddev.com/security-best-practices-symmetric-encryption-with-aes-in-java-7616beaaade9
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@ExperimentalApi
public class AesGcmCipherAdapter extends AbstractCipherAdapter {
    private final int TAG_LENGTH_BIT = initTagLengthBit();
    private final int IV_LENGTH_BYTE = initIvLengthBytes();

    protected int initTagLengthBit() {
        return Integer.getInteger(SystemProperties.CIPHER_TAG_LENGTH_SYSTEM_PROPERTY, 128);
    }

    protected int initIvLengthBytes() {
        return Integer.getInteger(SystemProperties.CIPHER_IV_LENGTH_SYSTEM_PROPERTY, 12);
    }

    @Override
    protected String initTransformation() {
        return "AES/GCM/NoPadding";
    }

    @Override
    protected String initAlgorithm() {
        return "AES";
    }

    @Override
    protected byte[] encrypt(byte[] data, SecretKey key) throws GeneralSecurityException {
        // Generate one-time Initialization Vector:
        byte[] iv = generateInitializationVector();
        // Encrypt:
        byte[] cipherText = cipher(key, iv, Cipher.ENCRYPT_MODE, data);
        // Package iv.length + iv + cipherText:
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + iv.length + cipherText.length);
        byteBuffer.putInt(iv.length);
        byteBuffer.put(iv);
        byteBuffer.put(cipherText);
        return byteBuffer.array();
    }

    @Override
    protected byte[] decrypt(byte[] data, SecretKey key) throws GeneralSecurityException {
        // Unpack iv.length + iv + cipherText:
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        int ivLength = byteBuffer.getInt();
        if (ivLength < 12 || ivLength >= 16) {
            throw new IllegalArgumentException("invalid iv length: " + ivLength);
        }
        byte[] iv = new byte[ivLength];
        byteBuffer.get(iv);
        byte[] cipherText = new byte[byteBuffer.remaining()];
        byteBuffer.get(cipherText);
        // Decrypt:
        return cipher(key, iv, Cipher.DECRYPT_MODE, cipherText);
    }

    /**
     * Generate initialization vector - NEVER REUSE THIS IV WITH SAME KEY!
     */
    protected byte[] generateInitializationVector() {
        byte[] iv = new byte[IV_LENGTH_BYTE];
        getSecureRandom().nextBytes(iv);
        return iv;
    }

    protected byte[] cipher(SecretKey key, byte[] iv, int mode, byte[] input) throws GeneralSecurityException {
        Cipher cipher = getCipher();
        cipher.init(mode, key, new GCMParameterSpec(TAG_LENGTH_BIT, iv));
        return cipher.doFinal(input);
    }
}

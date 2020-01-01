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

import javax.crypto.spec.GCMParameterSpec;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Adapter for encrypting properties using AES/GCM.
 *
 * Based on https://proandroiddev.com/security-best-practices-symmetric-encryption-with-aes-in-java-7616beaaade9
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public class AesGcmAdapter extends AbstractAesAdapter {
    private final int TAG_LENGTH_BIT = initTagLengthBit();

    protected int initTagLengthBit() {
        return GsEnv.propertyInt(SystemProperties.AES_GCM_TAG_LENGTH).get(128);
    }

    @Override
    protected String initTransformation() {
        return "AES/GCM/NoPadding";
    }

    @Override
    protected int initIvLengthBytes() {
        return GsEnv.propertyInt(SystemProperties.AES_GCM_IV_LENGTH).get(12);
    }

    /**
     * Mac is not required for AES/GCM.
     */
    @Override
    protected boolean initMacEnabled() {
        return false;
    }

    @Override
    protected AlgorithmParameterSpec toParameterSpec(byte[] iv) {
        return new GCMParameterSpec(TAG_LENGTH_BIT, iv);
    }
}

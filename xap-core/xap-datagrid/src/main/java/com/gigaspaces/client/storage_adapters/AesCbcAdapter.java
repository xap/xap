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

/**
 * Adapter for encrypting properties using AES/CBC.
 *
 * Based on https://proandroiddev.com/security-best-practices-symmetric-encryption-with-aes-in-java-and-android-part-2-b3b80e99ad36
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public class AesCbcAdapter extends AbstractAesAdapter {

    @Override
    protected int initIvLengthBytes() {
        return GsEnv.propertyInt(SystemProperties.AES_CBC_IV_LENGTH).get(16);
    }

    @Override
    protected String initTransformation() {
        return "AES/CBC/" + initPadding();
    }

    protected String initPadding() {
        return GsEnv.property(SystemProperties.AES_CBC_PADDING).get("PKCS5Padding");
    }

    @Override
    protected boolean initMacEnabled() {
        return true;
    }
}

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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SourceChannelConfig implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    public static final long UNLIMITED = -1;

    private long _maxAllowedDisconnectionTimeBeforeDrop = UNLIMITED;


    public SourceChannelConfig() {
    }

    public SourceChannelConfig(long maxAllowedDisconnectionTimeBeforeDrop) {
        _maxAllowedDisconnectionTimeBeforeDrop = maxAllowedDisconnectionTimeBeforeDrop;
    }

    public long getMaxAllowedDisconnectionTimeBeforeDrop() {
        return _maxAllowedDisconnectionTimeBeforeDrop;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_maxAllowedDisconnectionTimeBeforeDrop);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _maxAllowedDisconnectionTimeBeforeDrop = in.readLong();
    }

}

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
package com.gigaspaces.server.space.suspend;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.*;

/**
 * Represents current suspend state of the space.
 * @author yohanakh
 * @since 14.0.0
 **/
public class SuspendInfo implements SmartExternalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private SuspendType suspendType;

    public SuspendInfo() {
    }

    public SuspendInfo(SuspendType suspendType) {
        this.suspendType = suspendType;
    }


    public SuspendType getSuspendType() {
        return suspendType;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(suspendType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        suspendType = (SuspendType) in.readObject();
    }
}

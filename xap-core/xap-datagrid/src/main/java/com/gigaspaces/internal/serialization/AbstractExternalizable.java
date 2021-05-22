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

package com.gigaspaces.internal.serialization;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Base class for externalizable classes in gigaspaces, providing forward/backward compatibility
 * info.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class AbstractExternalizable implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    protected AbstractExternalizable() {
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        writeExternalImpl(out);
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        readExternalImpl(in);
    }

    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
    }

    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
    }
}

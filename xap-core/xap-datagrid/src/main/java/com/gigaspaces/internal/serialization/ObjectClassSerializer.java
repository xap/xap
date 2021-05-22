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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Serializer for Object.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ObjectClassSerializer implements IClassSerializer<Object> {
    private static final Logger logger = LoggerFactory.getLogger(ObjectClassSerializer.class);

    public static final ObjectClassSerializer instance = new ObjectClassSerializer();

    private ObjectClassSerializer() {
    }

    public byte getCode() {
        return CODE_OBJECT;
    }

    public Object read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readObject();
    }

    public void write(ObjectOutput out, Object obj)
            throws IOException {
        if (logger.isDebugEnabled())
            logger.debug("serializing {}", obj.getClass());
        out.writeObject(obj);
    }
}

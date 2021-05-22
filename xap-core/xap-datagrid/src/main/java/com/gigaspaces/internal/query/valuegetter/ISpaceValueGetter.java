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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;

/**
 * Represents an abstraction of getting a value from a target object.
 *
 * @param <T> Type of Target object.
 * @author Niv Ingberg
 * @since 7.1
 */
public interface ISpaceValueGetter<T> extends SmartExternalizable, ISwapExternalizable {
    /**
     * Gets the value from the specified target.
     *
     * @param target Target object to get value from.
     * @return Retrieved value.
     */
    Object getValue(T target);
}

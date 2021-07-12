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
package com.gigaspaces.client.internal;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.api.InternalApi;
import com.gigaspaces.client.ReadModifiers;
import com.j_spaces.core.client.Modifiers;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@InternalApi
public class InternalReadModifiers extends ReadModifiers {
    static final long serialVersionUID = -7615654650885502826L;
    protected InternalReadModifiers(int code) {
        super(code);

    }
    /**
     * Indicates operation should run directly on i/o thread, instead of being submitted to the operations thread pool.
     * This is useful for short, non-blocking operations (e.g. readById), but risky for long or non-blocking operations.
     * NOTE: This is an experimental API, subject to breaking changes in future releases.
     * @since 16.0
     */
    @ExperimentalApi
    public static final ReadModifiers RUN_ON_IO_THREAD = new InternalReadModifiers(Modifiers.RUN_ON_IO_THREAD);
}

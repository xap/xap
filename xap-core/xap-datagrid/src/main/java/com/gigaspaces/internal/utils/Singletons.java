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

package com.gigaspaces.internal.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class Singletons {
    private static final Map<String, Object> _instances = new HashMap<String, Object>();
    private static final Object _lock = new Object();
    // static member to prevent gc of class.
    private static final Singletons _instance = new Singletons();

    /**
     * private ctor to prevent instantiation
     */
    private Singletons() {
    }

    public static Object get(String name) {
        synchronized (_lock) {
            return _instances.get(name);
        }
    }

    public static Object putIfAbsent(String name, Object newValue) {
        synchronized (_lock) {
            if (_instances.containsKey(name))
                return _instances.get(name);
            _instances.put(name, newValue);
            return newValue;
        }
    }

    public static void remove(String name) {
        synchronized (_lock) {
            _instances.remove(name);
        }
    }

    /**
     * The usage of this method is to ensure that a remove followed by putIfAbsent
     * is safe from race conditions between the two calls.
     *
     * Acts like compare AND set, but with removal. Will remove the key if the
     * value matches the existing value in the map.
     *
     * @return the existing value mapped to this key before removal
     */
    public static Object compareAndRemove(String name, Object value) {
        synchronized (_lock) {
            final Object existing = _instances.get(name);
            if (existing == value) {
                return _instances.remove(name);
            } else {
                return existing;
            }
        }
    }
}

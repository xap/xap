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

import java.io.IOException;

/**
 * Adapter for serializing properties using standard serialization and storing them in space in binary form.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@ExperimentalApi
public class BinaryAdapter implements PropertyStorageAdapter {

    @Override
    public String getName() {
        return "Binary";
    }

    @Override
    public Object toSpace(Object value) throws IOException {
        return wrap(serialize(value));
    }

    @Override
    public Object fromSpace(Object value) throws IOException, ClassNotFoundException {
        return deserialize(unwrap(value));
    }
}

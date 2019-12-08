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


package com.gigaspaces.internal.io;

import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class ContextClassResolverObjectInputStream extends ObjectInputStream {

    protected ContextClassResolverObjectInputStream(InputStream in) throws IOException, SecurityException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
        String name = classDesc.getName();
        return ClassLoaderHelper.loadClass(name);
    }

    public static class Factory implements ObjectInputStreamFactory {

        public static Factory instance = new Factory();

        @Override
        public ObjectInputStream create(InputStream is) throws IOException {
            return new ContextClassResolverObjectInputStream(is);
        }
    }
}

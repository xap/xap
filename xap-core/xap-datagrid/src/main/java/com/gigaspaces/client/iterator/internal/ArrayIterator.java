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
package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.api.InternalApi;

import java.util.Iterator;

@InternalApi
public class ArrayIterator<T> implements Iterator<T> {
    private final Object[] array;
    private int pos;

    public static <T> ArrayIterator<T> wrap(Object[] array) {
        if (array == null)
            return null;
        return new ArrayIterator<>(array);
    }

    private ArrayIterator(Object[] array) {
        this.array = array;
    }

    @Override
    public boolean hasNext() {
        return pos < array.length;
    }

    @Override
    public T next() {
        return (T)array[pos++];
    }
}

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
package com.j_spaces.kernel.list;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;

public class CircularNumerator<T> {
    private final Iterator<T> iterator;
    private volatile T current;

    public CircularNumerator(T[] entries, int start) {
        if(entries == null)
            throw new IllegalArgumentException("entries cannot be null");
        iterator = createIteratorFromRandomStartPoint(entries, start);
        next();
    }

    public T getCurrent() {
        return current;
    }

    public void next(){
        current = iterator.hasNext() ? iterator.next() : null;
    }

    private Iterator<T> createIteratorFromRandomStartPoint(T[] entries, int start){
        int length = entries.length;
        if(length == 0)
            return new ArrayList<T>().iterator();
        if(start == 0)
            return Arrays.asList(entries).iterator();
        List<T> result = new ArrayList<>();
        for (int i = 0; i < length; i++, start++) {
            if(start >= length)
                start = 0;
            result.add(entries[start]);
        }
        return result.iterator();
    }
}

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

package com.gigaspaces.client.iterator.server_based;

import java.util.HashMap;
import java.util.Map;

public class SpaceIteratorException
        extends RuntimeException {
    private static final long serialVersionUID = -6396162967502290947L;

    private final Map<Integer, Exception> _exceptions = new HashMap<>();

    public SpaceIteratorException(String message) {
        super(message);
    }

    public Map<Integer, Exception> getExceptions() {
        return _exceptions;
    }

    public SpaceIteratorException addException(Integer partitionId, Exception e){
        _exceptions.put(partitionId, e);
        return this;
    }

    @Override
    public String getMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(super.getMessage());
        stringBuilder.append("[");
        for(Exception e: _exceptions.values()){
            stringBuilder.append(e.toString());
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(","));
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
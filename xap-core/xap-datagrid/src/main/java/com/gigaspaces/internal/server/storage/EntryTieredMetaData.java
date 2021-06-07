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
package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.cache.context.TieredState;

import java.io.Serializable;

public class EntryTieredMetaData implements Serializable {
    private TieredState tieredState;
    private boolean isIdenticalToCache;

    public EntryTieredMetaData() {
    }

    public TieredState getTieredState() {
        return tieredState;
    }

    public void setTieredState(TieredState tieredState) {
        this.tieredState = tieredState;
    }

    public boolean isIdenticalToCache() {
        return isIdenticalToCache;
    }

    public void setIdenticalToCache(boolean identicalToCache) {
        isIdenticalToCache = identicalToCache;
    }

    public boolean isExist(){
        return tieredState != null;
    }
}

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
package com.j_spaces.core.cache.context;

import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.metrics.LongCounter;
import com.j_spaces.core.cache.TypeData;

/**
 * Allows storing index hits
 *
 * @since 15.5.0
 */
@com.gigaspaces.api.InternalApi
public class IndexMetricsContext {

    private boolean ignoreUpdates;
    private final TypeData typeData;

    public IndexMetricsContext(TypeData typeData) {
        this.typeData = typeData;
    }

    public void setIgnoreUpdates(boolean ignoreUpdates) {
        this.ignoreUpdates = ignoreUpdates;
    }

    public void addChosenIndex(IQueryIndexScanner index) {
        if (!ignoreUpdates) {
            typeData.getIndex(index.getIndexName()).getUsageCounter().inc();
        }
    }

    public void addChosenIndex(LongCounter indexCounter) {
        if (!ignoreUpdates) {
            indexCounter.inc();
        }
    }
}
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Allows storing index hits
 *
 * @since 15.5.0
 */
@com.gigaspaces.api.InternalApi
public class IndexMetricsContext {

    private Set<String> chosenIndex = new HashSet<>(); //key is indexName
    private boolean ignoreUpdates;
    private String dataTypeName;

    public IndexMetricsContext(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public void setIgnoreUpdates(boolean ignoreUpdates) {
        this.ignoreUpdates = ignoreUpdates;
    }

    public void addChosenIndex(String indexName) {
        if (!isIgnoreUpdates()) {
            chosenIndex.add(indexName);
        }
    }

    /** @return true if no index was chosen */
    public boolean isEmpty() {
        return chosenIndex.isEmpty();
    }

    public Set<String> getChosenIndexes() {
        return Collections.unmodifiableSet(chosenIndex);
    }

    public boolean isIgnoreUpdates() {
        return ignoreUpdates;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }
}
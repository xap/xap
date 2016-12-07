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

package com.gigaspaces.query.extension.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class QueryExtensionPropertyInfo {
    private final Map<String, QueryExtensionPathInfo> pathsInfo = new HashMap<String, QueryExtensionPathInfo>();

    public Set<String> getPaths() {
        return pathsInfo.keySet();
    }

    public QueryExtensionPathInfo getPathInfo(String path) {
        return pathsInfo.get(path);
    }

    public void addPathInfo(String path, QueryExtensionPathInfo queryExtensionPathInfo) {
        pathsInfo.put(path, queryExtensionPathInfo);
    }
}

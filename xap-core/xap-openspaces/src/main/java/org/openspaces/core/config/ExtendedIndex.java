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

package org.openspaces.core.config;

/**
 *
 * @deprecated since 12.3 use OrderedIndex or EqualOrdered instead
 */
@Deprecated
public class ExtendedIndex extends SpaceIndex {

    public ExtendedIndex() {
        super();
    }

    public ExtendedIndex(String path) {
        super(path);
    }

    public ExtendedIndex(boolean unique) {
        super(unique);
    }

    public ExtendedIndex(String indexPropertyPath, boolean unique) {
        super(indexPropertyPath, unique);
    }


}

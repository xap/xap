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

package com.j_spaces.core.cache.blobStore.sadapter;

import com.j_spaces.core.cache.TypeData;

import java.util.HashMap;
import java.util.HashSet;

/**
 * classes used (in i/o ops) in off-heap storage adapter
 *
 * @author yechiel
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreStorageAdapterClasses {
    private volatile HashMap<String, BlobStoreStorageAdapterClassInfo> _classes;   //used classes

    public BlobStoreStorageAdapterClasses() {
        _classes = new HashMap<String, BlobStoreStorageAdapterClassInfo>();

    }

    public BlobStoreStorageAdapterClassInfo get(String className) {
        return _classes.get(className);
    }


    //NOTE- locked from outside (by SA)
    public void put(String className, BlobStoreStorageAdapterClassInfo classInfo) {
        HashMap<String, BlobStoreStorageAdapterClassInfo> classes = new HashMap<String, BlobStoreStorageAdapterClassInfo>(_classes);
        classes.put(className, classInfo);
        _classes = classes;
    }

    //is the typeData indexes identical or contained in the classInfo BlobStoreStorageAdapterClassInfo
    //NOTE- locked from outside (by SA)
    public boolean isContained(String className, TypeData typeData) {
        BlobStoreStorageAdapterClassInfo cur = get(className);
        boolean[] newf = typeData.getIndexesRelatedFixedProperties();
        for (int i = 0; i < newf.length; i++) {
            if (newf[i] && !cur.getIndexesRelatedFixedProperties()[i])
                return false;
        }

        HashSet<String> newd = typeData.getIndexesRelatedDynamicProperties();
        if (newd == null || newd.size() == 0)
            return true;

        for (String val : newd) {
            if (cur.getIndexesRelatedDynamicProperties() == null || !cur.getIndexesRelatedDynamicProperties().contains(val))
                return false;
        }
        return true;
    }

}

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

package com.gigaspaces.metadata.pojos;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceStorageType;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndexType;

@com.gigaspaces.api.InternalApi
public class PojoStorageTypeWithOtherAnnotations {
    Object id;
    Object routing;

    Object object1;
    Object object2;
    int object3;

    Object binary1;
    Object binary2;

    Object compressed1;
    Object compressed2;

    @SpaceId
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    @SpaceRouting
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object getRouting() {
        return routing;
    }

    public void setRouting(Object routing) {
        this.routing = routing;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object getObject1() {
        return object1;
    }

    public void setObject1(Object object1) {
        this.object1 = object1;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object getObject2() {
        return object2;
    }

    public void setObject2(Object object2) {
        this.object2 = object2;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.CONVERT, nullValue = "0")
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public int getObject3() {
        return object3;
    }

    public void setObject3(int object3) {
        this.object3 = object3;
    }

    @SpaceIndex(type = SpaceIndexType.NONE)
    @SpaceStorageType(storageType = StorageType.BINARY)
    public Object getBinary1() {
        return binary1;
    }

    public void setBinary1(Object binary1) {
        this.binary1 = binary1;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.CONVERT)
    @SpaceStorageType(storageType = StorageType.BINARY)
    public Object getBinary2() {
        return binary2;
    }

    public void setBinary2(Object binary2) {
        this.binary2 = binary2;
    }

    @SpaceIndex(type = SpaceIndexType.NONE)
    @SpaceStorageType(storageType = StorageType.COMPRESSED)
    public Object getCompressed1() {
        return compressed1;
    }

    public void setCompressed1(Object compressed1) {
        this.compressed1 = compressed1;
    }

    @SpaceIndex(type = SpaceIndexType.NONE)
    @SpaceStorageType(storageType = StorageType.COMPRESSED)
    public Object getCompressed2() {
        return compressed2;
    }

    public void setCompressed2(Object compressed2) {
        this.compressed2 = compressed2;
    }
}

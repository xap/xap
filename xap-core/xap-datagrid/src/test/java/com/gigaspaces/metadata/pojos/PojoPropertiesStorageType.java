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

import com.gigaspaces.annotation.pojo.SpaceDynamicProperties;
import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceLeaseExpiration;
import com.gigaspaces.annotation.pojo.SpacePersist;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceStorageType;
import com.gigaspaces.annotation.pojo.SpaceVersion;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@com.gigaspaces.api.InternalApi
public class PojoPropertiesStorageType {
    // primitives no storageType annotation
    Object id;
    Object routing;
    // primitives with storageType annotation
    int someInt;
    long someLong;
    boolean someBoolean;
    // non-primitives with storageType annotation
    Object _objectStorageTypeObject;
    List<String> _binaryStorageTypeList;
    Map<String, Object> _compressedStorageTypeMap;
    ArrayList<Object> _defaultStorageTypeArrayList;
    // space excluded properties
    Object spaceExcludeObject;
    Map<String, Object> dynamicPropertiesMap;
    boolean isPersistent;
    int version;
    long leasExpiration;
    // indexed properties
    Object basicIndexObject;
    Object ExtendedIndexObject;
    Object noneIndexObject;
    Object pathIndexObject;

    @SpaceId
    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    @SpaceRouting
    public Object getRouting() {
        return routing;
    }

    public void setRouting(Object routing) {
        this.routing = routing;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public int getSomeInt() {
        return someInt;
    }

    public void setSomeInt(int someInt) {
        this.someInt = someInt;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public long getSomeLong() {
        return someLong;
    }

    public void setSomeLong(long someLong) {
        this.someLong = someLong;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public boolean isSomeBoolean() {
        return someBoolean;
    }

    public void setSomeBoolean(boolean someBoolean) {
        this.someBoolean = someBoolean;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object get_objectStorageTypeObject() {
        return _objectStorageTypeObject;
    }

    public void set_objectStorageTypeObject(Object _objectStorageTypeObject) {
        this._objectStorageTypeObject = _objectStorageTypeObject;
    }

    @SpaceStorageType(storageType = StorageType.BINARY)
    public List<String> get_binaryStorageTypeList() {
        return _binaryStorageTypeList;
    }

    public void set_binaryStorageTypeList(List<String> _binaryStorageTypeList) {
        this._binaryStorageTypeList = _binaryStorageTypeList;
    }

    @SpaceStorageType(storageType = StorageType.COMPRESSED)
    public Map<String, Object> get_compressedStorageTypeMap() {
        return _compressedStorageTypeMap;
    }

    public void set_compressedStorageTypeMap(Map<String, Object> _compressedStorageTypeMap) {
        this._compressedStorageTypeMap = _compressedStorageTypeMap;
    }

    @SpaceStorageType(storageType = StorageType.DEFAULT)
    public ArrayList<Object> get_defaultStorageTypeArrayList() {
        return _defaultStorageTypeArrayList;
    }

    public void set_defaultStorageTypeArrayList(ArrayList<Object> _defaultStorageTypeArrayList) {
        this._defaultStorageTypeArrayList = _defaultStorageTypeArrayList;
    }

    @SpaceExclude
    public Object getSpaceExcludeObject() {
        return spaceExcludeObject;
    }

    public void setSpaceExcludeObject(Object spaceExcludeObject) {
        this.spaceExcludeObject = spaceExcludeObject;
    }

    @SpaceDynamicProperties
    public Map<String, Object> getDynamicPropertiesMap() {
        return dynamicPropertiesMap;
    }

    public void setDynamicPropertiesMap(Map<String, Object> dynamicPropertiesMap) {
        this.dynamicPropertiesMap = dynamicPropertiesMap;
    }

    @SpacePersist
    public boolean isPersistent() {
        return isPersistent;
    }

    public void setPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    @SpaceVersion
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @SpaceLeaseExpiration
    public long getLeasExpiration() {
        return leasExpiration;
    }

    public void setLeasExpiration(long leasExpiration) {
        this.leasExpiration = leasExpiration;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public Object getBasicIndexObject() {
        return basicIndexObject;
    }

    public void setBasicIndexObject(Object basicIndexObject) {
        this.basicIndexObject = basicIndexObject;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Object getExtendedIndexObject() {
        return ExtendedIndexObject;
    }

    public void setExtendedIndexObject(Object extendedIndexObject) {
        ExtendedIndexObject = extendedIndexObject;
    }

    @SpaceIndex(type = SpaceIndexType.NONE)
    public Object getNoneIndexObject() {
        return noneIndexObject;
    }

    public void setNoneIndexObject(Object noneIndexObject) {
        this.noneIndexObject = noneIndexObject;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path")
    public Object getPathIndexObject() {
        return pathIndexObject;
    }

    public void setPathIndexObject(Object pathIndexObject) {
        this.pathIndexObject = pathIndexObject;
    }


}

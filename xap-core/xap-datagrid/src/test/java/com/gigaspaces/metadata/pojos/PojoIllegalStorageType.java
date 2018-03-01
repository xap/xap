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

import java.util.List;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class PojoIllegalStorageType {

    public class DeclareStorageType {
        public class ObjectType {
            public class WithSpaceExcludeAnnotation {
                private Object object;

                @SpaceExclude
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }

            }

            public class WithSpaceDynamicPropertiesAnnotation {
                private Map<String, Object> map;

                @SpaceDynamicProperties
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Map<String, Object> getObject() {
                    return map;
                }

                public void setObject(Map<String, Object> map) {
                    this.map = map;
                }
            }

            public class WithSpacePersistAnnotation {
                Object object;

                @SpacePersist
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }
            }

            public class WithSpaceVesionAnnotation {
                Object object;

                @SpaceVersion
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }
            }

            public class WithSpaceLeaseExpirationAnnotation {
                Object object;

                @SpaceLeaseExpiration
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }
            }

            public class InheritSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public Map<String, Object> getDynamicPropertiesMap() {
                    // TODO Auto-generated method stub
                    return super.getDynamicPropertiesMap();
                }
            }

            public class InheritSpacePersist extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public boolean isPersistent() {
                    // TODO Auto-generated method stub
                    return super.isPersistent();
                }
            }

            public class InheritSpaceVesion extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public int getVersion() {
                    // TODO Auto-generated method stub
                    return super.getVersion();
                }
            }

            public class InheritSpaceLeaseExpiration extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.OBJECT)
                public long getLeasExpiration() {
                    // TODO Auto-generated method stub
                    return super.getLeasExpiration();
                }
            }
        }

        public class BinaryType {
            public class OnIntegerTypeProperty {
                Integer intType;

                @SpaceStorageType(storageType = StorageType.BINARY)
                public Integer getIntType() {
                    return intType;
                }

                public void setIntType(Integer intType) {
                    this.intType = intType;
                }
            }

            public class OnStringTypeProperty {
                String stringType;

                @SpaceStorageType(storageType = StorageType.BINARY)
                public String getStringType() {
                    return stringType;
                }

                public void setStringType(String stringType) {
                    this.stringType = stringType;
                }
            }

            public class OnBooleanTypeProperty {
                Boolean booleanType;

                @SpaceStorageType(storageType = StorageType.BINARY)
                public Boolean getBooleanType() {
                    return booleanType;
                }

                public void setBooleanType(Boolean booleanType) {
                    this.booleanType = booleanType;
                }
            }

            public class WithSpaceIdAnnotation {
                Object id;

                @SpaceId
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Object getId() {
                    return id;
                }

                public void setId(Object id) {
                    this.id = id;
                }
            }

            public class WithSpaceRoutingAnnotation {
                Object routing;

                @SpaceRouting
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Object getRouting() {
                    return routing;
                }

                public void setRouting(Object routing) {
                    this.routing = routing;
                }
            }

            public class WithSpaceIndexAnnotation {
                public class BasicIndex {
                    private Object indexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL)
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getIndexedObject() {
                        return indexedObject;
                    }

                    public void setIndexedObject(Object indexedObject) {
                        this.indexedObject = indexedObject;
                    }
                }

                public class ExtendedIndexType {
                    private Object indexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getIndexedObject() {
                        return indexedObject;
                    }

                    public void setIndexedObject(Object indexedObject) {
                        this.indexedObject = indexedObject;
                    }
                }

                public class PathIndex {
                    private Object pathIndexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path")
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getPathIndexedObject() {
                        return pathIndexedObject;
                    }

                    public void setPathIndexedObject(Object pathIndexedObject) {
                        this.pathIndexedObject = pathIndexedObject;
                    }
                }

                public class PathIndexWithCollection {
                    private Object pathIndexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "[*].path")
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getPathIndexedObject() {
                        return pathIndexedObject;
                    }

                    public void setPathIndexedObject(Object pathIndexedObject) {
                        this.pathIndexedObject = pathIndexedObject;
                    }
                }

                public class PathIndexWithInnerCollection {
                    private Object pathIndexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path[*].path")
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getPathIndexedObject() {
                        return pathIndexedObject;
                    }

                    public void setPathIndexedObject(Object pathIndexedObject) {
                        this.pathIndexedObject = pathIndexedObject;
                    }
                }
            }

            public class WithSpaceExcludeAnnotation {
                private Object object;

                @SpaceExclude
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }

            }

            public class WithSpaceDynamicPropertiesAnnotation {
                private Map<String, Object> map;

                @SpaceDynamicProperties
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Map<String, Object> getObject() {
                    return map;
                }

                public void setObject(Map<String, Object> map) {
                    this.map = map;
                }
            }

            public class InheritSpaceIdAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Object getId() {
                    // TODO Auto-generated method stub
                    return super.getId();
                }
            }

            public class InheritSpaceRoutingAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Object getRouting() {
                    // TODO Auto-generated method stub
                    return super.getRouting();
                }
            }

            public class InheritSpaceIndexAnnotation {
                public class BasicIndexType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getBasicIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getBasicIndexObject();
                    }
                }

                public class ExtendedIndexType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getExtendedIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getExtendedIndexObject();
                    }
                }

                public class PathIndex extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object getPathIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getPathIndexObject();
                    }
                }
            }

            public class InheritSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.BINARY)
                public Map<String, Object> getDynamicPropertiesMap() {
                    // TODO Auto-generated method stub
                    return super.getDynamicPropertiesMap();
                }
            }
        }

        public class CompressedType {
            public class OnIntegerTypeProperty {
                Integer intType;

                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Integer getIntType() {
                    return intType;
                }

                public void setIntType(Integer intType) {
                    this.intType = intType;
                }
            }

            public class OnStringTypeProperty {
                String stringType;

                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public String getStringType() {
                    return stringType;
                }

                public void setStringType(String stringType) {
                    this.stringType = stringType;
                }
            }

            public class OnBooleanTypeProperty {
                Boolean booleanType;

                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Boolean getBooleanType() {
                    return booleanType;
                }

                public void setBooleanType(Boolean booleanType) {
                    this.booleanType = booleanType;
                }
            }

            public class WithSpaceIdAnnotation {
                Object id;

                @SpaceId
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Object getId() {
                    return id;
                }

                public void setId(Object id) {
                    this.id = id;
                }
            }

            public class WithSpaceRoutingAnnotation {
                Object routing;

                @SpaceRouting
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Object getRouting() {
                    return routing;
                }

                public void setRouting(Object routing) {
                    this.routing = routing;
                }
            }

            public class WithSpaceIndexAnnotation {
                public class BasicIndex {
                    private Object indexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL)
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getIndexedObject() {
                        return indexedObject;
                    }

                    public void setIndexedObject(Object indexedObject) {
                        this.indexedObject = indexedObject;
                    }
                }

                public class ExtendedIndexType {
                    private Object indexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getIndexedObject() {
                        return indexedObject;
                    }

                    public void setIndexedObject(Object indexedObject) {
                        this.indexedObject = indexedObject;
                    }
                }

                public class PathIndex {
                    private Object pathIndexedObject;

                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path")
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getPathIndexedObject() {
                        return pathIndexedObject;
                    }

                    public void setPathIndexedObject(Object pathIndexedObject) {
                        this.pathIndexedObject = pathIndexedObject;
                    }
                }
            }

            public class WithSpaceExcludeAnnotation {
                private Object object;

                @SpaceExclude
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Object getObject() {
                    return object;
                }

                public void setObject(Object object) {
                    this.object = object;
                }

            }

            public class WithSpaceDynamicPropertiesAnnotation {
                private Map<String, Object> map;

                @SpaceDynamicProperties
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Map<String, Object> getObject() {
                    return map;
                }

                public void setObject(Map<String, Object> map) {
                    this.map = map;
                }
            }

            public class InheritSpaceIdAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Object getId() {
                    // TODO Auto-generated method stub
                    return super.getId();
                }
            }

            public class InheritSpaceRoutingAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Object getRouting() {
                    // TODO Auto-generated method stub
                    return super.getRouting();
                }
            }

            public class InheritSpaceIndexAnnotation {
                public class BasicIndexType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getBasicIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getBasicIndexObject();
                    }
                }

                public class ExtendedIndexType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getExtendedIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getExtendedIndexObject();
                    }
                }

                public class PathIndex extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object getPathIndexObject() {
                        // TODO Auto-generated method stub
                        return super.getPathIndexObject();
                    }
                }
            }

            public class InheritSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceStorageType(storageType = StorageType.COMPRESSED)
                public Map<String, Object> getDynamicPropertiesMap() {
                    // TODO Auto-generated method stub
                    return super.getDynamicPropertiesMap();
                }
            }
        }
    }

    public class InheritStorageType {
        public class ObjectType {
            public class DeclareSpaceExcludeAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceExclude
                public Object get_objectStorageTypeObject() {
                    // TODO Auto-generated method stub
                    return super.get_objectStorageTypeObject();
                }
            }

            public class DeclareSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceDynamicProperties
                public Object get_objectStorageTypeObject() {
                    // TODO Auto-generated method stub
                    return super.get_objectStorageTypeObject();
                }
            }

            public class DeclareSpacePersistAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpacePersist
                public Object get_objectStorageTypeObject() {
                    // TODO Auto-generated method stub
                    return super.get_objectStorageTypeObject();
                }
            }

            public class DeclareSpaceVesionAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceVersion
                public Object get_objectStorageTypeObject() {
                    // TODO Auto-generated method stub
                    return super.get_objectStorageTypeObject();
                }
            }

            public class DeclareSpaceLeaseExpirationAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceLeaseExpiration
                public Object get_objectStorageTypeObject() {
                    // TODO Auto-generated method stub
                    return super.get_objectStorageTypeObject();
                }
            }

            public class DeclareStorageType {
                public class BinaryType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Object get_objectStorageTypeObject() {
                        // TODO Auto-generated method stub
                        return super.get_objectStorageTypeObject();
                    }
                }

                public class CompressedType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public Object get_objectStorageTypeObject() {
                        // TODO Auto-generated method stub
                        return super.get_objectStorageTypeObject();
                    }
                }
            }
        }

        public class BinaryType {
            public class DeclareSpaceIdAnnotation extends PojoOnlyStorageTypeAnnotations {
                @SpaceId
                @Override
                public List<String> get_binaryStorageTypeList() {
                    // TODO Auto-generated method stub
                    return super.get_binaryStorageTypeList();
                }
            }

            public class DeclareSpaceRoutingAnnotation extends PojoOnlyStorageTypeAnnotations {
                @SpaceRouting
                @Override
                public List<String> get_binaryStorageTypeList() {
                    // TODO Auto-generated method stub
                    return super.get_binaryStorageTypeList();
                }
            }

            public class DeclareSpaceIndexAnnotation {
                public class BasicIndexType extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL)
                    @Override
                    public List<String> get_binaryStorageTypeList() {
                        // TODO Auto-generated method stub
                        return super.get_binaryStorageTypeList();
                    }
                }

                public class ExtendedIndexType extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
                    @Override
                    public List<String> get_binaryStorageTypeList() {
                        // TODO Auto-generated method stub
                        return super.get_binaryStorageTypeList();
                    }
                }

                public class PathIndex extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path")
                    @Override
                    public List<String> get_binaryStorageTypeList() {
                        // TODO Auto-generated method stub
                        return super.get_binaryStorageTypeList();
                    }
                }
            }

            public class DeclareSpaceExcludeAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceExclude
                public List<String> get_binaryStorageTypeList() {
                    // TODO Auto-generated method stub
                    return super.get_binaryStorageTypeList();
                }
            }

            public class DeclareSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceDynamicProperties
                public List<String> get_binaryStorageTypeList() {
                    // TODO Auto-generated method stub
                    return super.get_binaryStorageTypeList();
                }
            }

            public class DeclareStorageType {
                public class ObjectType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.OBJECT)
                    public List<String> get_binaryStorageTypeList() {
                        // TODO Auto-generated method stub
                        return super.get_binaryStorageTypeList();
                    }
                }

                public class CompressedType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.COMPRESSED)
                    public List<String> get_binaryStorageTypeList() {
                        // TODO Auto-generated method stub
                        return super.get_binaryStorageTypeList();
                    }
                }
            }
        }

        public class CompressedType {
            public class DeclareSpaceIdAnnotation extends PojoOnlyStorageTypeAnnotations {
                @SpaceId
                @Override
                public Map<String, Object> get_compressedStorageTypeMap() {
                    // TODO Auto-generated method stub
                    return super.get_compressedStorageTypeMap();
                }

            }

            public class DeclareSpaceRoutingAnnotation extends PojoOnlyStorageTypeAnnotations {
                @SpaceRouting
                @Override
                public Map<String, Object> get_compressedStorageTypeMap() {
                    // TODO Auto-generated method stub
                    return super.get_compressedStorageTypeMap();
                }
            }

            public class DeclareSpaceIndexAnnotation {
                public class BasicIndexType extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL)
                    @Override
                    public Map<String, Object> get_compressedStorageTypeMap() {
                        // TODO Auto-generated method stub
                        return super.get_compressedStorageTypeMap();
                    }
                }

                public class ExtendedIndexType extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
                    @Override
                    public Map<String, Object> get_compressedStorageTypeMap() {
                        // TODO Auto-generated method stub
                        return super.get_compressedStorageTypeMap();
                    }
                }

                public class PathIndex extends PojoOnlyStorageTypeAnnotations {
                    @SpaceIndex(type = SpaceIndexType.EQUAL, path = "path")
                    @Override
                    public Map<String, Object> get_compressedStorageTypeMap() {
                        // TODO Auto-generated method stub
                        return super.get_compressedStorageTypeMap();
                    }
                }
            }

            public class DeclareSpaceExcludeAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceExclude
                public Map<String, Object> get_compressedStorageTypeMap() {
                    // TODO Auto-generated method stub
                    return super.get_compressedStorageTypeMap();
                }
            }

            public class DeclareSpaceDynamicPropertiesAnnotation extends PojoPropertiesStorageType {
                @Override
                @SpaceDynamicProperties
                public Map<String, Object> get_compressedStorageTypeMap() {
                    // TODO Auto-generated method stub
                    return super.get_compressedStorageTypeMap();
                }
            }

            public class DeclareStorageType {
                public class ObjectType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.OBJECT)
                    public Map<String, Object> get_compressedStorageTypeMap() {
                        // TODO Auto-generated method stub
                        return super.get_compressedStorageTypeMap();
                    }
                }

                public class BinaryType extends PojoPropertiesStorageType {
                    @Override
                    @SpaceStorageType(storageType = StorageType.BINARY)
                    public Map<String, Object> get_compressedStorageTypeMap() {
                        // TODO Auto-generated method stub
                        return super.get_compressedStorageTypeMap();
                    }
                }
            }
        }
    }

}

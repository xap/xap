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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceClass.IncludeProperties;
import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.metadata.index.SpaceIndexType;

/**
 * Type to test affect of IncludeProperties.EXPLICIT on property inclusion.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
@SpaceClass(includeProperties = IncludeProperties.EXPLICIT)
@com.gigaspaces.api.InternalApi
public class PojoExplicit {
    private Integer _pojoId;
    private Integer _pojoIndexedField;
    private String _implicitProperty;
    private String _explicitProperty;
    private String _excludedProperty;

    @SpaceId
    public Integer getPojoId() {
        return _pojoId;
    }

    public void setPojoId(Integer pojoId) {
        _pojoId = pojoId;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public Integer getPojoIndexedField() {
        return _pojoIndexedField;
    }

    public void setPojoIndexedField(Integer pojoIndexedField) {
        _pojoIndexedField = pojoIndexedField;
    }

    public String getImplicitProperty() {
        return _implicitProperty;
    }

    public void setImplicitProperty(String value) {
        _implicitProperty = value;
    }

    @SpaceProperty
    public String getExplicitProperty() {
        return _explicitProperty;
    }

    public void setExplicitProperty(String value) {
        _explicitProperty = value;
    }

    @SpaceExclude
    public String getExcludedProperty() {
        return _excludedProperty;
    }

    public void setExcludedProperty(String value) {
        _excludedProperty = value;
    }
}

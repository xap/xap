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

import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;
import com.gigaspaces.metadata.index.SpaceIndexType;

@com.gigaspaces.api.InternalApi
public class PojoIndexes {
    private String _valueDefault;
    private String _valueNone;
    private String _valueBasic;
    private String _valueExtended;

    public String getValueDefault() {
        return _valueDefault;
    }

    public void setValueDefault(String value) {
        _valueDefault = value;
    }

    @SpaceProperty(index = IndexType.NONE)
    public String getValueNone() {
        return _valueNone;
    }

    public void setValueNone(String value) {
        _valueNone = value;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getValueBasic() {
        return _valueBasic;
    }

    public void setValueBasic(String value) {
        _valueBasic = value;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getValueExtended() {
        return _valueExtended;
    }

    public void setValueExtended(String value) {
        _valueExtended = value;
    }
}

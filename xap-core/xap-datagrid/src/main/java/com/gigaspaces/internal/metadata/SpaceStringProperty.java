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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class SpaceStringProperty implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private String propertyValue;
    private String propertyClassName;


    public SpaceStringProperty() {
    }

    public SpaceStringProperty(String propertyClassName, String propertyValue) {
        super();
        this.propertyClassName = propertyClassName;
        this.propertyValue = propertyValue;
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    public String getPropertyClassName() {
        return propertyClassName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, propertyValue);
        //TODO consider to use repetitive write if becomes performance issue
        IOUtils.writeString(out, propertyClassName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        propertyValue = IOUtils.readString(in);
        propertyClassName = IOUtils.readString(in);
    }

    @Override
    public String toString() {
        return "SpaceStringTuple [propertyValue=" + propertyValue
                + ", propertyClassName=" + propertyClassName + "]";
    }
}

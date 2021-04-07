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
package org.jini.rio.boot;

import com.gigaspaces.internal.io.BootIOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by tamirs
 * on 11/8/16.
 */
public class SupportCodeChangeAnnotationContainer implements Externalizable{
    private static final long serialVersionUID = 1L;

    private String version = "";

    public static final SupportCodeChangeAnnotationContainer ONE_TIME = new SupportCodeChangeAnnotationContainer();

    // Externalizable
    public SupportCodeChangeAnnotationContainer() {
    }

    public SupportCodeChangeAnnotationContainer(String version) {
        this.version = version;
    }


    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        BootIOUtils.writeString(out, version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        version = BootIOUtils.readString(in);
    }

    @Override
    public String toString() {
        return "SupportCodeChangeContainer{" +
                "version='" + version + '\'' +
                '}';
    }

}

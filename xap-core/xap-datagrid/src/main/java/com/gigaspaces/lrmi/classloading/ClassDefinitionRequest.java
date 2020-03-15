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
package com.gigaspaces.lrmi.classloading;

import java.io.Serializable;
/**
 * Request for a class definition, includes class name, classloader id and type of resource to retrieve (CLASS or RESOURCE file)
 *
 * @author alon shoham
 * @since 15.0
 */
public class ClassDefinitionRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private long classLoaderId;
    private String className;
    private FileType fileType;

    public ClassDefinitionRequest() {
    }

    public ClassDefinitionRequest(long classLoaderId, String className, FileType fileType) {
        this.classLoaderId = classLoaderId;
        this.className = className;
        this.fileType = fileType;
    }

    public String getClassName() {
        return className;
    }

    public long getClassLoaderId() {
        return classLoaderId;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getResourceName(){
        return className;
    }
}

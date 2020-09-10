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
 * Response to a class definition request, holds the class bytes
 *
 * @author alon shoham
 * @since 15.0
 */

public class ClassDefinitionResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] classBytes;
    private final Exception exception;

    public ClassDefinitionResponse() {
        classBytes = new byte[0];
        exception = null;
    }

    public ClassDefinitionResponse(byte[] classBytes, Exception e) {
        this.classBytes = classBytes;
        this.exception = e;
    }

    public byte[] getClassBytes() {
        return classBytes;
    }

    public Exception getException() {
        return exception;
    }
}

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
package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import java.io.*;

public class GSObjectInputStream extends DataInputStream implements ObjectInput{
    private ObjectInputStream ois;

    public GSObjectInputStream(java.io.InputStream in) {
        super(in);
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        if (ois == null) {
            ois = new GSObjectInputStream.HeaderlessObjectInputStream(super.in);
        }
        return ois.readObject();
    }

    @Override
    public void close() throws IOException {
        if (ois != null)
            ois.close();
        super.close();
    }

    private static class HeaderlessObjectInputStream extends ObjectInputStream {

        public HeaderlessObjectInputStream(java.io.InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected void readStreamHeader() throws IOException {
        }
    }
}

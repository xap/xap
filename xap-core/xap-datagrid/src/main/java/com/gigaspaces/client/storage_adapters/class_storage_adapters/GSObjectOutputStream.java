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


public class GSObjectOutputStream extends DataOutputStream implements ObjectOutput {
    private ObjectOutputStream oos;

    public GSObjectOutputStream(java.io.OutputStream out) {
        super(out);
    }

    public void writeObject(Object obj) throws IOException {
        if (oos == null) {
            oos = new HeaderlessObjectOutputStream(super.out);
        }
        oos.writeObject(obj);
    }

    @Override
    public void close() throws IOException {
        if (oos != null)
            oos.close();
        super.close();
    }

    private static class HeaderlessObjectOutputStream extends ObjectOutputStream {

        public HeaderlessObjectOutputStream(java.io.OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // skip header to reduce footprint super.writeStreamHeader();
        }
    }
}





















































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
package com.gigaspaces.internal.jvm;



public class J9DiagnosticWrapper implements JVMDiagnosticWrapper {
    Class<?> dumpClass;

    public J9DiagnosticWrapper() throws ClassNotFoundException {
        this.dumpClass = Class.forName("com.ibm.jvm.Dump",
                true, J9DiagnosticWrapper.class.getClassLoader());
    }

    @Override
    public void dumpHeap(String outputFile, boolean live) {
        throw new UnsupportedOperationException("DumpHeap operation is unsupported with Java vendor: " + JavaUtils.getVendor());
    }

    @Override
    public boolean useCompressedOopsAsBoolean() {
        String vmInfo = System.getProperty("java.vm.info");
        return vmInfo != null && vmInfo.contains("Compressed References");
    }
}

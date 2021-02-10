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

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;


public class HotSpotDiagnosticWrapper implements JVMDiagnosticWrapper {
    HotSpotDiagnosticMXBean bean;

    public HotSpotDiagnosticWrapper() {
        try {
        bean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                    "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void dumpHeap(String outputFile, boolean live) throws IOException {
        bean.dumpHeap(outputFile, live);
    }

    @Override
    public boolean useCompressedOopsAsBoolean() {
       VMOption vmOption = getVMOption("UseCompressedOops");
       String val = vmOption != null ? vmOption.getValue(): null;
       return Boolean.parseBoolean(val);
    }

    public VMOption getVMOption(String name) {
       return bean.getVMOption(name);
    }
}

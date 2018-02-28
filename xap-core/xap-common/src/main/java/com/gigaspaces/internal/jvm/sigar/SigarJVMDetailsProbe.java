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

package com.gigaspaces.internal.jvm.sigar;

import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMDetailsProbe;
import com.gigaspaces.internal.sigar.SigarHolder;

import org.hyperic.sigar.Sigar;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.UUID;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SigarJVMDetailsProbe implements JVMDetailsProbe {

    private static final String uid;

    private static RuntimeMXBean runtimeMXBean;

    private static MemoryMXBean memoryMXBean;

    static {
        uid = UUID.randomUUID().toString(); // TODO I think we can get the actual vmid
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    private Sigar sigar;

    public SigarJVMDetailsProbe() {
        sigar = SigarHolder.getSigar();
    }

    public JVMDetails probeDetails() {
        return new JVMDetails(uid, runtimeMXBean.getVmName(), System.getProperty("java.version"), System.getProperty("java.vendor"),
                runtimeMXBean.getStartTime(),
                memoryMXBean.getHeapMemoryUsage().getInit(), memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getInit(), memoryMXBean.getNonHeapMemoryUsage().getMax(),
                runtimeMXBean.getInputArguments().toArray(new String[0]),runtimeMXBean.isBootClassPathSupported() ? runtimeMXBean.getBootClassPath() : "", runtimeMXBean.getClassPath(), runtimeMXBean.getSystemProperties(),
                System.getenv(),
                sigar.getPid());
    }
}

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

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.RollingFileHandler;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * @author kimchy
 *
 *         GS-10774 Note to developers: This class is used by {@link RollingFileHandler} to extract
 *         the process ID, therefore this class must not use Logger in order to avoid deadlock.
 */
@com.gigaspaces.api.InternalApi
public class JVMDetails implements SmartExternalizable {

    private static final long serialVersionUID = -4083973634154614496L;

    private String uid = "";

    private String vmName = "";

    private String vmVersion = "";

    private String vmVendor = "";

    private long startTime = -1;

    private long memoryHeapInit = -1;

    private long memoryHeapMax = -1;

    private long memoryNonHeapInit = -1;

    private long memoryNonHeapMax = -1;

    private String[] inputArguments;

    private String bootClassPath;

    private String classPath;

    private Map<String, String> systemProperties;

    private Map<String, String> environmentVariables;

    private long pid = -1;

    public JVMDetails() {
    }

    public JVMDetails(String uid, String vmName, String vmVersion, String vmVendor, long startTime,
                      long memoryHeapInit, long memoryHeapMax, long memoryNonHeapInit, long memoryNonHeapMax,
                      String[] inputArguments, String bootClassPath, String classPath, Map<String, String> systemProperties, Map<String, String> environmentVariables) {
        this(uid, vmName, vmVersion, vmVendor, startTime, memoryHeapInit, memoryHeapMax, memoryNonHeapInit, memoryNonHeapMax, inputArguments, bootClassPath, classPath, systemProperties, environmentVariables, -1);
    }

    public JVMDetails(String uid, String vmName, String vmVersion, String vmVendor, long startTime,
                      long memoryHeapInit, long memoryHeapMax, long memoryNonHeapInit, long memoryNonHeapMax,
                      String[] inputArguments, String bootClassPath, String classPath, Map<String, String> systemProperties,
                      Map<String, String> environmentVariables, long pid) {
        this.uid = uid;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.startTime = startTime;
        this.memoryHeapInit = memoryHeapInit;
        this.memoryHeapMax = memoryHeapMax;
        this.memoryNonHeapInit = memoryNonHeapInit;
        this.memoryNonHeapMax = memoryNonHeapMax;
        this.inputArguments = inputArguments;
        this.bootClassPath = bootClassPath;
        this.classPath = classPath;
        this.systemProperties = systemProperties;
        this.environmentVariables = GsEnv.filterByPrefix(environmentVariables);
        this.pid = pid;
    }

    public boolean isNA() {
        return uid.length() == 0;
    }

    public String getUid() {
        return uid;
    }

    public String getVmName() {
        return vmName;
    }

    public String getVmVersion() {
        return vmVersion;
    }

    public String getVmVendor() {
        return vmVendor;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getMemoryHeapInit() {
        return memoryHeapInit;
    }

    public long getMemoryHeapMax() {
        return memoryHeapMax;
    }

    public long getMemoryNonHeapInit() {
        return memoryNonHeapInit;
    }

    public long getMemoryNonHeapMax() {
        return memoryNonHeapMax;
    }

    public String[] getInputArguments() {
        return inputArguments;
    }

    public String getBootClassPath() {
        return bootClassPath;
    }

    public String getClassPath() {
        return classPath;
    }

    public Map<String, String> getSystemProperties() {
        return systemProperties;
    }

    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    public long getPid() {
        return pid;
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_1_1)) {
            writeExternal9_1_1(out);
        } else {
            writeExternal9_1_0(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();

        if (version.greaterOrEquals(PlatformLogicalVersion.v9_1_1)) {
            readExternal9_1_1(in);
        } else {
            readExternal9_1_0(in);
        }

    }

    private void writeExternal9_1_0(ObjectOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(vmName);
        out.writeUTF(vmVersion);
        out.writeUTF(vmVendor);
        out.writeLong(startTime);
        out.writeLong(memoryHeapInit);
        out.writeLong(memoryHeapMax);
        out.writeLong(memoryNonHeapInit);
        out.writeLong(memoryNonHeapMax);
        out.writeObject(inputArguments);
        out.writeUTF(bootClassPath);
        out.writeUTF(classPath);
        out.writeObject(systemProperties);
        out.writeLong(pid);
    }

    private void writeExternal9_1_1(ObjectOutput out) throws IOException {
        BootIOUtils.writeString(out, uid);
        BootIOUtils.writeString(out, vmName);
        BootIOUtils.writeString(out, vmVersion);
        BootIOUtils.writeString(out, vmVendor);
        out.writeLong(startTime);
        out.writeLong(memoryHeapInit);
        out.writeLong(memoryHeapMax);
        out.writeLong(memoryNonHeapInit);
        out.writeLong(memoryNonHeapMax);
        BootIOUtils.writeStringArray(out, inputArguments);
        BootIOUtils.writeString(out, bootClassPath);
        BootIOUtils.writeString(out, classPath);
        BootIOUtils.writeMapStringString(out, systemProperties);
        out.writeLong(pid);
        BootIOUtils.writeMapStringString(out, environmentVariables);
    }

    private void readExternal9_1_0(ObjectInput in) throws IOException,
            ClassNotFoundException {
        uid = in.readUTF();
        vmName = in.readUTF();
        vmVersion = in.readUTF();
        vmVendor = in.readUTF();
        startTime = in.readLong();
        memoryHeapInit = in.readLong();
        memoryHeapMax = in.readLong();
        memoryNonHeapInit = in.readLong();
        memoryNonHeapMax = in.readLong();
        inputArguments = (String[]) in.readObject();
        bootClassPath = in.readUTF();
        classPath = in.readUTF();
        systemProperties = (Map<String, String>) in.readObject();
        pid = in.readLong();
    }

    private void readExternal9_1_1(ObjectInput in) throws IOException,
            ClassNotFoundException {
        uid = BootIOUtils.readString(in);
        vmName = BootIOUtils.readString(in);
        vmVersion = BootIOUtils.readString(in);
        vmVendor = BootIOUtils.readString(in);
        startTime = in.readLong();
        memoryHeapInit = in.readLong();
        memoryHeapMax = in.readLong();
        memoryNonHeapInit = in.readLong();
        memoryNonHeapMax = in.readLong();
        inputArguments = BootIOUtils.readStringArray(in);
        bootClassPath = BootIOUtils.readString(in);
        classPath = BootIOUtils.readString(in);
        systemProperties = BootIOUtils.readMapStringString(in);
        pid = in.readLong();
        environmentVariables = BootIOUtils.readMapStringString(in);
    }
}

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
package com.gigaspaces.internal.os.oshi;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.software.os.OSProcess;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class OshiProcessCpuSampler implements ProcessCpuSampler {
    private final int pid = (int) JavaUtils.getPid();

    @Override
    public long sampleTotalCpuTime() {
        OSProcess process = OshiUtils.getOperatingSystem().getProcess(pid);
        return process.getKernelTime() + process.getUserTime();
    }
}

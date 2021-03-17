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
package com.gigaspaces.internal.os;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class JavaProcessCpuSampler implements ProcessCpuSampler {

    private final Method infoMethod;
    private final Method totalCpuDurationMethod;
    private final Object processHandle;

    public JavaProcessCpuSampler() {
        try {
            Class<?> processHandleClass = Class.forName("java.lang.ProcessHandle");
            Class<?> processHandleInfoClass = Class.forName("java.lang.ProcessHandle$Info");
            Method currentMethod = processHandleClass.getMethod("current");
            infoMethod = processHandleClass.getMethod("info");
            totalCpuDurationMethod = processHandleInfoClass.getMethod("totalCpuDuration");
            processHandle = currentMethod.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create JavaProcessCpuSampler", e);
        }
    }

    @Override
    public long sampleTotalCpuTime() {
        try {
            Object info = infoMethod.invoke(processHandle);
            Optional<Duration> totalCpuDuration = (Optional<Duration>) totalCpuDurationMethod.invoke(info);
            return totalCpuDuration.map(Duration::toMillis).orElse(NA);
        } catch (ReflectiveOperationException e) {
            return NA;
        }
    }
}

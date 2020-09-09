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

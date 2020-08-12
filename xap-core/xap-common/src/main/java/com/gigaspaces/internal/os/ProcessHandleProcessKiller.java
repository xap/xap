package com.gigaspaces.internal.os;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.BootIOUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class ProcessHandleProcessKiller implements ProcessKiller {
    private final Class processHandleClass;
    private final Method ofMethod;
    private final Method descendantsMethod;
    private final Method destroyMethod;
    private final Method destroyForciblyMethod;
    private final Method isAliveMethod;

    public ProcessHandleProcessKiller() throws ReflectiveOperationException {
        processHandleClass = Class.forName("java.lang.ProcessHandle");
        ofMethod = processHandleClass.getMethod("of", long.class);
        descendantsMethod = processHandleClass.getMethod("descendants");
        destroyMethod = processHandleClass.getMethod("destroy");
        destroyForciblyMethod = processHandleClass.getMethod("destroyForcibly");
        isAliveMethod = processHandleClass.getMethod("isAlive");
    }

    @Override
    public String getName() {
        return "ProcessHandle";
    }

    @Override
    public boolean kill(long pid, long timeout, boolean recursive) {
        try {
            Optional<?> processHandle = (Optional<?>) ofMethod.invoke(null, pid);
            if (!processHandle.isPresent())
                return true;
            List<Object> processes = new ArrayList<>();
            processes.add(processHandle.get());
            if (recursive) {
                Stream<Object> descendants = (Stream<Object>) descendantsMethod.invoke(processHandle.get());
                processes.addAll(descendants.collect(Collectors.toList()));
            }
            // Destroy all processes:
            for (Object process : processes) {
                destroyMethod.invoke(process);
            }

            // Wait for all processes to terminate (pending timeout):
            try {
                BootIOUtils.waitFor(() -> pruneTerminated(processes), timeout, 100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            // If there are still processes, destroy them forcibly
            for (Object process : processes) {
                destroyForciblyMethod.invoke(process);
            }
            return pruneTerminated(processes);

        } catch (ReflectiveOperationException e) {
            ProcessUtils.logger.warn("Failed to kill process " + pid + ": " + e.getMessage());
            return false;
        }
    }

    private boolean pruneTerminated(Collection<Object> processes) {
        processes.removeIf(proc -> !isAlive(proc));
        return processes.isEmpty();
    }

    private boolean isAlive(Object proc) {
        try {
            return (boolean)isAliveMethod.invoke(proc);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to check if process is alive", e);
        }
    }
}

package com.gigaspaces.internal.os;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.sigar.SigarChecker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public class ProcessUtils {

    private static final Logger logger = Logger.getLogger(ProcessUtils.class.getName());
    private static final Optional<ProcessKiller> processKiller = initProcessKiller();

    private static Optional<ProcessKiller> initProcessKiller() {
        if (JavaUtils.greaterOrEquals(9)) {
            try {
                return Optional.of(new ProcessHandleProcessKiller());
            } catch (ReflectiveOperationException e) {
                logger.warning("Failed to create ProcessHandleProcessKiller: " + e.getMessage());
            }
        }
        if (SigarChecker.isAvailable()) {
            return Optional.of(SigarChecker.createProcessKiller());
        }
        return Optional.empty();
    }

    public static Optional<String> getProcessKillerName() {
        return processKiller.map(ProcessKiller::getName);
    }

    public static boolean kill(long pid, long timeout) {
        return processKiller.map(k -> k.kill(pid, timeout)).orElse(false);
    }

    private static class ProcessHandleProcessKiller implements ProcessKiller {
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
        public boolean kill(long pid, long timeoutMillis) {
            try {
                Optional<?> processHandle = (Optional<?>) ofMethod.invoke(null, pid);
                if (!processHandle.isPresent())
                    return true;
                Object mainProcess = processHandle.get();
                List<Object> processes = new ArrayList<>();
                processes.add(mainProcess);
                /*
                if (recursive) {
                    Stream<Object> descendants = (Stream<Object>) descendantsMethod.invoke(mainProcess);
                    processes.addAll(descendants.collect(Collectors.toList()));
                }
                */
                // Destroy all processes:
                for (Object process : processes) {
                    destroyMethod.invoke(process);
                }
                // Wait for all processes to terminate (pending timeout):
                long deadline = System.currentTimeMillis() + timeoutMillis;
                while (System.currentTimeMillis() < deadline) {
                    removeTerminatedProcesses(processes);
                    if (processes.isEmpty())
                        return true;
                    if (!trySleep(100))
                        return false;
                }
                // If there are still processes, destroy them forcibly
                for (Object process : processes) {
                    destroyForciblyMethod.invoke(process);
                }
                removeTerminatedProcesses(processes);
                return processes.isEmpty();

            } catch (ReflectiveOperationException e) {
                logger.warning("Failed to kill process " + pid + ": " + e.getMessage());
                return false;
            }
        }

        private void removeTerminatedProcesses(Collection<Object> processes) throws InvocationTargetException, IllegalAccessException {
            Iterator<Object> iterator = processes.iterator();
            while (iterator.hasNext()) {
                Object proc = iterator.next();
                if (!(boolean)isAliveMethod.invoke(proc))
                    iterator.remove();
            }
        }

        private static boolean trySleep(long millis) {
            try {
                Thread.sleep(millis);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /*
        public static boolean killWithProcessHandle(long pid, long timeout) {
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
            if (!processHandle.isPresent())
                return true;
            ProcessHandle mainProcess = processHandle.get();
            List<ProcessHandle> processes = new ArrayList<>();
            processes.add(mainProcess);
            if (JavaUtils.isWindows()) {
                processes.addAll(mainProcess.descendants().collect(Collectors.toList()));
            }
            // Destroy all processes:
            processes.forEach(ProcessHandle::destroy);
            // Wait for all processes to terminate (pending timeout):
            long deadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < deadline) {
                removeTerminatedProcesses(processes);
                if (processes.isEmpty())
                    return true;
                if (!trySleep(100))
                    return false;
            }
            // If there are still processes, destroy them forcibly
            processes.forEach(ProcessHandle::destroyForcibly);
            removeTerminatedProcesses(processes);
            return processes.isEmpty();
        }

        private static void removeTerminatedProcesses(Collection<ProcessHandle> processes) {
            Iterator<ProcessHandle> iterator = processes.iterator();
            while (iterator.hasNext()) {
                ProcessHandle proc = iterator.next();
                if (!proc.isAlive())
                    iterator.remove();
            }
        }
    */
    }
}

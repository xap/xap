package com.gigaspaces.internal.os;

import com.gigaspaces.internal.sigar.SigarChecker;

import java.util.*;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public class ProcessUtils {

    private static final Optional<ProcessKiller> processKiller = initProcessKiller();

    private static Optional<ProcessKiller> initProcessKiller() {
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
}

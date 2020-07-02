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

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.sigar.SigarChecker;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public class ProcessUtils {

    public static final Logger logger = LoggerFactory.getLogger(ProcessUtils.class.getName());
    private static final Optional<ProcessKiller> processKiller = initProcessKiller();
    private static final boolean RECURSIVE_DEFAULT = initDefaultRecursive();

    private static boolean initDefaultRecursive() {
        String recursive = System.getProperty(CommonSystemProperties.KILL_RECURSIVE, "");
        if (recursive.isEmpty())
            return JavaUtils.isWindows();
        else
            return Boolean.parseBoolean(recursive);
    }

    private static Optional<ProcessKiller> initProcessKiller() {
        if (JavaUtils.greaterOrEquals(9)) {
            try {
                return Optional.of(new ProcessHandleProcessKiller());
            } catch (ReflectiveOperationException e) {
                logger.warn("Failed to create ProcessHandleProcessKiller: " + e.getMessage());
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
        return kill(pid, timeout, RECURSIVE_DEFAULT);
    }

    public static boolean kill(long pid, long timeout, boolean recursive) {
        return processKiller.map(k -> k.kill(pid, timeout, recursive)).orElse(false);
    }
}

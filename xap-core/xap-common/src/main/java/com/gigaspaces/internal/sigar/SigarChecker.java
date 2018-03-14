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

package com.gigaspaces.internal.sigar;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SigarChecker {

    public static boolean isAvailable() {
        try {
            return SigarHolder.getSigar() != null;
        } catch (Throwable t) {
            return false;
        }
    }

    public static boolean killProcess(long pid, long timeout) {
        if (!isAvailable())
            return false;

        try {
            return SigarHolder.kill(pid, timeout);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to kill process " + pid + ": " + e.getMessage(), e);
        }
    }
}

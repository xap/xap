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

import com.gigaspaces.start.SystemBoot;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SigarHolder {

    private static Sigar sigar;
    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

    public static synchronized Sigar getSigar() {
        if (sigar == null) {
            Sigar newSigar = new Sigar();
            sigar = newSigar.getPid() != -1 ? newSigar : null;
        }
        return sigar;
    }

    public static boolean kill(long pid, long timeout) throws SigarException {
        Sigar sigar = getSigar();

        // Ask nicely, let process a chance to shutdown gracefully:
        kill(sigar, pid, "SIGTERM");
        // Wait for process to terminate:
        boolean isTerminated;
        try {
            isTerminated = waitForExit(sigar, pid, timeout, 100 /*pollInterval*/);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            isTerminated = !isAlive(sigar, pid);
        }
        // If process is still alive, kill it:
        if (!isTerminated && !isWindows) {
            kill(sigar, pid, "SIGKILL");
            isTerminated = !isAlive(sigar, pid);
        }
        return isTerminated;
    }

    private static boolean waitForExit(Sigar sigar, long pid, long timeout, long pollInterval) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeout;
        while (isAlive(sigar, pid)) {
            long currTime = System.currentTimeMillis();
            if (currTime >= deadline)
                return false;
            Thread.sleep(Math.min(pollInterval, deadline - currTime));
        }
        return true;
    }

    private static void kill(Sigar sigar, long pid, String signal) throws SigarException {
        try {
            sigar.kill(pid, signal);
        } catch (SigarException e) {
            // If the signal could not be sent because the process has already terminated, that's ok for us.
            if (isAlive(sigar, pid))
                throw e;
        }
    }

    private static boolean isAlive(Sigar sigar, long pid) {
        try {
            //sigar.getProcState(pid) is not used because its unstable.
            sigar.getProcTime(pid);
            return true;
        } catch (SigarException e) {
            return false;
        }
    }

    public static synchronized void release() {
        if (!SystemBoot.isRunningWithinGSC()) {
            if (sigar != null) {
                sigar.close();
                sigar = null;
            }
        }
    }
}

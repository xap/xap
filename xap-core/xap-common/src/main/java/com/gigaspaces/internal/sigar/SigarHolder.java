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

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.start.SystemBoot;

import org.hyperic.sigar.ProcState;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.*;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SigarHolder {

    private static SigarHolder singleton;
    private final Sigar sigar;

    public static synchronized SigarHolder singleton() {
        if (singleton == null) {
            Sigar sigar = new Sigar();
            singleton = sigar.getPid() != -1 ? new SigarHolder(sigar) : null;
        }
        return singleton;
    }

    public static Sigar getSigar() {
        return singleton().sigar;
    }

    public static synchronized void release() {
        if (!SystemBoot.isRunningWithinGSC()) {
            if (singleton != null) {
                singleton.sigar.close();
                singleton = null;
            }
        }
    }

    private SigarHolder(Sigar sigar) {
        this.sigar = sigar;
    }

    public boolean kill(long pid, long timeout, boolean recursive) throws SigarException {
        Set<Long> pids = new LinkedHashSet<>();
        pids.add(pid);
        if (recursive) {
            pids.addAll(getDescendants(pid));
        }

        // Ask nicely, let process(s) a chance to shutdown gracefully:
        if (killAll(pids, "SIGTERM"))
            return true;

        // Wait for process(s) to terminate:
        try {
            if (BootIOUtils.waitFor(() -> pruneTerminated(pids), timeout, 100))
                return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        // force kill remaining process(s) (except on windows - not supported)
        if (JavaUtils.isWindows())
            return false;
        return killAll(pids, "SIGKILL");
    }

    private boolean killAll(Set<Long> pids, String signal) throws SigarException {
        for (Long currPid : pids) {
            kill(currPid, signal);
        }
        return pruneTerminated(pids);
    }

    private Map<Long, ProcState> getAllProcesses() throws SigarException {
        final Map<Long, ProcState> result = new HashMap<>();
        for (final long pid : sigar.getProcList()) {
            try {
                result.put(pid, sigar.getProcState(pid));
            } catch (SigarException e) {
                //logger.warn("While scanning for child processes of process " + ppid + ", could not read process state of Process: " + pid + ". Ignoring.", e);
            }
        }
        return result;
    }

    private Set<Long> getDescendants(long ppid) throws SigarException {
        Set<Long> result = new LinkedHashSet<>();
        Map<Long, ProcState> processes = getAllProcesses();
        processes.remove(ppid);
        while (true) {
            int sizeBefore = result.size();
            Iterator<Map.Entry<Long, ProcState>> iterator = processes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, ProcState> entry = iterator.next();
                if (result.contains(entry.getValue().getPpid())) {
                    result.add(entry.getKey());
                    iterator.remove();
                }
            }
            int sizeAfter = result.size();
            if (sizeAfter == sizeBefore)
                return result;
        }
    }

    private boolean pruneTerminated(Set<Long> pids) {
        pids.removeIf(pid -> !isAlive(pid));
        return pids.isEmpty();
    }

    private void kill(long pid, String signal) throws SigarException {
        try {
            sigar.kill(pid, signal);
        } catch (SigarException e) {
            // If the signal could not be sent because the process has already terminated, that's ok for us.
            if (isAlive(pid))
                throw e;
        }
    }

    private boolean isAlive(long pid) {
        try {
            //sigar.getProcState(pid) is not used because its unstable.
            sigar.getProcTime(pid);
            return true;
        } catch (SigarException e) {
            return false;
        }
    }
}

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
}

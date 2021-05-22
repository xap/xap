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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Embedded Sync- info regarding generation ids from which not all embedded info used surly
 * transferred to the sync list
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedRelevantGenerationIdsInfo implements SmartExternalizable {
    private static final long serialVersionUID = 1L;


    private volatile Set<Long> _relevantGens;

    public EmbeddedRelevantGenerationIdsInfo(long current) {
        _relevantGens = new HashSet<Long>();
        _relevantGens.add(current);
    }

    public static String getStorageKey() {
        return "SYNC_ADMIN_EBEDDED_RELEVANT_GENS";
    }


    public EmbeddedRelevantGenerationIdsInfo() {
    }

    public boolean isRelevant(long gen) {
        return _relevantGens.contains(gen);
    }

    //note- called only on adding curre
    public void addCurrentGeneration(long currentGen) {
        _relevantGens.add(currentGen);
    }

    public boolean removeOldGens(long current) {
        if (_relevantGens.size() == 1)
            return false; //no old gens
        Set<Long> relevantGens = new HashSet<Long>();
        relevantGens.add(current);
        _relevantGens = relevantGens;
        return true;
    }

    public Collection<Long> getRelevantGenerations() {
        return _relevantGens;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        PlatformLogicalVersion myversion = (PlatformLogicalVersion) in.readObject();
        _relevantGens = new HashSet<Long>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long l = in.readLong();
            _relevantGens.add(l);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (_relevantGens == null || _relevantGens.size() == 0)
            throw new RuntimeException("internal error: no embedded gens info!");
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        out.writeObject(curversion);
        out.writeInt(_relevantGens.size());
        Iterator<Long> iter = _relevantGens.iterator();
        while (iter.hasNext()) {
            out.writeLong(iter.next());
        }
    }


}

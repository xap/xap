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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Embedded Sync- info regarding transferred records per segment used in order to avoid updating the
 * entry each time op was transferred to regular list
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSyncTransferredInfo implements SmartExternalizable {

    private static final long serialVersionUID = 1L;


    private long _generationId; //needed if during initial load records not transferred to synclist (performance!!!)
    private int _segmentNum; //the segment resulting
    //the following is non null for current generation only
    private Map<Integer, EmbeddedSyncTransferredCellInfo> _tranferredCells;
    //the following is non null for old generations only
    private TreeMap<Integer, OldGenCellInfo> _integratedCells; //key is upto
    private int _currentGenerationFullCells;
    private boolean _isCurrentGenId;

    public EmbeddedSyncTransferredInfo(long generationId, int segmentNum) {
        _generationId = generationId;
        _segmentNum = segmentNum;
        _tranferredCells = new HashMap<Integer, EmbeddedSyncTransferredCellInfo>();
        _isCurrentGenId = true;
    }

    public EmbeddedSyncTransferredInfo() {
    }


    public static String getStorageKey(long generationId, int segmentNum) {
        return "SYNC_ADMIN_EBEDDED_SEGMENT_INFO_" + generationId + "_" + segmentNum;
    }

    public long getGenerationId() {
        return _generationId;
    }

    public int getSegmentNum() {
        return _segmentNum;
    }


    //this segment+seq opinfo has been transferred to the regular list update the controlloing record for the segment
    public boolean updateTranferredInfo(int seq) {
        int from = seq - (seq % EmbeddedSyncTransferredCellInfo.TRANFERRED_CELL_CAPACITY_LIMIT);
        int to = from + EmbeddedSyncTransferredCellInfo.TRANFERRED_CELL_CAPACITY_LIMIT - 1;
        EmbeddedSyncTransferredCellInfo ci = _tranferredCells.get(from);
        if (ci == null) {//first one
            ci = new EmbeddedSyncTransferredCellInfo(from, to);
            _tranferredCells.put(from, ci);
        }
        if (ci.isCellFull())
            throw new RuntimeException("invalid situation- cell is full segment=" + _segmentNum + " generation=" + _generationId);
        ci.addToTransferred();
        if (ci.isCellFull()) {
            _currentGenerationFullCells++;
            integrateCellsIfPossible(ci);
        }
        return ci.isCellFull();  //need to update in SSD this info  record
    }


    private void integrateCellsIfPossible(EmbeddedSyncTransferredCellInfo ci) {
        EmbeddedSyncTransferredCellInfo curFull = ci;
        while (_tranferredCells.size() > 1) {
            EmbeddedSyncTransferredCellInfo r = null;
            Iterator<EmbeddedSyncTransferredCellInfo> iter = _tranferredCells.values().iterator();
            while (iter.hasNext()) {
                EmbeddedSyncTransferredCellInfo cur = iter.next();
                if (cur == curFull)
                    continue;
                if (cur.canJoin(curFull)) {
                    r = cur;
                    _currentGenerationFullCells--;
                    break;
                }
            }
            if (r == null)
                return;
            EmbeddedSyncTransferredCellInfo newCur = new EmbeddedSyncTransferredCellInfo(r, curFull);
            _tranferredCells.remove(r.getFromSeqNumber());
            _tranferredCells.remove(curFull.getFromSeqNumber());
            _tranferredCells.put(newCur.getFromSeqNumber(), newCur);
            curFull = newCur;
        }
    }

    //is this seq number transferred ?
    public boolean isSeqNumberCellTransferred(int seqNumber) {
        if (_integratedCells == null && _isCurrentGenId)
            throw new RuntimeException("internal error: invalid call -relevant for old gen only!");

        if (_integratedCells == null)
            return true;
        Map.Entry<Integer, OldGenCellInfo> res = _integratedCells.ceilingEntry(seqNumber);
        return (res != null && res.getValue().getFrom() <= seqNumber);

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        PlatformLogicalVersion myversion = (PlatformLogicalVersion) in.readObject();
        _integratedCells = new TreeMap<Integer, OldGenCellInfo>();
        _generationId = in.readLong();
        _segmentNum = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OldGenCellInfo oi = new OldGenCellInfo(in.readInt(), in.readInt());
            _integratedCells.put(oi.getUpTo(), oi);
        }


    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (_integratedCells != null)
            throw new RuntimeException("internal error: should write new gen only!");
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        out.writeObject(curversion);
        out.writeLong(_generationId);
        out.writeInt(_segmentNum);
        out.writeInt(_currentGenerationFullCells);
        Iterator<EmbeddedSyncTransferredCellInfo> iter = _tranferredCells.values().iterator();
        while (iter.hasNext()) {
            EmbeddedSyncTransferredCellInfo cur = iter.next();
            if (!cur.isCellFull())
                continue;
            out.writeInt(cur.getFromSeqNumber());
            out.writeInt(cur.getUptoSeqNumber());
        }
    }

    StringBuffer displayInfo() {
        StringBuffer sb = new StringBuffer();
        Iterator<EmbeddedSyncTransferredCellInfo> iter = _tranferredCells.values().iterator();
        while (iter.hasNext()) {
            EmbeddedSyncTransferredCellInfo c = iter.next();
            sb.append(c.displayInfo());
        }
        return sb;
    }

    void print() {
        // System.out.println("updtrans segment=" + _segmentNum + " seq=" +seq + " cells=" + displayInfo().toString() + " numfull=" + _currentGenerationFullCells + " thisfull=" + ci.isCellFull() ) ;

    }


    public static class OldGenCellInfo {
        private final int _from;
        private final int _upTo;

        OldGenCellInfo(int from, int upTo) {
            _from = from;
            _upTo = upTo;
        }

        public int getFrom() {
            return _from;
        }

        public int getUpTo() {
            return _upTo;
        }
    }

}

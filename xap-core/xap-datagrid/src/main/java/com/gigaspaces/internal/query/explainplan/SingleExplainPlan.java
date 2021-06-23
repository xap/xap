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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.SpaceCollectionIndex;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.index.CompoundIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.RelationRange;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class SingleExplainPlan implements SmartExternalizable {
    private static final long serialVersionUID = 2756216225619001564L;
    //    private static final long serialVersionUID =
    private String partitionId;
    private QueryOperationNode root;
    private Map<String, List<IndexChoiceNode>> indexesInfo;
    private Map<String, ScanningInfo> scanningInfo; // Pair = (int scanned, int matched)
    private Map<String, List<String>> tiersInfo;
    private Map<String, List<String>> aggregatorsInfo;

    public SingleExplainPlan() {
        this.scanningInfo = new HashMap<>();
        this.indexesInfo = new HashMap<>();
        this.tiersInfo = new HashMap<>();
        this.aggregatorsInfo = new HashMap<>();
    }

    public SingleExplainPlan(ICustomQuery customQuery) {
        this.scanningInfo = new HashMap<>();
        this.indexesInfo = new HashMap<>();
        this.tiersInfo = new HashMap<>();
        this.aggregatorsInfo = new HashMap<>();
        this.root = ExplainPlanUtil.buildQueryTree(customQuery);
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public void setRoot(QueryOperationNode root) {
        this.root = root;
    }

    public void setIndexesInfo(Map<String, List<IndexChoiceNode>> indexesInfo) {
        this.indexesInfo = indexesInfo;
    }

    public void setScanningInfo(Map<String, ScanningInfo> scanningInfo) {
        this.scanningInfo = scanningInfo;
    }

    public Map<String, List<String>> getAggregatorsInfo() {
        return this.aggregatorsInfo;
    }

    public void addAggregatorsInfo(String aggregatorName, String value) {
        if(!this.aggregatorsInfo.containsKey(aggregatorName)) {
            List<String> values = new ArrayList<>();
            this.aggregatorsInfo.put(aggregatorName, values);
        }
        this.aggregatorsInfo.get(aggregatorName).add(value);
    }

    public void addTiersInfo(String type, List<String> tiers) {
        this.tiersInfo.put(type, tiers);
    }

    public String getPartitionId() {
        return partitionId;
    }

    public QueryOperationNode getRoot() {
        return root;
    }

    public Map<String, List<IndexChoiceNode>> getIndexesInfo() {
        return indexesInfo;
    }

    public Map<String, ScanningInfo> getScanningInfo() {
        return scanningInfo;
    }

    public Map<String, List<String>> getTiersInfo() {
        return tiersInfo;
    }

    public void addIndexesInfo(String type, List<IndexChoiceNode> scanSelectionTree) {
        this.indexesInfo.put(type, scanSelectionTree);
    }

    public void addScanIndexChoiceNode(String clazz, IndexChoiceNode indexChoiceNode) {
        if (!indexesInfo.containsKey(clazz)) {
            List<IndexChoiceNode> scanSelectionTree = new ArrayList<>();
            indexesInfo.put(clazz, scanSelectionTree);
        }
        indexesInfo.get(clazz).add(indexChoiceNode);
    }

    public List<IndexChoiceNode> getScanSelectionTree(String clazz) {
        return indexesInfo.get(clazz);
    }

    public IndexChoiceNode getLatestIndexChoiceNode(String clazz) {
        if (indexesInfo.size() == 0)
            return null;
        List<IndexChoiceNode> scanSelectionTree = indexesInfo.get(clazz);
        return scanSelectionTree.get(scanSelectionTree.size() - 1);
    }

    public Integer getNumberOfScannedEntries(String clazz) {
        ScanningInfo defaultValue = new ScanningInfo(0, 0);
        return scanningInfo.getOrDefault(clazz, defaultValue).getScanned();

    }

    public Integer getNumberOfMatchedEntries(String clazz) {
        ScanningInfo defaultValue = new ScanningInfo(0, 0);
        return scanningInfo.getOrDefault(clazz, defaultValue).getMatched();
    }

    public void incrementScanned(String clazz) {
        if (!scanningInfo.containsKey(clazz)) {
            ScanningInfo info = new ScanningInfo();
            scanningInfo.put(clazz, info);
        }
        ScanningInfo info = this.scanningInfo.get(clazz);
        info.setScanned(info.getScanned() + 1);
    }

    public void incrementMatched(String clazz) {
        if (!scanningInfo.containsKey(clazz)) {
            ScanningInfo info = new ScanningInfo();
            scanningInfo.put(clazz, info);
        }
        ScanningInfo info = this.scanningInfo.get(clazz);
        info.setMatched(info.getMatched() + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SingleExplainPlan)) return false;

        SingleExplainPlan that = (SingleExplainPlan) o;

        if (!Objects.equals(partitionId, that.partitionId)) {
            return false;
        }
        if (!Objects.equals(root, that.root)) {
            return false;
        }
        if (!Objects.equals(indexesInfo, that.indexesInfo)) {
            return false;
        }
        if (!Objects.equals(tiersInfo, that.tiersInfo)) {
            return false;
        }
        if (!Objects.equals(aggregatorsInfo, that.aggregatorsInfo)) {
            return false;
        }
        return Objects.equals(scanningInfo, that.scanningInfo);
    }

    @Override
    public int hashCode() {
        int result = partitionId != null ? partitionId.hashCode() : 0;
        result = 31 * result + (root != null ? root.hashCode() : 0);
        result = 31 * result + (indexesInfo != null ? indexesInfo.hashCode() : 0);
        result = 31 * result + (scanningInfo != null ? scanningInfo.hashCode() : 0);
        result = 31 * result + (tiersInfo != null ? tiersInfo.hashCode() : 0);
        result = 31 * result + (aggregatorsInfo != null ? aggregatorsInfo.hashCode() : 0);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(root);
        IOUtils.writeString(objectOutput, partitionId);
        writeIndexes(objectOutput);
        writeScannigInfo(objectOutput);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            IOUtils.writeMapStringListString(objectOutput, tiersInfo);
        }
        IOUtils.writeMapStringListString(objectOutput, aggregatorsInfo);
    }

    private void writeScannigInfo(ObjectOutput objectOutput) throws IOException {
        int length = scanningInfo.size();
        objectOutput.writeInt(length);
        for (Map.Entry<String, ScanningInfo> entry : scanningInfo.entrySet()) {
            objectOutput.writeObject(entry.getKey());
            objectOutput.writeObject(entry.getValue());
        }
    }

    private void writeIndexes(ObjectOutput objectOutput) throws IOException {
        int length = indexesInfo.size();
        objectOutput.writeInt(length);
        for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
            objectOutput.writeObject(entry.getKey());
            if (entry.getValue() == null)
                objectOutput.writeInt(-1);
            else {
                int listLength = entry.getValue().size();
                objectOutput.writeInt(listLength);
                for (int i = 0; i < listLength; i++)
                    objectOutput.writeObject(entry.getValue().get(i));
            }
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.root = (QueryOperationNode) objectInput.readObject();
        this.partitionId = IOUtils.readString(objectInput);
        this.indexesInfo = readIndexes(objectInput);
        this.scanningInfo = readScanningInfo(objectInput);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            this.tiersInfo = IOUtils.readMapStringListString(objectInput);
        }
        this.aggregatorsInfo = IOUtils.readMapStringListString(objectInput);
    }

    private Map<String, ScanningInfo> readScanningInfo(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        int length = objectInput.readInt();
        Map<String, ScanningInfo> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            String key = (String) objectInput.readObject();
            ScanningInfo val = (ScanningInfo) objectInput.readObject();
            map.put(key, val);
        }
        return map;
    }

    private Map<String, List<IndexChoiceNode>> readIndexes(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        Map<String, List<IndexChoiceNode>> map = null;
        int length = objectInput.readInt();
        if (length >= 0) {
            map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {
                String key = (String) objectInput.readObject();
                List<IndexChoiceNode> list = null;
                int listLength = objectInput.readInt();
                if (listLength >= 0) {
                    list = new ArrayList<>(listLength);
                    for (int j = 0; j < listLength; j++)
                        list.add((IndexChoiceNode) objectInput.readObject());
                }
                map.put(key, list);
            }
        }

        return map;
    }

    public static void validate(long timeout, boolean blobStoreCachePolicy, int operationModifiers, ICustomQuery customQuery, Map<String, SpaceIndex> indexes) {
        if(timeout != 0){
            throw new UnsupportedOperationException("Sql explain plan does not support timeout operations");
        }
        if(blobStoreCachePolicy){
            throw new UnsupportedOperationException("Sql explain plan does not support off-heap cache policy");
        }
        if(Modifiers.contains(operationModifiers, Modifiers.FIFO_GROUPING_POLL)){
            throw new UnsupportedOperationException("Sql explain plan does not support FIFO grouping");
        }
        if (customQuery != null) {
            validateQueryTypes(customQuery);
        }
        validateIndexesTypes(indexes);
    }

    private static void validateIndexesTypes(Map<String, SpaceIndex> indexMap) {
        for (SpaceIndex spaceIndex : indexMap.values()) {
            if(spaceIndex instanceof CompoundIndex){
                throw new UnsupportedOperationException("Sql explain plan does not support compound index");
            }
            if(spaceIndex instanceof SpaceCollectionIndex){
                throw new UnsupportedOperationException("Sql explain plan does not support collection index");
            }
        }
    }

    private static void validateQueryTypes(ICustomQuery customQuery) {

        if(customQuery instanceof RelationRange){
            throw new UnsupportedOperationException("Sql explain plan does not support geo-spatial type queries");
        }
        if(customQuery instanceof Range && ((Range) customQuery).getFunctionCallDescription() != null){
            throw new UnsupportedOperationException("Sql explain plan does not support sql function type queries");
        }
        if(ExplainPlanUtil.getSubQueries(customQuery) != null){
            for( ICustomQuery subQuery : ExplainPlanUtil.getSubQueries(customQuery)){
                validateQueryTypes(subQuery);
            }
        }
    }
}

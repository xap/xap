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

package com.gigaspaces.metadata.fifoGrouping;

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingIndex;
import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class FifoGroupingBasicPojo {
    private Integer id;
    private String symbol;
    private String reporter;
    private boolean processed;
    private Info info;
    private List<String> formerReporters;
    private Timestamp time;

    public FifoGroupingBasicPojo() {
    }

    public FifoGroupingBasicPojo(Integer id, String symbol, String reporter) {
        this(id, symbol, reporter, null);
    }

    public FifoGroupingBasicPojo(Integer id, String symbol, String reporter, Info info) {
        this.id = id;
        this.symbol = symbol;
        this.reporter = reporter;
        this.processed = false;
        this.info = info;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @SpaceFifoGroupingProperty
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    @SpaceFifoGroupingIndex
    public String getReporter() {
        return reporter;
    }

    public void setReporter(String reporter) {
        this.reporter = reporter;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    @SpaceFifoGroupingIndex
    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    @SpaceIndex(path = "timeStamp", type = SpaceIndexType.EQUAL_AND_ORDERED)
    @SpaceFifoGroupingIndex
    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    @SpaceIndex
    public List<String> getFormerReporters() {
        return formerReporters;
    }

    public void setFormerReporters(List<String> reporters) {
        this.formerReporters = reporters;
    }

    @SpaceIndex(path = "nanos")
    @SpaceFifoGroupingIndex(path = "nanos")
    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public static class Info {
        private String[] lastReports;
        private Timestamp timeStamp;
        private Object scans;

        String[] getLastReports() {
            return lastReports;
        }

        Timestamp getTimeStamp() {
            return timeStamp;
        }

        Object getScans() {
            return scans;
        }

    }

    public static String getFifoGroupingPropertyName() {
        return "symbol";
    }

    public static Map<String, SpaceIndexType> getIndexes() {
        Map<String, SpaceIndexType> indexes = new HashMap<String, SpaceIndexType>();
        indexes.put("id", SpaceIndexType.EQUAL);
        indexes.put("symbol", SpaceIndexType.EQUAL);
        indexes.put("reporter", SpaceIndexType.EQUAL);
        indexes.put("processed", SpaceIndexType.EQUAL_AND_ORDERED);
        indexes.put("info", SpaceIndexType.EQUAL);
        indexes.put("info.timeStamp", SpaceIndexType.EQUAL_AND_ORDERED);
        indexes.put("formerReporters", SpaceIndexType.EQUAL);
        indexes.put("time.nanos", SpaceIndexType.EQUAL);
        return indexes;
    }

    public static String[] getFifoGroupingIndexes() {
        String[] res = {"reporter", "processed", "info", "time.nanos"};
        return res;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj instanceof FifoGroupingBasicPojo) {
            FifoGroupingBasicPojo fifoGroupingPojo = (FifoGroupingBasicPojo) obj;
            return fifoGroupingPojo.getId().equals(this.id);
        }
        return false;
    }

}

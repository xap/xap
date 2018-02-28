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
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.util.HashMap;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class FifoGroupingBasicInheritPojo extends FifoGroupingBasicPojo {

    public FifoGroupingBasicInheritPojo() {
        super();
    }

    public FifoGroupingBasicInheritPojo(Integer id, String symbol, String reporter, Info info) {
        super(id, symbol, reporter, info);
    }

    public FifoGroupingBasicInheritPojo(Integer id, String symbol, String reporter) {
        super(id, symbol, reporter);
    }

    @Override
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getSymbol() {
        // TODO Auto-generated method stub
        return super.getSymbol();
    }

    @Override
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getReporter() {
        // TODO Auto-generated method stub
        return super.getReporter();
    }

    @Override
    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public boolean isProcessed() {
        // TODO Auto-generated method stub
        return super.isProcessed();
    }

    @Override
    @SpaceFifoGroupingIndex(path = "scans")
    public Info getInfo() {
        // TODO Auto-generated method stub
        return super.getInfo();
    }

    public static Map<String, SpaceIndexType> getIndexes() {
        Map<String, SpaceIndexType> indexes = new HashMap<String, SpaceIndexType>();
        indexes.put("id", SpaceIndexType.EQUAL);
        indexes.put("symbol", SpaceIndexType.EQUAL_AND_ORDERED);
        indexes.put("reporter", SpaceIndexType.EQUAL_AND_ORDERED);
        indexes.put("processed", SpaceIndexType.EQUAL);
        indexes.put("info", SpaceIndexType.EQUAL);
        indexes.put("info.timeStamp", SpaceIndexType.EQUAL_AND_ORDERED);
        indexes.put("info.scans", SpaceIndexType.EQUAL);
        indexes.put("formerReporters", SpaceIndexType.EQUAL);
        indexes.put("time.nanos", SpaceIndexType.EQUAL);

        return indexes;
    }

    public static String[] getFifoGroupingIndexes() {
        String[] res = {"reporter", "processed", "info", "info.scans", "time.nanos"};
        return res;
    }
}

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

package com.j_spaces.jdbc.parser;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This node represents the rownum command. it has its start index and the number of rows to
 * collect
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class RowNumNode extends ValueNode {
    private static final long serialVersionUID = 1L;

    private int startIndex; //preset value
    private int endIndex; //preset value

    //additional limitation on the results number - set per SQLQuery
    private int maxResults = Integer.MAX_VALUE;

    public RowNumNode() {
    }

    public RowNumNode(int start, int end) {
        this.startIndex = Math.min(start, end);
        this.endIndex = Math.max(start, end);
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
    }

    /**
     * @return return true if results number has a limit else return false
     */
    public boolean hasLimit() {
        if (getStartIndex() <= 1 && endIndex == Integer.MAX_VALUE && maxResults == Integer.MAX_VALUE)
            return false;

        return true;
    }

    /**
     * @return true if RowNumNode has limitation on the results number
     */
    public int getLimit() {
        if (!hasLimit())
            return Integer.MAX_VALUE;

        long maxResultsLimit = startIndex + (long) maxResults - 1;
        return (int) Math.min(maxResultsLimit, endIndex);
    }

    public boolean isIndexOutOfRange(int i) {
        if (i < getStartIndex())
            return true;
        if (i > getLimit())
            return true;

        return false;
    }

    @Override
    public String toString() {
        return "RowNumNode [startIndex=" + startIndex + ", endIndex="
                + endIndex + ", maxResults=" + maxResults + "]";
    }

    /**
     * Return a cloned SelectQuery
     */
    @Override
    public Object clone() {
        return new RowNumNode(startIndex, endIndex);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(startIndex);
        out.writeInt(endIndex);
        out.writeInt(maxResults);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        startIndex = in.readInt();
        endIndex = in.readInt();
        maxResults = in.readInt();
    }
}

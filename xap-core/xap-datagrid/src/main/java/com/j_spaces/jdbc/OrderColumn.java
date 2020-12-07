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

package com.j_spaces.jdbc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.gigaspaces.internal.transport.IEntryPacket;

/**
 * Created by anna on 11/26/14.
 */
@com.gigaspaces.api.InternalApi
public class OrderColumn extends SelectColumn {
    private static final long serialVersionUID = 1L;

    private boolean isDesc = false;

    private boolean nullsLast = false;

    public OrderColumn(String columnName, String columnAlias) {
        super(columnName, columnAlias);
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        if (getProjectedIndex() != -1) {
            if (entry instanceof JoinedEntry) {
                JoinedEntry joinedEntry = ((JoinedEntry) entry);
                return joinedEntry.getEntry(this.getColumnTableData().getTableIndex()).getFieldValue(this.getProjectedIndex());
            } else {
                return entry.getFieldValue(getProjectedIndex());
            }
        } else {
            return super.getFieldValue(entry);
        }
    }

    public OrderColumn() {
    }

    public boolean isDesc() {
        return isDesc;
    }

    public void setDesc(boolean isDesc) {
        this.isDesc = isDesc;
    }

    public boolean areNullsLast() {
        return nullsLast;
    }

    public void setNullsLast(boolean nullsLast) {
        this.nullsLast = nullsLast;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isDesc);
        out.writeBoolean(nullsLast);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        isDesc = in.readBoolean();
        nullsLast = in.readBoolean();
    }
}

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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.EntryPacket;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.jdbc.query.QueryTableData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents an entry that was constructed from a join between several tables.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class JoinedEntry extends EntryPacket implements Serializable {

    private static final long serialVersionUID = 1L;

    private IEntryPacket[] _entries;

    public JoinedEntry() {
    }

    public JoinedEntry(IEntryPacket... entries) {
        _entries = entries;
    }

    @Override
    public boolean equals(Object ob) {

        if (ob instanceof JoinedEntry) {
            JoinedEntry entry = (JoinedEntry) ob;
            if (this._entries.length != entry._entries.length)
                return false;
            for (int i = 0; i < _entries.length; i++) {
                if (!getEntry(i).equals(entry.getEntry(i)))
                    return false;
            }
            return true;
        }
        if (ob instanceof IEntryPacket) {
            //if one of the local _entries equals ob, its enough for us
            IEntryPacket entry = (IEntryPacket) ob;

            for (IEntryPacket thisEntry : _entries) {
                if (entry.equals(thisEntry))
                    return true;
            }

            return false;
        }
        return false;
    }

    public IEntryPacket getEntry(int index) {
        return _entries[index];
    }

    public int getSize() {
        if (_entries == null)
            return 0;
        return _entries.length;
    }

    public boolean isOuterJoin() {
        for (IEntryPacket entry : _entries) {
            if (entry == null)
                return true;
        }
        return false;
    }

    /**
     * Create columns projection from the joined entries
     */
    public void createProjection(List<SelectColumn> columns) {

        ArrayList<Object> fieldValues = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {

            SelectColumn column = columns.get(i);
            if (!column.isVisible())
                continue;

            QueryTableData columnTableData = column.getColumnTableData();

            if (columnTableData == null) {
                fieldValues.add(null);
                continue;
            }
            IEntryPacket entry = getEntry(columnTableData.getTableIndex());

            if (entry == null) {
                fieldValues.add(null);
            } else if (column.isUid()) {
                fieldValues.add(entry.getUID());

            } else {
                if (entry.getTypeDescriptor() == null) {
                    entry.setTypeDesc(columnTableData.getTypeDesc(), false);
                }
                fieldValues.add(column.getFieldValue(entry));
            }

        }

        setFieldsValues(fieldValues.toArray());
    }

    /*
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(_entries);
        return result;
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0))
            _entries = IOUtils.readEntryPacketArray(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        super.writeExternal(out, version);
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0))
            IOUtils.writeObjectArray(out, _entries);
    }

    @Override
    public String toString() {
        return "JoinedEntry{" +
                "_entries=" + Arrays.toString(_entries) +
                '}';
    }
}

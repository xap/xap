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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

/**
 * Represents an SQL UPDATE query column.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UpdateColumn extends SelectColumn {
    private static final long serialVersionUID = 1L;

    protected String _selfIncrementedColumnName;

    public UpdateColumn(String columnPath) {
        super(columnPath);
        _selfIncrementedColumnName = null;
    }

    public UpdateColumn() {
    }

    /**
     * Sets the self incremented column name.
     */
    public void setSelfIncrementedColumnName(String selfIncrementedColumnName) {
        _selfIncrementedColumnName = selfIncrementedColumnName;
    }

    /**
     * Gets the self incremented column name.
     */
    public String getSelfIncrementedColumnName() {
        return _selfIncrementedColumnName;
    }

    /**
     * Gets whether this column is a self incremented update column. SQL update example: UPDATE
     * table SET version = version + 1
     */
    public boolean isSelfIncremented() {
        return _selfIncrementedColumnName != null;
    }

    /**
     * Validates that the self incremented column name is the same as the column name.
     */
    public void validateSelfIncrementedColumnName(UpdateQuery updateQuery) throws SQLException {
        UpdateColumn selfIncrementedColumn = new UpdateColumn(_selfIncrementedColumnName);
        selfIncrementedColumn.createColumnData(updateQuery);
        if (!getName().equals(selfIncrementedColumn.getName()))
            throw new SQLException("Operator '+' is only allowed for the same column.");
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, _selfIncrementedColumnName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _selfIncrementedColumnName = IOUtils.readString(in);
    }
}

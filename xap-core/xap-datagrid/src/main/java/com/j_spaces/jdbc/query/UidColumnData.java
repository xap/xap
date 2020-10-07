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

package com.j_spaces.jdbc.query;

import java.sql.SQLException;

/**
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class UidColumnData extends QueryColumnData {

    public UidColumnData(QueryTableData tableData) {
        super(tableData, QueryColumnData.UID_COLUMN);
    }

    @Override
    public boolean checkAndAssignTableData(QueryTableData tableData)
            throws SQLException {
        // every table has uid - so if it is already set - ambiguous expression
        QueryTableData columnTableData = getColumnTableData();
        if (columnTableData != null && !columnTableData.getTableName().equals(tableData.getTableName()))
            throw new SQLException("Ambiguous UID column . It is defined in [" + columnTableData.getTableName() + "] and [" + tableData.getTableName() + "]");

        setColumnTableData(tableData);
        return true;
    }

    @Override
    public boolean isUidColumn() {
        return true;
    }
}

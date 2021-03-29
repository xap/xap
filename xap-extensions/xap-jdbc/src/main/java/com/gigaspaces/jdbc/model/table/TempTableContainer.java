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
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        return tableResult;
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return tableResult.getQueryColumns();
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableResult.getQueryColumns().stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void addRange(Range range) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setLimit(Integer value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }
}

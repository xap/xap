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
package com.gigaspaces.sql.aggregatornode.netty.server.output;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by moran on 6/8/18.
 */
public class Output {

    public static String EMPTY = "";
    private List<String> columns = new LinkedList<String>();
    private List<Integer> columnWidth = new LinkedList<Integer>();

    private List<List<String>> rows = new LinkedList<List<String>>();

    private boolean upperCaseCols = false;

    public Output upperCaseColumns(boolean upperCaseCols) {
        this.upperCaseCols = upperCaseCols;
        return this;
    }

    public Output addColumn(String column) {
        final int index = columns.size();
        columns.add(upperCaseCols ? column.toUpperCase() : column);
        updateColumnWidth(index, column.length());
        return this;
    }

    public Output addColumns(String... columns) {
        for (String column : columns) {
            addColumn(column);
        }
        return this;
    }

    public Output addColumns(Collection<String> columns) {
        for (String column : columns) {
            addColumn(column);
        }
        return this;
    }

    private void updateColumnWidth(int index, int length) {
        if (index < columnWidth.size()) {
            columnWidth.set(index, Math.max(columnWidth.get(index), length));
        } else {
            columnWidth.add(length);
        }
    }

    public Output addRow(List<String> row) {
        row = row.stream().map(x->x.replace("\t","  ")).collect(Collectors.toList());
        rows.add(row);
        for (int i=0; i<row.size(); i++) {
            updateColumnWidth(i, row.get(i).length());
        }
        return this;
    }

    public Output addRow(String... row) {
        return addRow(Arrays.asList(row));
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public List<Integer> getColumnWidth() {
        return columnWidth;
    }
}

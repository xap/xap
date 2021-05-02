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
package com.gigaspaces.jdbc.model.result;

import java.util.Iterator;
import java.util.List;

public class RowScanCursor implements Cursor<TableRow> {
    private final List<TableRow> rows;
    private Iterator<TableRow> iterator;
    private TableRow current;

    public RowScanCursor(List<TableRow> rows) {
        this.rows = rows;
    }

    @Override
    public boolean next() {
        if (iterator().hasNext()) {
            current = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public TableRow getCurrent() {
        return current;
    }

    @Override
    public void reset() {
        iterator = null;
    }

    @Override
    public boolean isBeforeFirst() {
        return iterator == null;
    }

    private Iterator<TableRow> iterator() {
        if (iterator == null)
            iterator = rows.iterator();
        return iterator;
    }
}

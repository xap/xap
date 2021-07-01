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

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.*;

import java.util.List;

public class TableRowFactory {

    public static TableRow createTableRowFromSpecificColumns(List<IQueryColumn> columns,
                                                             List<OrderColumn> orderColumns,
                                                             List<ConcreteColumn> groupByColumns) {
        return new TableRow(columns, orderColumns, groupByColumns);
    }

    public static TableRow createTableRowFromIEntryPacket(IEntryPacket entryPacket, ConcreteTableContainer tableContainer) {
        return new TableRow(entryPacket, tableContainer);
    }

    public static TableRow createProjectedTableRow(TableRow row, TempTableContainer tempTableContainer) {
        return new TableRow(row, tempTableContainer);
    }
}

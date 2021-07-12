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

import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;

import java.util.function.Predicate;

public class TempTableQTP extends QueryTemplatePacket {
    private Predicate<TableRow> predicate;

    public TempTableQTP() {
    }

    private TempTableQTP(Predicate<TableRow> predicate) {
        this.predicate = predicate;
    }

    public TempTableQTP(EqualValueRange range) {
         predicate = (tableRow) -> tableRow.getPropertyValue(range.getPath()).equals(range.getValue());
    }

    public TempTableQTP(Range range) {
        predicate = (tableRow -> range.getPredicate().execute(tableRow.getPropertyValue(range.getPath())));
    }

    @Override
    public QueryTemplatePacket and(QueryTemplatePacket template) {
        if (!(template instanceof TempTableQTP)) throw new UnsupportedOperationException("unsupported");
        return new TempTableQTP((tableRow) -> ((TempTableQTP) template).predicate.test(tableRow) && this.predicate.test(tableRow));
    }

    @Override
    public QueryTemplatePacket union(QueryTemplatePacket template) {
        if (!(template instanceof TempTableQTP)) throw new UnsupportedOperationException("unsupported");
        return new TempTableQTP((tableRow) -> ((TempTableQTP) template).predicate.test(tableRow) || this.predicate.test(tableRow));
    }

    public boolean eval(TableRow tableRow) {
        return predicate.test(tableRow);
    }
}

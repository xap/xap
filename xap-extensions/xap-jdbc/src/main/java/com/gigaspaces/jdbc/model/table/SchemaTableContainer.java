package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.calcite.schema.GSSchemaTable;
import com.j_spaces.core.IJSpace;

public class SchemaTableContainer extends TempTableContainer {


    public SchemaTableContainer(GSSchemaTable table, IJSpace space) {
        super(null, null);

    }

}

package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;

public interface GSTable extends Table, Wrapper {
    String getName();
    TableContainer createTableContainer(IJSpace space);
}

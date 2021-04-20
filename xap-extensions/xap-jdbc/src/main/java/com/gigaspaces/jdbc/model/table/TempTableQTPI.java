package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;

public interface TempTableQTPI {
    boolean matches(TableRow tableRow);
}
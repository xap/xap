package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;

public interface ICaseCondition {

    public boolean check(TableRow tableRow);

    public Object getResult();

    public void setResult(Object result);
}

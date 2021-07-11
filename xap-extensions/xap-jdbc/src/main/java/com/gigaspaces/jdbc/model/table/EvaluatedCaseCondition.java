package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;

public class EvaluatedCaseCondition implements ICaseCondition{

    private final boolean value;
    private Object result;


    public EvaluatedCaseCondition(boolean value, Object result) {
        this.value = value;
        this.result = result;
    }

    @Override
    public boolean check(TableRow tableRow) {
        return value;
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return value + " -> " + result;
    }
}

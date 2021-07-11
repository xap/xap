package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;

import java.util.LinkedList;

public class CompoundCaseCondition implements ICaseCondition{

    private final LinkedList<CompoundConditionCode> conditionCodes;
    private final LinkedList<ICaseCondition> caseConditions;
    private Object result;

    public CompoundCaseCondition() {
        this.conditionCodes = new LinkedList<>();
        this.caseConditions = new LinkedList<>();
    }

    @Override
    public boolean check(TableRow tableRow) {
        //TODO: @sagiv think of better way without copy...
        LinkedList<CompoundConditionCode> conditionCodes1 = new LinkedList<>(conditionCodes);
        LinkedList<ICaseCondition> caseConditions1 = new LinkedList<>(caseConditions);
        while (!conditionCodes1.isEmpty()) {
            CompoundConditionCode compoundConditionCode = conditionCodes1.removeLast(); //TODO: @sagiv validate the order
            ICaseCondition first = caseConditions1.removeFirst();
            ICaseCondition second = caseConditions1.removeFirst();
            switch (compoundConditionCode) {
                case OR:
                    if (first.check(tableRow) || second.check(tableRow)) {
                        caseConditions1.addLast(new EvaluatedCaseCondition(true, result));
                    } else {
                        caseConditions1.addLast(new EvaluatedCaseCondition(false, result));
                    }
                    break;
                case AND:
                    if (first.check(tableRow) && second.check(tableRow)) {
                        caseConditions1.addLast(new EvaluatedCaseCondition(true, result));
                    } else {
                        caseConditions1.addLast(new EvaluatedCaseCondition(false, result));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported compound condition code");

            }
        }
        return caseConditions1.removeFirst().check(tableRow);
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void setResult(Object result) {
        this.result = result;
    }

    public void addCompoundConditionCode(CompoundConditionCode code) {
        this.conditionCodes.addLast(code);
    }

    public void addCaseCondition(ICaseCondition condition) {
        this.caseConditions.addLast(condition);
    }

    public enum CompoundConditionCode {
        AND, OR
    }
}

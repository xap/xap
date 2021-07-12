package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;

import java.util.ArrayDeque;
import java.util.Deque;

public class CompoundCaseCondition implements ICaseCondition {

    private final Deque<Object> stack;
    private Object result;

    public CompoundCaseCondition() {
        this.stack = new ArrayDeque<>();
    }

    @Override
    public boolean check(TableRow tableRow) {
        ArrayDeque<Object> stackCopy = new ArrayDeque<>(this.stack);
        Object top = stackCopy.poll();
        return checkHelper(tableRow, stackCopy, top);
    }

    private boolean checkHelper(TableRow tableRow, Deque<Object> stack, Object obj) {
        if (obj == null) {
            return false;
        } else if (stack.size() == 1) {
            return ((ICaseCondition) obj).check(tableRow);
        } else if (obj instanceof CompoundConditionCode) {
            Object first = stack.poll();
            boolean firstEvaluation = checkHelper(tableRow, stack, first);
            Object second = stack.poll();
            boolean secondEvaluation = checkHelper(tableRow, stack, second);
            switch ((CompoundConditionCode) obj) {
                case AND:
                    return firstEvaluation && secondEvaluation;
                case OR:
                    return firstEvaluation || secondEvaluation;
                default:
                    throw new UnsupportedOperationException("unsupported compound condition code: " + obj);
            }
        } else if (obj instanceof ICaseCondition) {
            return ((ICaseCondition) obj).check(tableRow);
        }
        throw new IllegalStateException("should not arrive here");
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
        this.stack.add(code);
    }

    public void addCaseCondition(ICaseCondition condition) {
        this.stack.add(condition);
    }

    public enum CompoundConditionCode {
        AND, OR
    }
}

package com.j_spaces.jdbc;


import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.math.MutableNumber;
import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SumColumn extends SelectColumn {


    private SelectColumn left;
    private SelectColumn right;
    private String operator;
    private QueryTableData columnTableData;


    public SumColumn(String left, String right, String operator) {
        this.left = new SelectColumn(left);
        this.right = new SelectColumn(right);
        this.operator = operator;
    }

    public SumColumn(SumColumn left, String right, String operator) {
        this.left = left;
        this.right = new SelectColumn(right);
        this.operator = operator;
    }

    public SumColumn(SumColumn left, Integer right, String operator) {
        this.left = left;
        this.right = new ValueSelectColumn(right);
        this.operator = operator;
    }

    public SumColumn(String left, SumColumn right, String operator) {
        this.left = new SelectColumn(left);
        this.right = right;
        this.operator = operator;
    }

    @Override
    public void createColumnData(AbstractDMLQuery query) throws SQLException {
        if (!(left instanceof ValueSelectColumn)) {
            left.createColumnData(query);
            this.columnTableData = left.getColumnTableData();
        }
        if (!(right instanceof ValueSelectColumn)) {
            right.createColumnData(query);
            this.columnTableData = right.getColumnTableData();
        }
    }

    @Override
    public boolean isAllColumns() {
        return false;
    }

    @Override
    public boolean isUid() {
        return false;
    }

    @Override
    public QueryTableData getColumnTableData() {
        return columnTableData;
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        return calc(entry, this).toNumber();
    }

    public SelectColumn getLeft() {
        return left;
    }

    public SumColumn setLeft(SelectColumn left) {
        this.left = left;
        return this;
    }

    public SelectColumn getRight() {
        return right;
    }

    public SelectColumn setRight(SelectColumn right) {
        this.right = right;
        return this;
    }

    private MutableNumber add(MutableNumber total, Number addition) {
        if (addition != null) {
            if (total == null)
                total = MutableNumber.fromClass(addition.getClass(), true);
            total.add(addition);
        }
        return total;
    }

    private MutableNumber remove(MutableNumber total, Number remove) {
        if (remove != null) {
            if (total == null)
                total = MutableNumber.fromClass(remove.getClass(), true);
            total.remove(remove);
        }
        return total;
    }



    private MutableNumber calc(IEntryPacket entry, SumColumn sumColumn) {
        MutableNumber total = null;
        if (sumColumn.getLeft() instanceof SumColumn) {
            total = add(total, calc(entry, (SumColumn) sumColumn.getLeft()).toNumber());
        } else {
            total = add(total, (Number) sumColumn.getLeft().getFieldValue(entry));
        }

        if (sumColumn.getRight() instanceof SumColumn) {
            total = add(total, calc(entry, (SumColumn) sumColumn.getRight()).toNumber());
        } else if (sumColumn.getRight() instanceof ValueSelectColumn) {
            if (sumColumn.getOperator().equals("-")) {
                total = remove(total, (Number) sumColumn.getRight().getValue());
            } else {
                total = add(total, (Number) sumColumn.getRight().getValue());
            }
        } else {
            Object rightVal = sumColumn.getRight().getFieldValue(entry);
            Number right = rightVal instanceof MutableNumber ? ((MutableNumber) rightVal).toNumber() : (Number)rightVal;
            if (sumColumn.getOperator().equals("-")) {
                total = remove(total, right);
            } else {
                total = add(total, right);
            }
        }
        return total;
    }

    @Override
    public void setAlias(String alias) {
        super.setAlias(alias);
        if (this.getName() == null) this.setName(alias);

        if (left.getName() == null) left.setAlias(alias);
        if (right.getName() == null) right.setAlias(alias);

    }

    public String getOperator() {
        return operator;
    }

    public SumColumn setOperator(String operator) {
        this.operator = operator;
        return this;
    }

    List<String> getColumnNames() {
        List<String> names = new ArrayList<>();
        if (getLeft() instanceof SumColumn) {
            names.addAll(((SumColumn) getLeft()).getColumnNames());
        } else if (getLeft() instanceof ValueSelectColumn) {

        } else {
            names.add(getLeft().getName());
        }


        if (getRight() instanceof SumColumn) {
            names.addAll(((SumColumn) getRight()).getColumnNames());
        } else if (getRight() instanceof ValueSelectColumn) {

        } else {
            names.add(getRight().getName());
        }

        return names;
    }
}

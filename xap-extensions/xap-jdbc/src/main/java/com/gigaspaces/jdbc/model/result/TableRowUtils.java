package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.AggregationFunctionType;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableRowUtils {

    public static TableRow aggregate(List<TableRow> tableRows, List<AggregationColumn> aggregationColumns) {
        if (tableRows.isEmpty()) {
            return new TableRow((IQueryColumn[]) null, null);
        }
        IQueryColumn[] rowsColumns = aggregationColumns.toArray(new IQueryColumn[0]);
        //TODO: @sagiv validate! if from first or use aggregateValues
        OrderColumn[] firstRowOrderColumns = tableRows.get(0).getOrderColumns();
        Object[] firstRowOrderValues = tableRows.get(0).getOrderValues();

        Object[] aggregateValues = new Object[rowsColumns.length];
        int index = 0;
        for (AggregationColumn aggregationColumn : aggregationColumns) {
            Object value = null;
            if(tableRows.get(0).hasColumn(aggregationColumn)) { // if this column already exists by reference.
                value = tableRows.get(0).getPropertyValue(aggregationColumn);
            } else {
                AggregationFunctionType type = aggregationColumn.getType();
                String columnName = aggregationColumn.getColumnName();
                switch (type) {
                    case COUNT:
                        boolean isAllColumn = aggregationColumn.isAllColumns();
                        if (isAllColumn) {
                            value = tableRows.size();
                        } else {
                            value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                    .filter(Objects::nonNull).count();
                        }
                        break;
                    case MAX:
                        value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).max(getObjectComparator()).orElse(null);
                        break;
                    case MIN:
                        value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).min(getObjectComparator()).orElse(null);
                        break;
                    case AVG:
                        //TODO: for now supported only Number.
                        List<Number> collect = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).collect(Collectors.toList());
                        value = collect.stream()
                                .reduce(0d, (a, b) -> a.doubleValue() + b.doubleValue()).doubleValue() / collect.size();
                        break;
                    case SUM:
                        //TODO: for now supported only Number.
                        value = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).reduce(0d, (a, b) -> a.doubleValue() + b.doubleValue()).doubleValue();
                        break;
                }
            }
            aggregateValues[index++] = value;
        }
        return new TableRow(rowsColumns, aggregateValues, firstRowOrderColumns, firstRowOrderValues);
    }

    private static Comparator<Object> getObjectComparator() {
        return (o1, o2) -> { //TODO: @sagiv add try catch block. like in use castToComparable from WhereHandler.
            Comparable first = (Comparable) o1;
            Comparable second = (Comparable) o2;
            return first.compareTo(second);
        };
    }

}

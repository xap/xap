package com.gigaspaces.jdbc.calcite.experimental.handlers;

import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.model.AggregationColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.AggregationFunctionType;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.PhysicalColumn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class AggregateHandler {
    private static AggregateHandler _instance;

    public static AggregateHandler instance(){
        if(_instance == null){
            _instance = new AggregateHandler();
        }
        return _instance;
    }

    private AggregateHandler() {

    }

    public void apply(GSAggregate gsAggregate, ResultSupplier resultSupplier){
        RelNode child = gsAggregate.getInput();
        resultSupplier.clearProjections();
        List<String> fields = child.getRowType().getFieldNames();
        for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
            groupSet.forEach(bit -> {
                String field = fields.get(bit);
                PhysicalColumn physicalColumn = new PhysicalColumn(field, null, resultSupplier);
                resultSupplier.addGroupByColumn(physicalColumn);
                resultSupplier.addProjection(physicalColumn);
            });
        }

        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            AggregationFunctionType aggregationFunctionType = AggregationFunctionType.valueOf(aggregateCall.getAggregation().getName().toUpperCase());
            IQueryColumn physicalColumn = resultSupplier.getOrCreatePhysicalColumn(fields.get(aggregateCall.getArgList().get(0)));
            AggregationColumn aggregationColumn = new AggregationColumn(aggregationFunctionType, "(null)", physicalColumn, false);
            resultSupplier.addAggregationColumn(aggregationColumn);
            resultSupplier.addProjection(aggregationColumn);
        }
    }
}

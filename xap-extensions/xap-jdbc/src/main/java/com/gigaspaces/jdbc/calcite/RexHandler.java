package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.jsql.QueryColumnHandler;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.NotEqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.*;

public class RexHandler extends RexShuttle {
    private final RexProgram program;
    private final List<String> fields = new ArrayList<>();

    public RexHandler(RexProgram program) {
        this.program = program;
    }


    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        fields.add(program.getInputRowType().getFieldNames().get(inputRef.getIndex()));
        return inputRef;
    }

    public List<String> getFields() {
        return fields;
    }
}

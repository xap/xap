package com.gigaspaces.jdbc.calcite.sql.extension;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class SqlShowOption extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW_OPTION", SqlKind.OTHER_FUNCTION) {
                @Override public SqlCall createCall(SqlLiteral functionQualifier,
                                                    SqlParserPos pos, SqlNode... operands) {
                    return new SqlShowOption(pos, (SqlIdentifier) operands[0]);
                }
            };
    SqlIdentifier name;

    public SqlShowOption(SqlParserPos pos, SqlIdentifier name) {
        super(pos);
        this.name = name;

        assert name != null;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getName() {
        return name;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        final List<SqlNode> operandList = new ArrayList<>();
        operandList.add(name);
        return ImmutableList.copyOf(operandList);
    }
}

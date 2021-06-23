package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowOption extends SqlBasicCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW_OPTION", SqlKind.OTHER_FUNCTION) {
                @Override public SqlCall createCall(SqlLiteral functionQualifier,
                                                    SqlParserPos pos, SqlNode... operands) {
                    return new SqlShowOption(pos, (SqlIdentifier) operands[0]);
                }
            };

    public SqlShowOption(SqlParserPos pos, SqlIdentifier name) {
        super(OPERATOR, new SqlNode[] {name}, pos);
    }

    public SqlIdentifier getName() {
        return (SqlIdentifier) operands[0];
    }
}

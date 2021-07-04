package com.gigaspaces.jdbc.calcite.sql.extension;

import com.gigaspaces.internal.utils.LazySingleton;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public class GSSqlOperatorTable extends ReflectiveSqlOperatorTable {
    private static final LazySingleton<GSSqlOperatorTable> INSTANCE = new LazySingleton<>(() -> {
        GSSqlOperatorTable operatorTable = new GSSqlOperatorTable();
        operatorTable.init();
        return operatorTable;
    });

    private GSSqlOperatorTable() {

    }

    public static GSSqlOperatorTable instance() {
        return INSTANCE.getOrCreate();
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // set caseSensitive=false to make sure the behavior is same with before.
        super.lookupOperatorOverloads(
                opName, category, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false));
    }

    // FUNCTIONS

    public static final SqlFunction RANDOM =
            new SqlFunction(
                    "RANDOM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE,
                    null,
                    OperandTypes.or(OperandTypes.NILADIC, OperandTypes.NUMERIC),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction LOG =
            new SqlFunction(
                    "LOG",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.NUMERIC,
                            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
                    SqlFunctionCategory.NUMERIC);
}

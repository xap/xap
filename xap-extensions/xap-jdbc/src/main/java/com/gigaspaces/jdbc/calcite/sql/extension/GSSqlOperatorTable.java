/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc.calcite.sql.extension;

import com.gigaspaces.internal.utils.LazySingleton;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
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
                    OperandTypes.NILADIC,
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

    public static final SqlFunction SUBSTR =
            new SqlFunction(
                    "SUBSTR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.CHAR,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction STRPOS =
            new SqlFunction(
                    "STRPOS",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction SUBSTRING =
            new SqlFunction(
                    "SUBSTRING",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.CHAR,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
                    SqlFunctionCategory.STRING);


    public static final SqlFunction TRUNC =
            new SqlFunction(
                    "TRUNC",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction ODBC_TIMESTAMP =
            new NoParameterSqlFunction(
                    "ODBC_TIMESTAMP", SqlTypeName.VARCHAR) {

                @Override
                public SqlSyntax getSyntax() {
                    return SqlSyntax.FUNCTION;
                }
            };


    public static final SqlFunction DAYNAME =
            new SqlFunction(
                    "DAYNAME",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction DAYOFMONTH =
            new SqlFunction(
                    "DAYOFMONTH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction DAYOFWEEK =
            new SqlFunction(
                    "DAYOFWEEK",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction DAYOFYEAR =
            new SqlFunction(
                    "DAYOFYEAR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction HOUR =
            new SqlFunction(
                    "HOUR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction MINUTE =
            new SqlFunction(
                    "MINUTE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction MONTH =
            new SqlFunction(
                    "MONTH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction MONTHNAME =
            new SqlFunction(
                    "MONTHNAME",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction QUARTER =
            new SqlFunction(
                    "QUARTER",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction SECOND =
            new SqlFunction(
                    "SECOND",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction WEEK =
            new SqlFunction(
                    "WEEK",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction YEAR =
            new SqlFunction(
                    "YEAR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_4,
                    null,
                    OperandTypes.or(OperandTypes.PERIOD_OR_DATETIME, OperandTypes.STRING),
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction GENERATE_SERIES =
            new SqlFunction(
                    "GENERATE_SERIES",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction PG_GET_EXPR =
            new SqlFunction(
                    "PG_GET_EXPR",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                    SqlFunctionCategory.STRING);

    public static final SqlFunction ARRAY_LOWER =
            new SqlFunction(
                    "ARRAY_LOWER",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction ARRAY_UPPER =
            new SqlFunction(
                    "ARRAY_UPPER",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                    SqlFunctionCategory.NUMERIC);
}
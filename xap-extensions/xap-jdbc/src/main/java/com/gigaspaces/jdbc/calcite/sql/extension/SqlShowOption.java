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

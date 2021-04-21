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
package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.Cursor;

import java.util.List;

public class JoinExplainPlan extends JdbcExplainPlan {
    private final JoinInfo joinInfo;
    private List<String> selectColumns;
    private final JdbcExplainPlan left;
    private final JdbcExplainPlan right;

    public JoinExplainPlan(JoinInfo joinInfo, JdbcExplainPlan left, JdbcExplainPlan right) {
        this.joinInfo = joinInfo;
        this.left = left;
        this.right = right;
    }

    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    @Override
    public void format(TextReportFormatter formatter) {
        JoinInfo.JoinAlgorithm joinAlgorithm = joinInfo.getRightColumn().getTableContainer().getQueryResult().getCursorType().equals(Cursor.Type.HASH) ? JoinInfo.JoinAlgorithm.Hash : JoinInfo.JoinAlgorithm.Nested;
        boolean hashJoin = joinAlgorithm.equals(JoinInfo.JoinAlgorithm.Hash);
        formatter.line(String.format("%s Join (%s)", joinInfo.getJoinType(), joinAlgorithm));
        formatter.indent(() -> {
            if(selectColumns != null)
                formatter.line(String.format("Select: %s", String.join(", ", selectColumns)));
            formatter.line(String.format("Join condition: (%s = %s)", joinInfo.getLeftColumn(), joinInfo.getRightColumn()));
            formatter.indent(() -> {
                if(hashJoin) {
                    formatter.line(String.format("BuildPhase - Hash by: %s", joinInfo.getRightColumn()));
                    formatter.indent(() -> left.format(formatter));
                }
                else{
                    left.format(formatter);
                }
            });
            formatter.indent(() -> {
                if(hashJoin) {
                    formatter.line("ProbePhase");
                    formatter.indent(() -> right.format(formatter));
                }
                else{
                    right.format(formatter);
                }
            });
        });
    }


}

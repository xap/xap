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
package com.j_spaces.jdbc;


import java.sql.SQLException;

public class SumColumn extends SelectColumn {


    private SelectColumn left;
    private SelectColumn right;
    private String operator;


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
        if (!(left instanceof ValueSelectColumn)) left.createColumnData(query);
        if (!(right instanceof ValueSelectColumn)) right.createColumnData(query);
    }

    @Override
    public boolean isAllColumns() {
        return false;
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

//    @Override
//    public String getName() {
//        return super.getAlias();
//    }

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
}

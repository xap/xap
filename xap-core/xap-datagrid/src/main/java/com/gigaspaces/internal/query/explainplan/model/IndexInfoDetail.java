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
package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.ExplainPlanUtil;
import com.gigaspaces.internal.query.explainplan.IndexInfo;
import com.gigaspaces.internal.query.explainplan.QueryOperator;
import com.gigaspaces.metadata.index.SpaceIndexType;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.getValueDesc;

/**
 * Single index choice detail
 *
 * @author Mishel Liberman
 * @since 16.0
 */
public class IndexInfoDetail {
    private Integer id;
    private Integer size;
    private String name;
    private Object value;
    private QueryOperator operator;
    private SpaceIndexType type;


    public IndexInfoDetail(Integer id, IndexInfo option) {
        this.id = id;
        name = option.getName();
        value = option.getValue();
        operator = option.getOperator();
        size = option.getSize();
        type = option.getType();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public SpaceIndexType getType() {
        return type;
    }

    public void setType(SpaceIndexType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return getString(true);
    }

    public String toStringNotVerbose(Integer min, Integer max) {
        if (min.equals(max)) {
            return getString(false);
        }

        return String.format("- " + getValueFormatting() + ", size=[min=%s, max=%s]"
                , getNameDescription(), getOperationDescription()
                , getValueDescription(value), min, max); //TODO mishel - move to another method in my code instead of here.
    }
//
    protected String getString(boolean verbose) {
        if (verbose) {
            return String.format("[#%s] " + getValueFormatting() + ", size=%s, type=%s"
                    , getId(), getNameDescription(), getOperationDescription()
                    , getValueDescription(value), getSizeDesc(), getType());
        } else {
            return String.format("- " + getValueFormatting() + ", size=%s"
                    , getNameDescription(), getOperationDescription()
                    , getValueDescription(value), getSizeDesc());
        }
    }

    protected String getValueFormatting(){
        return "(%s %s%s)";
    }

    protected String getNameDescription(){
        return getName();
    }

    protected String getOperationDescription(){
        return ExplainPlanUtil.getQueryOperatorDescription( operator );
    }

    protected Object getValueDescription( Object value ){
        return getValueDesc( value );
    }

    protected String getSizeDesc() {
        return size == null || size == -1 ? "unknown" : String.valueOf(size);
    }
}

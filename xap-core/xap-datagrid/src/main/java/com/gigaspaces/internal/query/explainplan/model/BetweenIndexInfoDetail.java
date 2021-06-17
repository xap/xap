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

import com.gigaspaces.internal.query.explainplan.BetweenIndexInfo;

public class BetweenIndexInfoDetail extends IndexInfoDetail{
    private Comparable min;
    private Comparable max;
    private boolean includeMin;
    private boolean includeMax;

    public BetweenIndexInfoDetail(Integer id, BetweenIndexInfo betweenIndexOption){
        super(id,betweenIndexOption);
        max = betweenIndexOption.getMax();
        min = betweenIndexOption.getMin();
        includeMax = betweenIndexOption.isIncludeMax();
        includeMin = betweenIndexOption.isIncludeMin();
    }

    public Comparable getMin() {
        return min;
    }

    public Comparable getMax() {
        return max;
    }

    public boolean isIncludeMin() {
        return includeMin;
    }

    public boolean isIncludeMax() {
        return includeMax;
    }

    @Override
    public String toString() {
        return getString(true);
    }

    @Override
    protected String getValueFormatting(){
        return "(%s%s%s)";
    }

    @Override
    public String getNameDescription() {
        return "";
    }

    @Override
    protected String getOperationDescription(){
        return "";
    }

    @Override
    protected Object getValueDescription( Object value ){

        StringBuilder stringBuilder = new StringBuilder();
        if( min != null ){
            stringBuilder.append( getMin() );
            stringBuilder.append( " " );
            stringBuilder.append( isIncludeMin() ? "<=" : "<" );
            stringBuilder.append( " " );
        }
        stringBuilder.append( getName() );
        if( max != null ){
            stringBuilder.append( " " );
            stringBuilder.append( isIncludeMax() ? "<=" : "<" );
            stringBuilder.append( " " );
            stringBuilder.append( getMax() );
        }

        return stringBuilder.toString();
    }
}
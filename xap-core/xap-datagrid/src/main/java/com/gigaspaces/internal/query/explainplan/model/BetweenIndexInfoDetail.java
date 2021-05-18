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
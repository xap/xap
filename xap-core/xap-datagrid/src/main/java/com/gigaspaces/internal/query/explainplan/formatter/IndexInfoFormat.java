package com.gigaspaces.internal.query.explainplan.formatter;

import com.gigaspaces.internal.query.explainplan.QueryOperator;
import com.gigaspaces.metadata.index.SpaceIndexType;

public class IndexInfoFormat {
    private Integer id;
    private String name;
    private Object value;
    private QueryOperator operator;
    private Integer size;
    private SpaceIndexType type;

    public IndexInfoFormat() {
    }

    public IndexInfoFormat(Integer id, String name, Object value, QueryOperator operator, Integer size, SpaceIndexType type) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.operator = operator;
        this.size = size;
        this.type = type;
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
        return String.format("[#%s] (%s %s %s), Size=%s, IndexType=%s"
                , getId(), getName(), getOperator().getValueOnTheRightOperator()
                , getValue(), getSize(), getType());
    }
}

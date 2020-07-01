package com.gigaspaces.config;

import java.io.Serializable;
import java.util.Arrays;

public class CompoundIdBase implements Serializable {
    private Object[] values;

    public CompoundIdBase(Object[] values) {
        this.values = values;
    }

    public Object[] getValues() {
        return values;
    }

    public String toString()
    {
        return Arrays.toString(getValues());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompoundIdBase that = (CompoundIdBase) o;
        return Arrays.equals(that.getValues(),getValues());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValues());
    }

}

package com.gigaspaces.entry;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Author: Ayelet Morris
 * Since 15.5.0
 */
public class CompoundKey implements Serializable {

    private static final long serialVersionUID = 1L;
    private Object[] values;

    public CompoundKey(Object... values) {
        this.values = values;
    }

    public CompoundKey(int numOfValues) {
        this.values = new Object[numOfValues];
    }

    public Object getValue(int index) {
        return values[index];
    }

    public void setValue(int index, Object value) {
        values[index] = value;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompoundKey that = (CompoundKey) o;
        return Arrays.equals(that.values,this.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}

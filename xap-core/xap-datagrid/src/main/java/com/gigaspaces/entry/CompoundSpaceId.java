package com.gigaspaces.entry;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This Object was created in order to implement GigaSpaces CompoundId logic
 * Extending this object will result in inherit all the capabilities of CompoundSpaceId
 * In order to use this, please implement your CompoundId pojo as explained:
 *
 * use EmbeddedId hibernate annotation on your getter for the compoundId instance in your POJO that holds the conpoundId
 * use @Embeddable hibernate annotation on your class in order to use this in hivernate
 * Implement your empty constructor with call to super with the number of values you have in your compoundID
 * public MyCompoundSpaceId() {
 *    super(2);
 * }
 *
 * @Column(name = "FIELDKEY1")
 * public String getFieldKey1() {
 *     return (String) getValue(0);
 * }
 * public void setFieldKey1(String fieldKey1) {
 *   setValue(0, fieldKey1);
 * }
 *
 * todo: should this be externelizable?
 * todo: consult David about this decumentation
 *
 * Author: Ayelet Morris
 * Since 15.5.0
 */
public class CompoundSpaceId implements Serializable {

    private static final long serialVersionUID = 1L;
    private Object[] values;

    public CompoundSpaceId(Object... values) {
        this.values = values;
    }

    public CompoundSpaceId(int numOfValues) {
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
        CompoundSpaceId that = (CompoundSpaceId) o;
        return Arrays.equals(that.values,this.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}
